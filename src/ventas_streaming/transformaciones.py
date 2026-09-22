"""Transformaciones Apache Beam reutilizables (independientes de Kafka).

El núcleo del pipeline se construye con ``construir_nucleo`` a partir de una
PCollection de pares ``(key_bytes, value_bytes)`` tal como los entrega KafkaIO,
de modo que las pruebas lo ejecuten con ``TestPipeline``/``TestStream`` sin
tocar Kafka ni Flink.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.metrics import Metrics
from apache_beam.transforms import trigger, window
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.windowed_value import PaneInfoTiming

from ventas_streaming.config import Settings
from ventas_streaming.contratos import (
    SCHEMA_VERSION_AGREGADO,
    EventoInvalido,
    aggregate_id,
    decodificar_evento,
    epoch_seconds,
    iso_utc,
)

NOMBRE_TIMING = {
    PaneInfoTiming.EARLY: "EARLY",
    PaneInfoTiming.ON_TIME: "ON_TIME",
    PaneInfoTiming.LATE: "LATE",
    PaneInfoTiming.UNKNOWN: "UNKNOWN",
}


def _ahora() -> datetime:
    return datetime.now(UTC)


class ParsearYValidar(beam.DoFn):
    """Decodifica el contrato y separa las salidas laterales.

    Salidas:
    * principal: eventos válidos normalizados al esquema actual;
    * ``invalido``: JSON corrupto, campos faltantes, versión no soportada, reloj
      implausible (más de 2 min en el futuro o más de 24 h en el pasado);
    * ``muy_tardio``: eventos válidos cuyo ``event_time`` queda fuera del
      horizonte de corrección (fin de ventana + lateness) respecto de un
      watermark heurístico local (máximo ``event_time`` válido visto por este
      worker). Beam los descartaría en silencio; aquí se desvían a la DLQ con
      ``motivo=demasiado_tardio`` para la conciliación contable.
    """

    INVALIDO = "invalido"
    MUY_TARDIO = "muy_tardio"

    def __init__(self, settings: Settings, ahora_fn: Callable[[], datetime] = _ahora) -> None:
        super().__init__()
        self.settings = settings
        self.ahora_fn = ahora_fn
        self.max_event_time: float | None = None
        self.validos = Metrics.counter("ventas", "eventos_validos")
        self.invalidos = Metrics.counter("ventas", "eventos_invalidos")
        self.muy_tardios = Metrics.counter("ventas", "eventos_demasiado_tardios")
        self.retraso_ms = Metrics.distribution("ventas", "retraso_ingesta_ms")

    def setup(self) -> None:
        self.max_event_time = None

    def _rechazo(
        self,
        motivo: str,
        detalle: str,
        key: bytes | None,
        value: bytes,
        evento: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        rechazo: dict[str, Any] = {
            "motivo": motivo,
            "detalle": detalle,
            "key": key.decode(errors="replace") if key else None,
            "rechazado_en": iso_utc(self.ahora_fn()),
            "mensaje_original": value.decode(errors="replace"),
        }
        if evento is not None:
            rechazo["event_id"] = evento["event_id"]
            rechazo["event_time"] = evento["event_time"]
            rechazo["amount_gs"] = evento["payload"]["amount_gs"]
        return rechazo

    def process(self, element: tuple[bytes | None, bytes]):
        key, value = element
        try:
            evento = decodificar_evento(value)
        except EventoInvalido as error:
            self.invalidos.inc()
            yield pvalue.TaggedOutput(
                self.INVALIDO, self._rechazo(error.motivo, error.detalle, key, value)
            )
            return

        ahora = self.ahora_fn()
        ahora_epoch = ahora.timestamp()
        event_epoch = epoch_seconds(evento["event_time"])
        if event_epoch > ahora_epoch + self.settings.max_future_skew_seconds:
            self.invalidos.inc()
            yield pvalue.TaggedOutput(
                self.INVALIDO,
                self._rechazo(
                    "reloj_futuro",
                    f"{event_epoch - ahora_epoch:.0f}s adelantado",
                    key,
                    value,
                    evento,
                ),
            )
            return
        if event_epoch < ahora_epoch - self.settings.max_past_skew_seconds:
            self.invalidos.inc()
            yield pvalue.TaggedOutput(
                self.INVALIDO,
                self._rechazo(
                    "reloj_pasado", f"{ahora_epoch - event_epoch:.0f}s atrasado", key, value, evento
                ),
            )
            return

        horizonte = self.settings.horizonte_tardios_seconds
        if self.max_event_time is not None and event_epoch < self.max_event_time - horizonte:
            self.muy_tardios.inc()
            exceso = self.max_event_time - horizonte - event_epoch
            yield pvalue.TaggedOutput(
                self.MUY_TARDIO,
                self._rechazo(
                    "demasiado_tardio", f"{exceso:.0f}s más allá del horizonte", key, value, evento
                ),
            )
            return
        self.max_event_time = max(self.max_event_time or event_epoch, event_epoch)

        self.validos.inc()
        self.retraso_ms.update(int(max(0.0, ahora_epoch - event_epoch) * 1000))
        evento["ingest_time"] = iso_utc(ahora)
        yield evento


def asignar_tiempo_evento(evento: dict[str, Any]) -> TimestampedValue:
    """El timestamp Beam es el ``event_time`` del dominio, nunca el de procesamiento."""
    return TimestampedValue(evento, epoch_seconds(evento["event_time"]))


class VentasPorSucursalCombineFn(beam.CombineFn):
    """Agregación incremental con deduplicación por ``event_id`` dentro de la ventana.

    El acumulador guarda, por ``event_id``, el monto, los ítems y el medio de pago
    ya sumados; un duplicado que llegue en cualquier pane de la misma ventana no
    altera el total, y al fusionar acumuladores parciales (combiner lifting) los
    solapamientos se descuentan. El horizonte de deduplicación es, por
    construcción, el de la ventana: fin de ventana + lateness permitida.
    """

    def create_accumulator(self):
        return {"ventas": {}, "dup": 0}

    def add_input(self, acc, evento):
        event_id = evento["event_id"]
        if event_id in acc["ventas"]:
            acc["dup"] += 1
            return acc
        payload = evento["payload"]
        acc["ventas"][event_id] = (
            int(payload["amount_gs"]),
            int(payload["items"]),
            payload["payment"],
        )
        return acc

    def merge_accumulators(self, accumulators):
        resultado = self.create_accumulator()
        for acc in accumulators:
            solapados = len(resultado["ventas"].keys() & acc["ventas"].keys())
            resultado["dup"] += acc["dup"] + solapados
            resultado["ventas"].update(acc["ventas"])
        return resultado

    def extract_output(self, acc):
        ventas = acc["ventas"].values()
        tickets = len(acc["ventas"])
        total = sum(monto for monto, _, _ in ventas)
        por_pago: dict[str, int] = {}
        for monto, _, pago in ventas:
            por_pago[pago] = por_pago.get(pago, 0) + monto
        return {
            "ticket_count": tickets,
            "total_amount_gs": total,
            "items": sum(items for _, items, _ in ventas),
            "avg_ticket_gs": round(total / tickets) if tickets else 0,
            "by_payment_gs": dict(sorted(por_pago.items())),
            "duplicates_discarded": acc["dup"],
        }


class FormatearAgregado(beam.DoFn):
    """Adjunta ventana, pane y clave idempotente al resultado combinado."""

    def __init__(self, ahora_fn: Callable[[], datetime] = _ahora) -> None:
        super().__init__()
        self.ahora_fn = ahora_fn
        self.panes = Metrics.counter("ventas", "panes_emitidos")

    def process(
        self,
        element,
        ventana=beam.DoFn.WindowParam,
        pane=beam.DoFn.PaneInfoParam,
    ):
        store_id, metricas = element
        inicio = iso_utc(ventana.start.to_utc_datetime())
        fin = iso_utc(ventana.end.to_utc_datetime())
        self.panes.inc()
        yield {
            "schema_version": SCHEMA_VERSION_AGREGADO,
            "aggregate_id": aggregate_id(store_id, inicio),
            "store_id": store_id,
            "window_start": inicio,
            "window_end": fin,
            "pane_index": pane.index,
            "pane_timing": NOMBRE_TIMING.get(pane.timing, "UNKNOWN"),
            "is_first": pane.is_first,
            "is_last": pane.is_last,
            "emitted_at": iso_utc(self.ahora_fn()),
            **metricas,
        }


def ventanas_fijas(eventos, settings: Settings, *, triggers_streaming: bool = True):
    """Ventana fija por tiempo de evento con la política temporal del dominio.

    * ``AfterWatermark``: pane ON_TIME cuando el watermark cruza el fin de la ventana;
    * ``early=AfterProcessingTime``: panes EARLY provisionales para el tablero;
    * ``late=AfterCount(1)``: un pane LATE por cada corrección dentro de la lateness;
    * ``ACCUMULATING``: cada pane trae el total conocido hasta el momento, lo que
      permite un upsert simple por ``aggregate_id`` en el sink.
    """
    kwargs: dict[str, Any] = {
        "windowfn": window.FixedWindows(settings.window_seconds),
        "allowed_lateness": settings.allowed_lateness_seconds,
        "accumulation_mode": trigger.AccumulationMode.ACCUMULATING,
    }
    if triggers_streaming:
        kwargs["trigger"] = trigger.AfterWatermark(
            early=trigger.AfterProcessingTime(settings.early_firing_seconds),
            late=trigger.AfterCount(1),
        )
    return eventos | "Ventanas fijas por tiempo de evento" >> beam.WindowInto(**kwargs)


def construir_nucleo(
    registros,
    settings: Settings,
    *,
    triggers_streaming: bool = True,
    ahora_fn: Callable[[], datetime] = _ahora,
):
    """Kafka records -> (agregados, invalidos, muy_tardios)."""
    parseados = registros | "Parsear y validar contrato" >> beam.ParDo(
        ParsearYValidar(settings, ahora_fn)
    ).with_outputs(ParsearYValidar.INVALIDO, ParsearYValidar.MUY_TARDIO, main="validos")

    con_tiempo = parseados.validos | "Asignar tiempo de evento" >> beam.Map(asignar_tiempo_evento)
    agregados = (
        ventanas_fijas(con_tiempo, settings, triggers_streaming=triggers_streaming)
        | "Clave por sucursal" >> beam.Map(lambda evento: (evento["key"], evento))
        | "Combinar ventas por sucursal" >> beam.CombinePerKey(VentasPorSucursalCombineFn())
        | "Formatear agregado" >> beam.ParDo(FormatearAgregado(ahora_fn))
    )
    return agregados, parseados[ParsearYValidar.INVALIDO], parseados[ParsearYValidar.MUY_TARDIO]


def agregado_a_json(agregado: dict[str, Any]) -> str:
    return json.dumps(agregado, sort_keys=True)
