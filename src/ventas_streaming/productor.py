"""Productor sintético y reproducible de ventas POS hacia Kafka.

Genera eventos ``sale.confirmed`` con reloj de evento propio y permite inyectar,
de forma controlada y con semilla fija, los cuatro comportamientos adversos que
el pipeline debe tolerar:

* duplicados (mismo ``event_id`` reenviado);
* desorden (un evento se retiene unos segundos mientras otros más nuevos salen);
* tardíos aceptables (ventas de la ventana anterior, dentro de la lateness);
* demasiado tardíos (fuera del horizonte de corrección) e inválidos.

Modos::

    python -m ventas_streaming.productor --modo continuo --tasa 4 --duracion 120
    python -m ventas_streaming.productor --modo demo
    python -m ventas_streaming.productor --modo continuo --salida-jsonl data/eventos.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import random
import signal
import sys
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from typing import Any

from ventas_streaming.config import Settings
from ventas_streaming.contratos import (
    EVENT_TYPE_VENTA,
    SCHEMA_VERSION_ACTUAL,
    codificar_evento,
    iso_utc,
    parse_iso_utc,
)

log = logging.getLogger("productor")

# Sucursales con pesos distintos a propósito: permite analizar skew por clave.
SUCURSALES: dict[str, float] = {
    "S1": 0.30,
    "S2": 0.25,
    "S3": 0.15,
    "S4": 0.15,
    "S5": 0.10,
    "S6": 0.05,
}
POS_POR_SUCURSAL = 3
PESOS_PAGO = {"cash": 0.35, "card": 0.40, "qr": 0.20, "transfer": 0.05}
# Segundos tras el fin de ventana a partir de los cuales se asume que el watermark ya la cerró.
MARGEN_WATERMARK = 15


@dataclass
class GeneradorVentas:
    """Genera eventos válidos deterministas a partir de una semilla."""

    semilla: int = 7
    version_esquema: int = SCHEMA_VERSION_ACTUAL
    mezclar_versiones: bool = True
    rng: random.Random = field(init=False)
    secuencias: dict[str, int] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.rng = random.Random(self.semilla)

    def siguiente(self, event_time: datetime) -> dict[str, Any]:
        store = self.rng.choices(list(SUCURSALES), weights=list(SUCURSALES.values()))[0]
        pos = f"pos-{self.rng.randint(1, POS_POR_SUCURSAL):02d}"
        clave_seq = f"{store}-{pos}"
        seq = self.secuencias.get(clave_seq, 0) + 1
        self.secuencias[clave_seq] = seq
        items = max(1, int(self.rng.expovariate(1 / 2.5)) + 1)
        monto = int(round(self.rng.lognormvariate(math.log(120_000), 0.6) * items / 1000)) * 1000
        version = self.version_esquema
        if self.mezclar_versiones and self.rng.random() < 0.2:
            version = 1
        payload: dict[str, Any] = {
            "store_id": store,
            "pos_id": pos,
            "ticket_id": f"T-{store}-{pos}-{seq:06d}",
            "amount_gs": max(5_000, monto),
            "items": items,
            "payment": self.rng.choices(list(PESOS_PAGO), weights=list(PESOS_PAGO.values()))[0],
        }
        if version >= 2:
            payload["channel"] = "store" if self.rng.random() < 0.9 else "delivery"
        return {
            "schema_version": version,
            "event_id": f"{store}-{pos}-{seq:06d}",
            "event_type": EVENT_TYPE_VENTA,
            "event_time": iso_utc(event_time),
            "key": store,
            "payload": payload,
        }


def evento_invalido(base: dict[str, Any], tipo: str) -> bytes:
    """Construye un mensaje corrupto a partir de un evento válido."""
    if tipo == "json_corrupto":
        return b'{"schema_version": 2, "event_id": "roto"'
    roto = json.loads(json.dumps(base))
    if tipo == "monto_negativo":
        roto["payload"]["amount_gs"] = -roto["payload"]["amount_gs"]
    elif tipo == "sin_payload":
        roto.pop("payload")
    elif tipo == "reloj_futuro":
        futuro = parse_iso_utc(base["event_time"]) + timedelta(minutes=10)
        roto["event_time"] = iso_utc(futuro)
    elif tipo == "version_desconocida":
        roto["schema_version"] = 99
    elif tipo == "pago_desconocido":
        roto["payload"]["payment"] = "cripto"
    else:
        raise ValueError(f"tipo de evento inválido desconocido: {tipo}")
    return codificar_evento(roto)


TIPOS_INVALIDOS = (
    "json_corrupto",
    "monto_negativo",
    "sin_payload",
    "reloj_futuro",
    "version_desconocida",
    "pago_desconocido",
)


def event_time_tardio_aceptable(
    ahora: datetime, settings: Settings, rng: random.Random
) -> datetime | None:
    """Devuelve un event_time de la ventana anterior que Beam debe aceptar como tardío.

    Para que el evento sea *tardío* y no simplemente desordenado, el watermark
    tiene que haber cruzado ya el fin de la ventana anterior: en Flink el
    watermark de KafkaIO avanza unos segundos por detrás del reloj, así que se
    espera al menos ``MARGEN_WATERMARK`` s tras el borde. Y para que Beam lo
    acepte, debe llegar antes de que expire la lateness (con 5 s de margen).
    Fuera de ese intervalo devuelve ``None`` y el llamador reintenta más tarde.
    """
    ventana = settings.window_seconds
    epoch = ahora.timestamp()
    fin_anterior = math.floor(epoch / ventana) * ventana
    transcurrido = epoch - fin_anterior
    if not (MARGEN_WATERMARK <= transcurrido <= settings.allowed_lateness_seconds - 5):
        return None
    retroceso = rng.uniform(2, max(3, ventana - 2))
    return datetime.fromtimestamp(fin_anterior - retroceso, tz=UTC)


def event_time_demasiado_tardio(
    ahora: datetime, settings: Settings, rng: random.Random
) -> datetime:
    exceso = settings.horizonte_tardios_seconds + rng.uniform(30, 120)
    return ahora - timedelta(seconds=exceso)


@dataclass
class Mensaje:
    """Un mensaje listo para Kafka, con la etiqueta del escenario que lo originó."""

    etiqueta: str
    key: str
    value: bytes
    event_time: datetime | None
    headers: dict[str, str]
    event_id: str = ""


class Emisor:
    """Publica mensajes en Kafka o en un archivo JSONL (para pruebas y datos de ejemplo)."""

    def __init__(self, settings: Settings, *, salida_jsonl: Path | None = None) -> None:
        self.settings = settings
        self.enviados = 0
        self.confirmados = 0
        self.errores = 0
        self.por_etiqueta: dict[str, int] = {}
        self._archivo = None
        self._producer = None
        if salida_jsonl is not None:
            salida_jsonl.parent.mkdir(parents=True, exist_ok=True)
            self._archivo = salida_jsonl.open("w", encoding="utf-8")
        else:
            from confluent_kafka import Producer

            self._producer = Producer(
                {
                    "bootstrap.servers": settings.kafka_bootstrap_servers,
                    "client.id": "ventas-pos-productor",
                    # Reintentos sin duplicar ni reordenar dentro de la partición.
                    "enable.idempotence": True,
                    "acks": "all",
                    "compression.type": "snappy",
                    "linger.ms": 20,
                }
            )

    def _confirmacion(self, mensaje: Mensaje) -> Callable[[Any, Any], None]:
        def _cb(error: Any, record: Any) -> None:
            if error is not None:
                self.errores += 1
                log.error("entrega fallida %s: %s", mensaje.event_id, error)
                return
            self.confirmados += 1
            log.info(
                "[%s] %s key=%s event_time=%s -> partición %d offset %d",
                mensaje.etiqueta,
                mensaje.event_id or "-",
                mensaje.key,
                mensaje.event_time.isoformat(timespec="seconds") if mensaje.event_time else "-",
                record.partition(),
                record.offset(),
            )

        return _cb

    def emitir(self, mensaje: Mensaje) -> None:
        self.enviados += 1
        self.por_etiqueta[mensaje.etiqueta] = self.por_etiqueta.get(mensaje.etiqueta, 0) + 1
        timestamp_ms = int(mensaje.event_time.timestamp() * 1000) if mensaje.event_time else 0
        if self._archivo is not None:
            registro = {
                "etiqueta": mensaje.etiqueta,
                "key": mensaje.key,
                "timestamp_ms": timestamp_ms,
                "headers": mensaje.headers,
                "value": mensaje.value.decode("utf-8", errors="replace"),
            }
            self._archivo.write(json.dumps(registro, ensure_ascii=False) + "\n")
            self.confirmados += 1
            log.info(
                "[%s] %s key=%s -> archivo", mensaje.etiqueta, mensaje.event_id or "-", mensaje.key
            )
            return
        assert self._producer is not None
        kwargs: dict[str, Any] = {}
        if mensaje.event_time is not None:
            # El timestamp Kafka = event_time: KafkaIO (create_time) deriva de él su watermark.
            kwargs["timestamp"] = timestamp_ms
        self._producer.produce(
            self.settings.topico_ventas,
            key=mensaje.key.encode(),
            value=mensaje.value,
            headers=list(mensaje.headers.items()),
            on_delivery=self._confirmacion(mensaje),
            **kwargs,
        )
        self._producer.poll(0)

    def sondear(self) -> None:
        if self._producer is not None:
            self._producer.poll(0)

    def cerrar(self) -> dict[str, Any]:
        if self._producer is not None:
            self._producer.flush(15)
        if self._archivo is not None:
            self._archivo.close()
        return {
            "enviados": self.enviados,
            "confirmados": self.confirmados,
            "errores": self.errores,
            "por_etiqueta": dict(sorted(self.por_etiqueta.items())),
        }


class VigilanteDeSalida:
    """Observa el tópico de agregados para saber cuándo Beam cerró una ventana.

    El guion de demostración lo usa para enviar la venta tardía *después* del
    pane ON_TIME de la ventana anterior, sin depender del retraso con que el
    watermark de KafkaIO avanza en Flink (entre 10 y 25 s en el laboratorio).
    """

    def __init__(self, settings: Settings) -> None:
        from confluent_kafka import Consumer

        self.consumidor = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": f"productor-demo-vigilante-{random.randrange(1 << 30)}",
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
            }
        )
        self.consumidor.subscribe([settings.topico_agregados])
        self.vistos: set[tuple[str, str]] = set()

    def pane_on_time_visto(self, window_end: datetime) -> bool:
        """Sondeo no bloqueante: ¿ya se publicó un pane ON_TIME para esa ventana?"""
        objetivo = iso_utc(window_end)
        for mensaje in self.consumidor.consume(num_messages=50, timeout=0.05):
            if mensaje.error():
                continue
            try:
                agregado = json.loads(mensaje.value())
            except (ValueError, UnicodeDecodeError):
                continue
            if agregado.get("pane_timing") == "ON_TIME":
                self.vistos.add((agregado["window_end"], agregado["store_id"]))
        return any(fin == objetivo for fin, _ in self.vistos)

    def cerrar(self) -> None:
        self.consumidor.close()


def mensaje_de_evento(evento: dict[str, Any], etiqueta: str) -> Mensaje:
    return Mensaje(
        etiqueta=etiqueta,
        key=evento["key"],
        value=codificar_evento(evento),
        event_time=parse_iso_utc(evento["event_time"]),
        headers={
            "schema_version": str(evento["schema_version"]),
            "event_type": evento["event_type"],
        },
        event_id=evento["event_id"],
    )


@dataclass
class Perturbaciones:
    """Probabilidades por evento de cada comportamiento adverso (modo continuo)."""

    duplicados: float = 0.0
    desorden: float = 0.0
    tardios: float = 0.0
    muy_tardios: float = 0.0
    invalidos: float = 0.0


class Simulador:
    """Reloj de pared + generador + perturbaciones; produce mensajes listos para emitir."""

    def __init__(
        self,
        settings: Settings,
        generador: GeneradorVentas,
        perturbaciones: Perturbaciones,
        *,
        semilla: int = 11,
        ahora_fn: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.settings = settings
        self.generador = generador
        self.perturbaciones = perturbaciones
        self.rng = random.Random(semilla)
        self.ahora_fn = ahora_fn
        self.pendientes: list[tuple[float, Mensaje]] = []  # (momento de emisión, mensaje)
        self.tardios_pendientes = 0
        self._tipo_invalido = 0

    def _programar(self, retardo: float, mensaje: Mensaje) -> None:
        self.pendientes.append((time.monotonic() + retardo, mensaje))

    def paso(self) -> list[Mensaje]:
        """Genera una venta normal y, según las probabilidades, sus perturbaciones."""
        ahora = self.ahora_fn()
        salida: list[Mensaje] = []
        # Retardo normal POS -> Kafka: de 0 a 3 s (p95 asumido de 40 s escalado a la demo).
        event_time = ahora - timedelta(seconds=self.rng.uniform(0, 3))
        evento = self.generador.siguiente(event_time)
        mensaje = mensaje_de_evento(evento, "OK")
        p = self.perturbaciones

        if self.rng.random() < p.desorden:
            # Se retiene unos segundos: llegará después de eventos más nuevos.
            self._programar(self.rng.uniform(2, 6), mensaje_de_evento(evento, "DESORDEN"))
        else:
            salida.append(mensaje)

        if self.rng.random() < p.duplicados:
            # Reenvío del POS (no recibió el ack): mismo event_id, mismos bytes.
            self._programar(self.rng.uniform(0, 4), mensaje_de_evento(evento, "DUPLICADO"))

        if self.rng.random() < p.tardios:
            self.tardios_pendientes += 1
        if self.tardios_pendientes:
            tardio = event_time_tardio_aceptable(ahora, self.settings, self.rng)
            if tardio is not None:
                self.tardios_pendientes -= 1
                salida.append(mensaje_de_evento(self.generador.siguiente(tardio), "TARDIO"))

        if self.rng.random() < p.muy_tardios:
            muy_tardio = event_time_demasiado_tardio(ahora, self.settings, self.rng)
            salida.append(mensaje_de_evento(self.generador.siguiente(muy_tardio), "MUY_TARDIO"))

        if self.rng.random() < p.invalidos:
            salida.append(self.invalido(evento))

        salida.extend(self.vencidos())
        return salida

    def invalido(self, base: dict[str, Any]) -> Mensaje:
        tipo = TIPOS_INVALIDOS[self._tipo_invalido % len(TIPOS_INVALIDOS)]
        self._tipo_invalido += 1
        return Mensaje(
            etiqueta=f"INVALIDO:{tipo}",
            key=base["key"],
            value=evento_invalido(base, tipo),
            event_time=parse_iso_utc(base["event_time"]),
            headers={"schema_version": str(base["schema_version"]), "event_type": EVENT_TYPE_VENTA},
            event_id=base["event_id"],
        )

    def vencidos(self) -> list[Mensaje]:
        """Mensajes retenidos (desorden, duplicados) cuyo momento de emisión ya llegó."""
        ahora = time.monotonic()
        listos = [m for (momento, m) in self.pendientes if momento <= ahora]
        self.pendientes = [(momento, m) for (momento, m) in self.pendientes if momento > ahora]
        return listos

    def drenar(self) -> list[Mensaje]:
        listos = [m for (_, m) in self.pendientes]
        self.pendientes = []
        return listos


def escenario_demo(
    simulador: Simulador, vigilante: VigilanteDeSalida | None = None
) -> Iterator[tuple[float, list[Mensaje]]]:
    """Guion determinista para la demostración: (segundos de espera, mensajes).

    1. ventas normales; 2. tres duplicados; 3. cinco ventas en orden inverso;
    4. una venta tardía aceptable (tras observar el pane ON_TIME de su ventana);
    5. una venta demasiado tardía; 6. seis mensajes inválidos; 7. ventas normales.
    """
    settings = simulador.settings
    gen = simulador.generador
    rng = simulador.rng

    def normal(etiqueta: str = "OK") -> Mensaje:
        ahora = simulador.ahora_fn()
        return mensaje_de_evento(
            gen.siguiente(ahora - timedelta(seconds=rng.uniform(0, 2))), etiqueta
        )

    log.info("FASE 1: ventas normales")
    for _ in range(40):
        yield 0.25, [normal()]

    log.info("FASE 2: duplicados (mismo event_id reenviado)")
    for _ in range(3):
        original = normal()
        duplicado = Mensaje(
            "DUPLICADO",
            original.key,
            original.value,
            original.event_time,
            original.headers,
            original.event_id,
        )
        yield 0.5, [original]
        yield 1.0, [duplicado]

    log.info("FASE 3: desorden (cinco ventas emitidas en orden inverso a su event_time)")
    ahora = simulador.ahora_fn()
    lote = [
        mensaje_de_evento(gen.siguiente(ahora - timedelta(seconds=10 - 2 * i)), "DESORDEN")
        for i in range(5)
    ]
    for mensaje in reversed(lote):
        yield 0.3, [mensaje]

    log.info("FASE 4: venta tardía de la ventana anterior")
    ventana = settings.window_seconds
    if vigilante is None:
        # Sin acceso al tópico de salida (archivo JSONL): heurística por reloj.
        while (tardio := event_time_tardio_aceptable(simulador.ahora_fn(), settings, rng)) is None:
            yield 1.0, [normal()]
        yield 0.5, [mensaje_de_evento(gen.siguiente(tardio), "TARDIO")]
    else:
        # Se elige la ventana en curso, se sigue vendiendo hasta que Beam publique su
        # pane ON_TIME y recién entonces se envía una venta con event_time dentro de ella:
        # llega tarde (la ventana ya cerró) pero dentro de la lateness permitida.
        epoch = simulador.ahora_fn().timestamp()
        fin = datetime.fromtimestamp(math.ceil(epoch / ventana) * ventana, tz=UTC)
        log.info(
            "  ventana objetivo [%s, %s); esperando su pane ON_TIME",
            iso_utc(fin - timedelta(seconds=ventana)),
            iso_utc(fin),
        )
        limite = fin + timedelta(seconds=ventana + settings.allowed_lateness_seconds)
        while not vigilante.pane_on_time_visto(fin):
            if simulador.ahora_fn() > limite:
                log.warning("  no se observó el pane ON_TIME a tiempo; se envía el tardío igual")
                break
            yield 0.5, [normal()]
        tardio = fin - timedelta(seconds=rng.uniform(2, ventana - 2))
        log.info(
            "  pane ON_TIME observado; enviando venta tardía con event_time=%s", iso_utc(tardio)
        )
        yield 0.5, [mensaje_de_evento(gen.siguiente(tardio), "TARDIO")]

    log.info("FASE 5: venta demasiado tardía (fuera del horizonte de corrección)")
    muy_tardio = event_time_demasiado_tardio(simulador.ahora_fn(), settings, rng)
    yield 0.5, [mensaje_de_evento(gen.siguiente(muy_tardio), "MUY_TARDIO")]

    log.info("FASE 6: mensajes inválidos (uno por tipo)")
    for _ in TIPOS_INVALIDOS:
        base = gen.siguiente(simulador.ahora_fn())
        yield 0.5, [simulador.invalido(base)]

    log.info("FASE 7: ventas normales hasta el final")
    for _ in range(80):
        yield 0.25, [normal()]


def ejecutar_continuo(
    emisor: Emisor, simulador: Simulador, *, tasa: float, duracion: float, parar: Event
) -> None:
    intervalo = 1.0 / tasa
    fin = time.monotonic() + duracion if duracion > 0 else float("inf")
    proximo = time.monotonic()
    while not parar.is_set() and time.monotonic() < fin:
        for mensaje in simulador.paso():
            emisor.emitir(mensaje)
        proximo += intervalo
        while not parar.is_set() and (restante := proximo - time.monotonic()) > 0:
            time.sleep(min(restante, 0.1))
            emisor.sondear()
    for mensaje in simulador.drenar():
        emisor.emitir(mensaje)


def ejecutar_demo(
    emisor: Emisor, simulador: Simulador, *, parar: Event, vigilante: VigilanteDeSalida | None
) -> None:
    for espera, mensajes in escenario_demo(simulador, vigilante):
        if parar.is_set():
            break
        for mensaje in mensajes:
            emisor.emitir(mensaje)
        for mensaje in simulador.vencidos():
            emisor.emitir(mensaje)
        fin = time.monotonic() + espera
        while not parar.is_set() and (restante := fin - time.monotonic()) > 0:
            time.sleep(min(restante, 0.1))
            emisor.sondear()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--modo", choices=("continuo", "demo"), default="continuo")
    parser.add_argument(
        "--tasa", type=float, default=4.0, help="ventas por segundo (modo continuo)"
    )
    parser.add_argument("--duracion", type=float, default=120, help="segundos; 0 = hasta Ctrl+C")
    parser.add_argument("--semilla", type=int, default=7)
    parser.add_argument("--dup-rate", type=float, default=0.05)
    parser.add_argument("--desorden-rate", type=float, default=0.10)
    parser.add_argument("--tardios-rate", type=float, default=0.03)
    parser.add_argument("--muy-tardios-rate", type=float, default=0.01)
    parser.add_argument("--invalidos-rate", type=float, default=0.02)
    parser.add_argument("--solo-validos", action="store_true", help="desactiva toda perturbación")
    parser.add_argument("--salida-jsonl", type=Path, help="escribe a un archivo en lugar de Kafka")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )
    settings = Settings.from_env()
    perturbaciones = (
        Perturbaciones()
        if args.solo_validos
        else Perturbaciones(
            duplicados=args.dup_rate,
            desorden=args.desorden_rate,
            tardios=args.tardios_rate,
            muy_tardios=args.muy_tardios_rate,
            invalidos=args.invalidos_rate,
        )
    )
    simulador = Simulador(
        settings, GeneradorVentas(args.semilla), perturbaciones, semilla=args.semilla + 1
    )
    emisor = Emisor(settings, salida_jsonl=args.salida_jsonl)
    parar = Event()
    signal.signal(signal.SIGTERM, lambda *_: parar.set())
    signal.signal(signal.SIGINT, lambda *_: parar.set())
    log.info(
        "productor iniciado modo=%s tópico=%s ventana=%ss lateness=%ss semilla=%d",
        args.modo,
        settings.topico_ventas,
        settings.window_seconds,
        settings.allowed_lateness_seconds,
        args.semilla,
    )
    try:
        if args.modo == "demo":
            vigilante = None if args.salida_jsonl is not None else VigilanteDeSalida(settings)
            try:
                ejecutar_demo(emisor, simulador, parar=parar, vigilante=vigilante)
            finally:
                if vigilante is not None:
                    vigilante.cerrar()
        else:
            ejecutar_continuo(
                emisor, simulador, tasa=args.tasa, duracion=args.duracion, parar=parar
            )
    finally:
        resumen = emisor.cerrar()
        log.info("resumen del productor: %s", json.dumps(resumen, ensure_ascii=False))
        print(json.dumps(resumen, ensure_ascii=False))


if __name__ == "__main__":
    main()
