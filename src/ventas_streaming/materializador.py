"""Materialización idempotente de los agregados en SQLite y registro de la DLQ.

Consume ``ventas.por_ventana.v1`` y ``ventas.dlq.v1``. Cada agregado se
persiste con ``INSERT ... ON CONFLICT(aggregate_id) DO UPDATE`` condicionado a
que la versión del pane no retroceda. La versión es el par
``(ticket_count, pane_index)``: en modo ACCUMULATING y sin retractaciones el
conteo de una ventana nunca decrece, y ``pane_index`` desempata entre panes con
el mismo conteo (por ejemplo, el pane de cierre). Esto descarta tanto panes
viejos reproducidos como los panes *vacíos* que el runner puede emitir al
expirar una ventana con el mismo ``pane_index`` que el pane real. Releer el
tópico desde el inicio, recibir el mismo pane dos veces o recibir panes fuera de
orden deja la tabla en el mismo estado final: el tramo Kafka -> sink es
*efectivamente una sola vez* aunque la entrega sea *al menos una vez*.

Uso::

    python -m ventas_streaming.materializador                 # continuo
    python -m ventas_streaming.materializador --timeout 30    # termina tras 30 s sin mensajes
    python -m ventas_streaming.materializador --reporte       # imprime las tablas y sale
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sqlite3
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from typing import Any

from ventas_streaming.config import Settings

log = logging.getLogger("materializador")

DDL = """
CREATE TABLE IF NOT EXISTS ventas_por_ventana (
    aggregate_id          TEXT PRIMARY KEY,
    store_id              TEXT NOT NULL,
    window_start          TEXT NOT NULL,
    window_end            TEXT NOT NULL,
    ticket_count          INTEGER NOT NULL,
    total_amount_gs       INTEGER NOT NULL,
    items                 INTEGER NOT NULL,
    avg_ticket_gs         INTEGER NOT NULL,
    by_payment_gs         TEXT NOT NULL,
    duplicates_discarded  INTEGER NOT NULL,
    pane_index            INTEGER NOT NULL,
    pane_timing           TEXT NOT NULL,
    is_last               INTEGER NOT NULL,
    emitted_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL,
    panes_recibidos       INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS rechazos (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    motivo        TEXT NOT NULL,
    detalle       TEXT,
    key           TEXT,
    event_id      TEXT,
    event_time    TEXT,
    amount_gs     INTEGER,
    rechazado_en  TEXT NOT NULL,
    mensaje_original TEXT,
    UNIQUE(motivo, event_id, mensaje_original) ON CONFLICT IGNORE
);
CREATE TABLE IF NOT EXISTS offsets_consumidos (
    topico     TEXT NOT NULL,
    particion  INTEGER NOT NULL,
    offset     INTEGER NOT NULL,
    PRIMARY KEY (topico, particion)
);
"""

UPSERT = """
INSERT INTO ventas_por_ventana (
    aggregate_id, store_id, window_start, window_end, ticket_count, total_amount_gs,
    items, avg_ticket_gs, by_payment_gs, duplicates_discarded, pane_index, pane_timing,
    is_last, emitted_at, updated_at, panes_recibidos
) VALUES (
    :aggregate_id, :store_id, :window_start, :window_end, :ticket_count, :total_amount_gs,
    :items, :avg_ticket_gs, :by_payment_gs, :duplicates_discarded, :pane_index, :pane_timing,
    :is_last, :emitted_at, :updated_at, 1
)
ON CONFLICT(aggregate_id) DO UPDATE SET
    ticket_count = excluded.ticket_count,
    total_amount_gs = excluded.total_amount_gs,
    items = excluded.items,
    avg_ticket_gs = excluded.avg_ticket_gs,
    by_payment_gs = excluded.by_payment_gs,
    duplicates_discarded = excluded.duplicates_discarded,
    pane_index = excluded.pane_index,
    pane_timing = excluded.pane_timing,
    is_last = excluded.is_last,
    emitted_at = excluded.emitted_at,
    updated_at = excluded.updated_at,
    panes_recibidos = ventas_por_ventana.panes_recibidos + 1
WHERE excluded.ticket_count > ventas_por_ventana.ticket_count
   OR (excluded.ticket_count = ventas_por_ventana.ticket_count
       AND excluded.pane_index >= ventas_por_ventana.pane_index)
"""


class AlmacenVentas:
    """Sink idempotente: upsert por ``aggregate_id`` con versión (tickets, pane) monótona."""

    def __init__(self, ruta: str | Path = ":memory:") -> None:
        if ruta != ":memory:":
            Path(ruta).parent.mkdir(parents=True, exist_ok=True)
        self.conexion = sqlite3.connect(str(ruta), isolation_level=None)
        self.conexion.row_factory = sqlite3.Row
        self.conexion.executescript(DDL)
        self.mensajes_vistos = 0
        self.aplicados = 0
        self.ignorados = 0

    def upsert(self, agregado: dict[str, Any]) -> bool:
        self.mensajes_vistos += 1
        fila = {
            "aggregate_id": agregado["aggregate_id"],
            "store_id": agregado["store_id"],
            "window_start": agregado["window_start"],
            "window_end": agregado["window_end"],
            "ticket_count": int(agregado["ticket_count"]),
            "total_amount_gs": int(agregado["total_amount_gs"]),
            "items": int(agregado.get("items", 0)),
            "avg_ticket_gs": int(agregado.get("avg_ticket_gs", 0)),
            "by_payment_gs": json.dumps(agregado.get("by_payment_gs", {}), sort_keys=True),
            "duplicates_discarded": int(agregado.get("duplicates_discarded", 0)),
            "pane_index": int(agregado.get("pane_index", 0)),
            "pane_timing": str(agregado.get("pane_timing", "UNKNOWN")),
            "is_last": int(bool(agregado.get("is_last", False))),
            "emitted_at": str(agregado.get("emitted_at", "")),
            "updated_at": datetime.now(UTC).isoformat(timespec="milliseconds"),
        }
        cursor = self.conexion.execute(UPSERT, fila)
        aplicado = cursor.rowcount > 0
        if aplicado:
            self.aplicados += 1
        else:
            self.ignorados += 1
        return aplicado

    def registrar_rechazo(self, rechazo: dict[str, Any]) -> None:
        self.conexion.execute(
            "INSERT INTO rechazos (motivo, detalle, key, event_id, event_time, amount_gs, "
            "rechazado_en, mensaje_original) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                rechazo.get("motivo", "desconocido"),
                rechazo.get("detalle"),
                rechazo.get("key"),
                rechazo.get("event_id"),
                rechazo.get("event_time"),
                rechazo.get("amount_gs"),
                rechazo.get("rechazado_en", datetime.now(UTC).isoformat()),
                rechazo.get("mensaje_original"),
            ),
        )

    def guardar_offset(self, topico: str, particion: int, offset: int) -> None:
        self.conexion.execute(
            "INSERT INTO offsets_consumidos (topico, particion, offset) VALUES (?, ?, ?) "
            "ON CONFLICT(topico, particion) DO UPDATE SET offset = excluded.offset",
            (topico, particion, offset),
        )

    def filas(self, *, store_id: str | None = None) -> list[dict[str, Any]]:
        consulta = "SELECT * FROM ventas_por_ventana"
        parametros: tuple[Any, ...] = ()
        if store_id is not None:
            consulta += " WHERE store_id = ?"
            parametros = (store_id,)
        consulta += " ORDER BY window_start, store_id"
        return [dict(fila) for fila in self.conexion.execute(consulta, parametros)]

    def rechazos(self) -> list[dict[str, Any]]:
        return [dict(f) for f in self.conexion.execute("SELECT * FROM rechazos ORDER BY id")]

    def resumen(self) -> dict[str, Any]:
        agregados = self.conexion.execute("SELECT COUNT(*) FROM ventas_por_ventana").fetchone()[0]
        panes = self.conexion.execute(
            "SELECT COALESCE(SUM(panes_recibidos), 0) FROM ventas_por_ventana"
        ).fetchone()[0]
        rechazos = self.conexion.execute(
            "SELECT motivo, COUNT(*) FROM rechazos GROUP BY motivo ORDER BY motivo"
        ).fetchall()
        resumen: dict[str, Any] = {
            "filas_ventas_por_ventana": agregados,
            "panes_aplicados_acumulados": panes,
            "rechazos_por_motivo": {motivo: total for motivo, total in rechazos},
        }
        if self.mensajes_vistos:  # contadores de esta sesión del proceso
            resumen.update(
                mensajes_vistos=self.mensajes_vistos,
                upserts_aplicados=self.aplicados,
                panes_ignorados=self.ignorados,
            )
        return resumen

    def cerrar(self) -> None:
        self.conexion.close()


def formatear_tabla(filas: list[dict[str, Any]], columnas: list[str]) -> str:
    if not filas:
        return "(sin filas)"
    anchos = {c: max(len(c), *(len(str(f.get(c, ""))) for f in filas)) for c in columnas}
    cabecera = " | ".join(c.ljust(anchos[c]) for c in columnas)
    separador = "-+-".join("-" * anchos[c] for c in columnas)
    cuerpo = [" | ".join(str(f.get(c, "")).ljust(anchos[c]) for c in columnas) for f in filas]
    return "\n".join([cabecera, separador, *cuerpo])


COLUMNAS_REPORTE = [
    "aggregate_id",
    "window_start",
    "window_end",
    "ticket_count",
    "total_amount_gs",
    "duplicates_discarded",
    "pane_index",
    "pane_timing",
    "panes_recibidos",
]
COLUMNAS_RECHAZOS = ["id", "motivo", "detalle", "event_id", "event_time", "rechazado_en"]


def imprimir_reporte(almacen: AlmacenVentas) -> None:
    print("\n== ventas_por_ventana (último pane por sucursal y ventana) ==")
    print(formatear_tabla(almacen.filas(), COLUMNAS_REPORTE))
    print("\n== rechazos (DLQ) ==")
    print(formatear_tabla(almacen.rechazos(), COLUMNAS_RECHAZOS))
    print("\n== resumen ==")
    print(json.dumps(almacen.resumen(), ensure_ascii=False, indent=2))


def construir_consumidor(settings: Settings, *, group_id: str, desde_inicio: bool):
    from confluent_kafka import Consumer

    consumidor = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest" if desde_inicio else "latest",
            # El commit se hace después de persistir: entrega al menos una vez,
            # y el upsert idempotente absorbe cualquier reentrega.
            "enable.auto.commit": False,
        }
    )
    consumidor.subscribe([settings.topico_agregados, settings.topico_dlq])
    return consumidor


def procesar_mensaje(almacen: AlmacenVentas, settings: Settings, mensaje) -> str:
    topico = mensaje.topic()
    try:
        cuerpo = json.loads(mensaje.value().decode())
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        log.warning("mensaje ilegible en %s: %s", topico, error)
        return "ilegible"
    if topico == settings.topico_dlq:
        almacen.registrar_rechazo(cuerpo)
        log.info(
            "DLQ motivo=%s event_id=%s detalle=%s",
            cuerpo.get("motivo"),
            cuerpo.get("event_id"),
            cuerpo.get("detalle"),
        )
        return "rechazo"
    aplicado = almacen.upsert(cuerpo)
    log.info(
        "%s %s pane=%s %s tickets=%s total=%s dup_descartados=%s",
        "UPSERT  " if aplicado else "IGNORADO",
        cuerpo.get("aggregate_id"),
        cuerpo.get("pane_index"),
        cuerpo.get("pane_timing"),
        cuerpo.get("ticket_count"),
        cuerpo.get("total_amount_gs"),
        cuerpo.get("duplicates_discarded"),
    )
    return "upsert" if aplicado else "ignorado"


def consumir(
    almacen: AlmacenVentas,
    settings: Settings,
    *,
    group_id: str,
    desde_inicio: bool,
    timeout_inactividad: float,
    parar: Event,
    max_mensajes: int | None = None,
) -> dict[str, int]:
    from confluent_kafka import KafkaError

    consumidor = construir_consumidor(settings, group_id=group_id, desde_inicio=desde_inicio)
    contadores: dict[str, int] = {}
    ultimo = time.monotonic()
    procesados = 0
    try:
        while not parar.is_set():
            if timeout_inactividad > 0 and time.monotonic() - ultimo > timeout_inactividad:
                log.info("sin mensajes durante %.0f s; terminando", timeout_inactividad)
                break
            if max_mensajes is not None and procesados >= max_mensajes:
                break
            mensaje = consumidor.poll(1.0)
            if mensaje is None:
                continue
            if mensaje.error():
                if mensaje.error().code() != KafkaError._PARTITION_EOF:
                    log.error("error de consumo: %s", mensaje.error())
                continue
            ultimo = time.monotonic()
            resultado = procesar_mensaje(almacen, settings, mensaje)
            almacen.guardar_offset(mensaje.topic(), mensaje.partition(), mensaje.offset())
            consumidor.commit(message=mensaje, asynchronous=True)
            contadores[resultado] = contadores.get(resultado, 0) + 1
            procesados += 1
    finally:
        consumidor.close()
    return contadores


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--group-id", default="materializador-ventas-v1")
    parser.add_argument(
        "--timeout", type=float, default=0, help="segundos sin mensajes para terminar"
    )
    parser.add_argument("--desde-ultimo", action="store_true", help="no releer desde el inicio")
    parser.add_argument("--reporte", action="store_true", help="solo imprime las tablas y sale")
    parser.add_argument("--reporte-al-final", action="store_true")
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    almacen = AlmacenVentas(settings.sqlite_path)
    if args.reporte:
        imprimir_reporte(almacen)
        return
    parar = Event()
    signal.signal(signal.SIGTERM, lambda *_: parar.set())
    signal.signal(signal.SIGINT, lambda *_: parar.set())
    log.info(
        "materializador iniciado: sqlite=%s tópicos=%s,%s",
        settings.sqlite_path,
        settings.topico_agregados,
        settings.topico_dlq,
    )
    contadores = consumir(
        almacen,
        settings,
        group_id=args.group_id,
        desde_inicio=not args.desde_ultimo,
        timeout_inactividad=args.timeout,
        parar=parar,
    )
    log.info("consumo terminado: %s", json.dumps(contadores))
    if args.reporte_al_final:
        imprimir_reporte(almacen)
    print(json.dumps({"contadores": contadores, **almacen.resumen()}, ensure_ascii=False))
    almacen.cerrar()


if __name__ == "__main__":
    main()
