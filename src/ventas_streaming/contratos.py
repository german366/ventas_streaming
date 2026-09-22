"""Contrato de eventos de venta y de agregados de salida.

Evento de entrada (``ventas.confirmadas.v1``), versión 2 del esquema::

    {
      "schema_version": 2,
      "event_id":   "S2-pos07-000123",          # estable, generado en el POS
      "event_type": "sale.confirmed",
      "event_time": "2026-09-21T14:23:05.120Z", # reloj del POS al confirmar el pago
      "key":        "S2",                        # store_id: clave de negocio y de partición
      "produced_at":"2026-09-21T14:23:06.004Z", # reloj del productor al publicar (opcional)
      "payload": {
        "store_id": "S2", "pos_id": "pos-07", "ticket_id": "T-000123",
        "amount_gs": 415000, "items": 3, "payment": "card",
        "channel": "store"                       # nuevo en v2, opcional
      }
    }

Estrategia de evolución: cambios aditivos y compatibles hacia atrás. Un consumidor
que entiende la versión N acepta versiones menores completando valores por defecto
(``upgrade_evento``) y rechaza versiones mayores que la suya (van a la DLQ). El
``schema_version`` viaja también como cabecera Kafka para poder filtrar sin
deserializar.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

SCHEMA_VERSION_ACTUAL = 2
VERSIONES_SOPORTADAS = (1, 2)
EVENT_TYPE_VENTA = "sale.confirmed"
METODOS_PAGO = ("cash", "card", "qr", "transfer")
CAMPOS_REQUERIDOS = ("schema_version", "event_id", "event_type", "event_time", "key", "payload")
PAYLOAD_REQUERIDO = ("store_id", "pos_id", "ticket_id", "amount_gs", "items", "payment")

SCHEMA_VERSION_AGREGADO = 1


class EventoInvalido(ValueError):
    """El evento no cumple el contrato; lleva un ``motivo`` corto y estable."""

    def __init__(self, motivo: str, detalle: str = "") -> None:
        super().__init__(f"{motivo}: {detalle}" if detalle else motivo)
        self.motivo = motivo
        self.detalle = detalle


def iso_utc(valor: datetime) -> str:
    """Serializa un datetime como ISO-8601 UTC con sufijo Z y milisegundos."""
    if valor.tzinfo is None:
        valor = valor.replace(tzinfo=UTC)
    return valor.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_iso_utc(valor: str) -> datetime:
    parsed = datetime.fromisoformat(valor.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def epoch_seconds(valor: str) -> float:
    return parse_iso_utc(valor).timestamp()


def upgrade_evento(evento: dict[str, Any]) -> dict[str, Any]:
    """Lleva un evento de una versión anterior al esquema actual (cambios aditivos)."""
    version = int(evento.get("schema_version", 0))
    if version not in VERSIONES_SOPORTADAS:
        raise EventoInvalido("version_no_soportada", f"schema_version={version!r}")
    actualizado = {**evento, "payload": dict(evento.get("payload") or {})}
    if version < 2:
        actualizado["payload"].setdefault("channel", "store")
        actualizado.setdefault("produced_at", None)
    actualizado["schema_version"] = SCHEMA_VERSION_ACTUAL
    return actualizado


def validar_evento(evento: dict[str, Any]) -> dict[str, Any]:
    """Valida estructura y semántica; devuelve el evento normalizado al esquema actual."""
    if not isinstance(evento, dict):
        raise EventoInvalido("no_es_objeto", type(evento).__name__)
    faltantes = [campo for campo in CAMPOS_REQUERIDOS if campo not in evento]
    if faltantes:
        raise EventoInvalido("campos_faltantes", ",".join(faltantes))
    if not isinstance(evento["schema_version"], int) or isinstance(evento["schema_version"], bool):
        raise EventoInvalido("version_no_soportada", repr(evento["schema_version"]))
    evento = upgrade_evento(evento)
    if evento["event_type"] != EVENT_TYPE_VENTA:
        raise EventoInvalido("event_type_desconocido", str(evento["event_type"]))
    if not isinstance(evento["event_id"], str) or not evento["event_id"]:
        raise EventoInvalido("event_id_invalido", repr(evento["event_id"]))
    try:
        parse_iso_utc(str(evento["event_time"]))
    except ValueError as error:
        raise EventoInvalido("event_time_invalido", str(evento["event_time"])) from error
    payload = evento["payload"]
    if not isinstance(payload, dict):
        raise EventoInvalido("payload_invalido", type(payload).__name__)
    faltantes = [campo for campo in PAYLOAD_REQUERIDO if campo not in payload]
    if faltantes:
        raise EventoInvalido("payload_incompleto", ",".join(faltantes))
    if payload["store_id"] != evento["key"]:
        raise EventoInvalido(
            "clave_inconsistente", f"key={evento['key']} store={payload['store_id']}"
        )
    monto = payload["amount_gs"]
    if isinstance(monto, bool) or not isinstance(monto, int | float) or monto <= 0:
        raise EventoInvalido("monto_invalido", repr(monto))
    items = payload["items"]
    if isinstance(items, bool) or not isinstance(items, int) or items <= 0:
        raise EventoInvalido("items_invalido", repr(items))
    if payload["payment"] not in METODOS_PAGO:
        raise EventoInvalido("metodo_pago_desconocido", str(payload["payment"]))
    return evento


def codificar_evento(evento: dict[str, Any]) -> bytes:
    return json.dumps(evento, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def decodificar_evento(payload: bytes | str) -> dict[str, Any]:
    """Deserializa y valida; lanza ``EventoInvalido`` ante cualquier problema."""
    try:
        texto = payload.decode() if isinstance(payload, bytes) else payload
        crudo = json.loads(texto)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise EventoInvalido("json_invalido", str(error)[:80]) from error
    return validar_evento(crudo)


def aggregate_id(store_id: str, window_start: str) -> str:
    """Clave estable del resultado: una fila por sucursal y ventana."""
    return f"{store_id}|{window_start}"


def codificar_agregado(agregado: dict[str, Any]) -> tuple[bytes, bytes]:
    return (
        agregado["aggregate_id"].encode(),
        json.dumps(agregado, sort_keys=True, separators=(",", ":")).encode(),
    )


def codificar_rechazo(rechazo: dict[str, Any]) -> tuple[bytes, bytes]:
    clave = rechazo.get("key") or rechazo.get("event_id") or "sin-clave"
    return (
        str(clave).encode(),
        json.dumps(rechazo, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(),
    )
