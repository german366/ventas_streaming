from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from ventas_streaming.config import Settings
from ventas_streaming.contratos import codificar_evento

AHORA = datetime(2026, 9, 21, 10, 10, 0, tzinfo=UTC)


@pytest.fixture
def settings() -> Settings:
    return Settings(window_seconds=60, allowed_lateness_seconds=30, early_firing_seconds=10)


def evento(
    event_id: str,
    event_time: str,
    *,
    store: str = "S1",
    monto: int = 100_000,
    pago: str = "card",
    version: int = 2,
    items: int = 2,
) -> dict[str, Any]:
    payload = {
        "store_id": store,
        "pos_id": "pos-01",
        "ticket_id": f"T-{event_id}",
        "amount_gs": monto,
        "items": items,
        "payment": pago,
    }
    if version >= 2:
        payload["channel"] = "store"
    return {
        "schema_version": version,
        "event_id": event_id,
        "event_type": "sale.confirmed",
        "event_time": event_time,
        "key": store,
        "payload": payload,
    }


def registro(ev: dict[str, Any]) -> tuple[bytes, bytes]:
    """Par (key, value) tal como lo entrega KafkaIO."""
    return (ev["key"].encode(), codificar_evento(ev))
