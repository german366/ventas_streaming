import json

import pytest

from tests.conftest import evento
from ventas_streaming.contratos import (
    EventoInvalido,
    aggregate_id,
    codificar_agregado,
    codificar_evento,
    decodificar_evento,
    upgrade_evento,
    validar_evento,
)


def test_evento_valido_v2_se_decodifica_sin_cambios():
    original = evento("S1-pos-01-000001", "2026-09-21T10:00:05.000Z")
    decodificado = decodificar_evento(codificar_evento(original))
    assert decodificado == original


def test_evento_v1_se_actualiza_al_esquema_actual_con_valores_por_defecto():
    v1 = evento("S1-pos-01-000001", "2026-09-21T10:00:05.000Z", version=1)
    assert "channel" not in v1["payload"]
    actualizado = upgrade_evento(v1)
    assert actualizado["schema_version"] == 2
    assert actualizado["payload"]["channel"] == "store"
    assert actualizado["produced_at"] is None
    assert validar_evento(v1)["schema_version"] == 2


@pytest.mark.parametrize(
    ("mutacion", "motivo"),
    [
        (lambda e: e.pop("payload"), "campos_faltantes"),
        (lambda e: e.__setitem__("schema_version", 99), "version_no_soportada"),
        (lambda e: e.__setitem__("event_type", "sale.cancelled"), "event_type_desconocido"),
        (lambda e: e.__setitem__("event_time", "ayer"), "event_time_invalido"),
        (lambda e: e["payload"].__setitem__("amount_gs", -5), "monto_invalido"),
        (lambda e: e["payload"].__setitem__("items", 0), "items_invalido"),
        (lambda e: e["payload"].__setitem__("payment", "cripto"), "metodo_pago_desconocido"),
        (lambda e: e["payload"].__setitem__("store_id", "S9"), "clave_inconsistente"),
        (lambda e: e["payload"].pop("ticket_id"), "payload_incompleto"),
    ],
)
def test_eventos_invalidos_reportan_un_motivo_estable(mutacion, motivo):
    roto = evento("S1-pos-01-000001", "2026-09-21T10:00:05.000Z")
    mutacion(roto)
    with pytest.raises(EventoInvalido) as excinfo:
        validar_evento(roto)
    assert excinfo.value.motivo == motivo


def test_json_corrupto_es_rechazado():
    with pytest.raises(EventoInvalido) as excinfo:
        decodificar_evento(b'{"schema_version": 2')
    assert excinfo.value.motivo == "json_invalido"


def test_clave_del_agregado_es_estable_por_sucursal_y_ventana():
    clave = aggregate_id("S2", "2026-09-21T10:00:00.000Z")
    assert clave == "S2|2026-09-21T10:00:00.000Z"
    key, value = codificar_agregado({"aggregate_id": clave, "ticket_count": 3})
    assert key == clave.encode()
    assert json.loads(value) == {"aggregate_id": clave, "ticket_count": 3}
