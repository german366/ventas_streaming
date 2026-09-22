from ventas_streaming.materializador import AlmacenVentas


def agregado(pane_index: int, tickets: int, timing: str = "ON_TIME") -> dict:
    return {
        "aggregate_id": "S1|2026-09-21T10:00:00.000Z",
        "store_id": "S1",
        "window_start": "2026-09-21T10:00:00.000Z",
        "window_end": "2026-09-21T10:01:00.000Z",
        "ticket_count": tickets,
        "total_amount_gs": tickets * 10_000,
        "items": tickets,
        "avg_ticket_gs": 10_000,
        "by_payment_gs": {"card": tickets * 10_000},
        "duplicates_discarded": 0,
        "pane_index": pane_index,
        "pane_timing": timing,
        "is_last": False,
        "emitted_at": "2026-09-21T10:01:02.000Z",
    }


def test_upsert_reemplaza_por_pane_mas_reciente_sin_duplicar_filas():
    almacen = AlmacenVentas()
    assert almacen.upsert(agregado(0, 3, "EARLY"))
    assert almacen.upsert(agregado(1, 5))
    filas = almacen.filas()
    assert len(filas) == 1
    assert filas[0]["ticket_count"] == 5
    assert filas[0]["pane_timing"] == "ON_TIME"
    assert filas[0]["panes_recibidos"] == 2


def test_reentrega_del_mismo_pane_y_pane_viejo_son_idempotentes():
    almacen = AlmacenVentas()
    almacen.upsert(agregado(2, 7, "LATE"))
    assert almacen.upsert(agregado(2, 7, "LATE"))  # misma versión: no cambia el estado
    assert not almacen.upsert(agregado(1, 5))  # pane anterior reproducido: ignorado
    filas = almacen.filas()
    assert len(filas) == 1
    assert filas[0]["ticket_count"] == 7
    assert almacen.resumen()["panes_ignorados"] == 1


def test_releer_todo_el_topico_produce_el_mismo_estado_final():
    secuencia = [agregado(0, 3, "EARLY"), agregado(1, 5), agregado(2, 6, "LATE")]
    primera = AlmacenVentas()
    for a in secuencia:
        primera.upsert(a)
    segunda = AlmacenVentas()
    for a in secuencia + secuencia:  # replay completo
        segunda.upsert(a)
    campos = ("aggregate_id", "ticket_count", "total_amount_gs", "pane_index", "pane_timing")
    assert [{c: f[c] for c in campos} for f in primera.filas()] == [
        {c: f[c] for c in campos} for f in segunda.filas()
    ]


def test_pane_vacio_con_el_mismo_indice_no_borra_el_resultado():
    """Al expirar una ventana el runner puede emitir un pane vacío con pane_index repetido."""
    almacen = AlmacenVentas()
    almacen.upsert(agregado(0, 15))
    fantasma = agregado(0, 0)
    assert not almacen.upsert(fantasma)
    assert almacen.filas()[0]["ticket_count"] == 15


def test_tras_un_reinicio_del_pipeline_un_pane_con_mas_tickets_se_aplica_aunque_el_indice_baje():
    almacen = AlmacenVentas()
    almacen.upsert(agregado(4, 20, "LATE"))
    assert almacen.upsert(agregado(0, 21, "LATE"))  # pane_index reiniciado, pero con la corrección
    assert almacen.filas()[0]["ticket_count"] == 21


def test_rechazos_se_registran_una_sola_vez():
    almacen = AlmacenVentas()
    rechazo = {
        "motivo": "monto_invalido",
        "detalle": "-5",
        "key": "S1",
        "event_id": "S1-x",
        "rechazado_en": "2026-09-21T10:00:00Z",
        "mensaje_original": "{}",
    }
    almacen.registrar_rechazo(rechazo)
    almacen.registrar_rechazo(rechazo)
    assert len(almacen.rechazos()) == 1
    assert almacen.resumen()["rechazos_por_motivo"] == {"monto_invalido": 1}
