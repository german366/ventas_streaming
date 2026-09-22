"""Lógica de validación, transformación y agregación con TestPipeline (DirectRunner, lote)."""

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from tests.conftest import AHORA, evento, registro
from ventas_streaming.transformaciones import VentasPorSucursalCombineFn, construir_nucleo


def ahora_fija():
    return AHORA


def primer_pane(agregados):
    """En lote, con lateness > 0, el runner emite además un pane de cierre (is_last)."""
    return agregados | beam.Filter(lambda a: a["is_first"])


def test_combinefn_deduplica_por_event_id_y_suma_incrementalmente():
    fn = VentasPorSucursalCombineFn()
    e1 = evento("a", "2026-09-21T10:00:01Z", monto=100_000, pago="card")
    e2 = evento("b", "2026-09-21T10:00:02Z", monto=50_000, pago="cash")
    acc = fn.create_accumulator()
    for ev in (e1, e2, e1):
        acc = fn.add_input(acc, ev)
    salida = fn.extract_output(acc)
    assert salida["ticket_count"] == 2
    assert salida["total_amount_gs"] == 150_000
    assert salida["by_payment_gs"] == {"card": 100_000, "cash": 50_000}
    assert salida["duplicates_discarded"] == 1
    assert salida["avg_ticket_gs"] == 75_000


def test_merge_de_acumuladores_descuenta_solapamientos():
    fn = VentasPorSucursalCombineFn()
    e1 = evento("a", "2026-09-21T10:00:01Z", monto=100_000)
    e2 = evento("b", "2026-09-21T10:00:02Z", monto=50_000)
    acc1 = fn.add_input(fn.add_input(fn.create_accumulator(), e1), e2)
    acc2 = fn.add_input(fn.create_accumulator(), e1)
    fusion = fn.merge_accumulators([acc1, acc2])
    salida = fn.extract_output(fusion)
    assert salida["ticket_count"] == 2
    assert salida["total_amount_gs"] == 150_000
    assert salida["duplicates_discarded"] == 1


def test_nucleo_agrega_por_sucursal_y_ventana_descartando_duplicados(settings):
    eventos = [
        evento("S1-1", "2026-09-21T10:00:05Z", store="S1", monto=100_000),
        evento("S1-2", "2026-09-21T10:00:15Z", store="S1", monto=200_000),
        evento("S1-1", "2026-09-21T10:00:05Z", store="S1", monto=100_000),  # duplicado
        evento("S2-1", "2026-09-21T10:00:20Z", store="S2", monto=70_000),
        evento("S1-3", "2026-09-21T10:01:02Z", store="S1", monto=30_000),  # ventana siguiente
    ]
    with TestPipeline() as p:
        registros = p | beam.Create([registro(e) for e in eventos])
        agregados, invalidos, muy_tardios = construir_nucleo(
            registros, settings, triggers_streaming=False, ahora_fn=ahora_fija
        )
        filas = primer_pane(agregados) | beam.Map(
            lambda a: (
                a["aggregate_id"],
                a["ticket_count"],
                a["total_amount_gs"],
                a["duplicates_discarded"],
            )
        )
        assert_that(
            filas,
            equal_to(
                [
                    ("S1|2026-09-21T10:00:00.000Z", 2, 300_000, 1),
                    ("S2|2026-09-21T10:00:00.000Z", 1, 70_000, 0),
                    ("S1|2026-09-21T10:01:00.000Z", 1, 30_000, 0),
                ]
            ),
        )
        assert_that(invalidos, equal_to([]), label="sin invalidos")
        assert_that(muy_tardios, equal_to([]), label="sin muy tardios")


def test_nucleo_separa_invalidos_con_motivo(settings):
    futuro = evento("S1-f", "2026-09-21T10:30:00Z")  # 20 min adelantado respecto de AHORA
    sin_monto = evento("S1-m", "2026-09-21T10:00:05Z")
    sin_monto["payload"].pop("amount_gs")
    registros_entrada = [
        registro(evento("S1-ok", "2026-09-21T10:00:05Z")),
        (b"S1", b"esto no es json"),
        registro(futuro),
        registro(sin_monto),
    ]
    with TestPipeline() as p:
        registros = p | beam.Create(registros_entrada)
        agregados, invalidos, _ = construir_nucleo(
            registros, settings, triggers_streaming=False, ahora_fn=ahora_fija
        )
        assert_that(
            invalidos | beam.Map(lambda r: r["motivo"]),
            equal_to(["json_invalido", "reloj_futuro", "payload_incompleto"]),
            label="motivos",
        )
        assert_that(
            primer_pane(agregados) | beam.Map(lambda a: a["ticket_count"]),
            equal_to([1]),
            label="validos",
        )


def test_nucleo_desvia_demasiado_tardios_segun_watermark_heuristico(settings):
    # Horizonte = 60 + 30 = 90 s. Tras ver 10:05:00, un evento de 10:00:00 queda 210 s fuera.
    registros_entrada = [
        registro(evento("S1-nuevo", "2026-09-21T10:05:00Z")),
        registro(evento("S1-viejo", "2026-09-21T10:00:00Z")),
        registro(evento("S1-borde", "2026-09-21T10:03:31Z")),  # 89 s: dentro del horizonte
    ]
    with TestPipeline() as p:
        registros = p | beam.Create(registros_entrada)
        agregados, _, muy_tardios = construir_nucleo(
            registros, settings, triggers_streaming=False, ahora_fn=ahora_fija
        )
        assert_that(
            muy_tardios | beam.Map(lambda r: (r["motivo"], r["event_id"])),
            equal_to([("demasiado_tardio", "S1-viejo")]),
            label="muy tardios",
        )
        assert_that(
            primer_pane(agregados) | beam.Map(lambda a: (a["window_start"], a["ticket_count"])),
            equal_to([("2026-09-21T10:05:00.000Z", 1), ("2026-09-21T10:03:00.000Z", 1)]),
            label="agregados",
        )
