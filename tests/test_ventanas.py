"""Tiempo de evento, ventanas, panes y datos tardíos con TestStream (DirectRunner streaming)."""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.utils.timestamp import Timestamp

from tests.conftest import AHORA, evento, registro
from ventas_streaming.contratos import epoch_seconds
from ventas_streaming.transformaciones import construir_nucleo


def ts(iso: str) -> Timestamp:
    return Timestamp(epoch_seconds(iso))


def opciones_streaming() -> PipelineOptions:
    opciones = PipelineOptions()
    opciones.view_as(StandardOptions).streaming = True
    return opciones


def resumen(agregados):
    return agregados | beam.Map(
        lambda a: (
            a["aggregate_id"],
            a["pane_timing"],
            a["pane_index"],
            a["ticket_count"],
            a["total_amount_gs"],
        )
    )


def test_pane_on_time_pane_late_y_descarte_fuera_de_lateness(settings):
    # Ventana [10:00, 10:01); lateness 30 s => se aceptan correcciones hasta WM = 10:01:30.
    e1 = evento("S1-1", "2026-09-21T10:00:10Z", monto=100_000)
    e2 = evento("S1-2", "2026-09-21T10:00:20Z", monto=50_000)
    tardio_ok = evento("S1-3", "2026-09-21T10:00:30Z", monto=25_000)
    demasiado_tardio = evento("S1-4", "2026-09-21T10:00:40Z", monto=999_999)
    otra = evento("S2-1", "2026-09-21T10:01:05Z", store="S2", monto=10_000)
    stream = (
        TestStream()
        .advance_watermark_to(ts("2026-09-21T10:00:00Z"))
        .add_elements([registro(e1), registro(e2)])
        .advance_watermark_to(ts("2026-09-21T10:01:05Z"))  # cierra la ventana: pane ON_TIME
        .add_elements([registro(tardio_ok)])  # llega tarde pero dentro de la lateness: pane LATE
        .add_elements([registro(otra)])
        .advance_watermark_to(ts("2026-09-21T10:01:40Z"))  # WM > fin + lateness: ventana expirada
        .add_elements([registro(demasiado_tardio)])  # Beam lo descarta
        .advance_watermark_to_infinity()
    )
    with TestPipeline(options=opciones_streaming()) as p:
        agregados, _, _ = construir_nucleo(p | stream, settings, ahora_fn=lambda: AHORA)
        assert_that(
            resumen(agregados),
            equal_to(
                [
                    ("S1|2026-09-21T10:00:00.000Z", "ON_TIME", 0, 2, 150_000),
                    ("S1|2026-09-21T10:00:00.000Z", "LATE", 1, 3, 175_000),
                    ("S2|2026-09-21T10:01:00.000Z", "ON_TIME", 0, 1, 10_000),
                ]
            ),
        )


def test_duplicado_tardio_no_altera_el_total_pero_reemite_el_pane(settings):
    e1 = evento("S1-1", "2026-09-21T10:00:10Z", monto=100_000)
    stream = (
        TestStream()
        .advance_watermark_to(ts("2026-09-21T10:00:00Z"))
        .add_elements([registro(e1), registro(e1)])  # duplicado inmediato
        .advance_watermark_to(ts("2026-09-21T10:01:05Z"))
        .add_elements([registro(e1)])  # reenvío tardío del mismo event_id
        .advance_watermark_to_infinity()
    )
    with TestPipeline(options=opciones_streaming()) as p:
        agregados, _, _ = construir_nucleo(p | stream, settings, ahora_fn=lambda: AHORA)
        assert_that(
            agregados
            | beam.Map(lambda a: (a["pane_timing"], a["ticket_count"], a["duplicates_discarded"])),
            equal_to([("ON_TIME", 1, 1), ("LATE", 1, 2)]),
        )


def test_evento_desordenado_dentro_de_la_ventana_se_absorbe_sin_pane_extra(settings):
    e_nuevo = evento("S1-2", "2026-09-21T10:00:50Z", monto=30_000)
    e_viejo = evento("S1-1", "2026-09-21T10:00:05Z", monto=20_000)
    stream = (
        TestStream()
        .advance_watermark_to(ts("2026-09-21T10:00:00Z"))
        .add_elements([registro(e_nuevo)])
        .advance_watermark_to(ts("2026-09-21T10:00:55Z"))  # aún no cierra la ventana
        .add_elements([registro(e_viejo)])  # fuera de orden, pero a tiempo
        .advance_watermark_to_infinity()
    )
    with TestPipeline(options=opciones_streaming()) as p:
        agregados, _, _ = construir_nucleo(p | stream, settings, ahora_fn=lambda: AHORA)
        assert_that(
            resumen(agregados), equal_to([("S1|2026-09-21T10:00:00.000Z", "ON_TIME", 0, 2, 50_000)])
        )
