import json
import random
from datetime import UTC, datetime

from ventas_streaming.config import Settings
from ventas_streaming.contratos import decodificar_evento, validar_evento
from ventas_streaming.productor import (
    TIPOS_INVALIDOS,
    GeneradorVentas,
    Perturbaciones,
    Simulador,
    escenario_demo,
    event_time_demasiado_tardio,
    event_time_tardio_aceptable,
    evento_invalido,
)


def test_generador_es_determinista_y_produce_eventos_validos():
    t = datetime(2026, 9, 21, 10, 0, tzinfo=UTC)
    ga, gb = GeneradorVentas(semilla=3), GeneradorVentas(semilla=3)
    a = [ga.siguiente(t) for _ in range(20)]
    b = [gb.siguiente(t) for _ in range(20)]
    assert a == b
    assert len({e["event_id"] for e in a}) == 20
    for e in a:
        validar_evento(e)
        assert e["key"] == e["payload"]["store_id"]


def test_eventos_invalidos_por_tipo_son_rechazados_por_el_contrato():
    base = GeneradorVentas().siguiente(datetime(2026, 9, 21, 10, 0, tzinfo=UTC))
    for tipo in TIPOS_INVALIDOS:
        if tipo == "reloj_futuro":
            continue  # es válido estructuralmente; lo rechaza el pipeline por plausibilidad
        payload = evento_invalido(base, tipo)
        try:
            decodificar_evento(payload)
        except Exception as error:  # noqa: BLE001
            assert error.__class__.__name__ == "EventoInvalido"
        else:
            raise AssertionError(f"{tipo} debería ser inválido")


def test_tardio_aceptable_cae_en_la_ventana_anterior_dentro_de_la_lateness():
    settings = Settings(window_seconds=60, allowed_lateness_seconds=30)
    rng = random.Random(1)
    ahora = datetime(2026, 9, 21, 10, 1, 18, tzinfo=UTC)  # 18 s después del fin de [10:00,10:01)
    tardio = event_time_tardio_aceptable(ahora, settings, rng)
    assert tardio is not None
    assert (
        datetime(2026, 9, 21, 10, 0, tzinfo=UTC)
        <= tardio
        < datetime(2026, 9, 21, 10, 1, tzinfo=UTC)
    )
    temprano = datetime(2026, 9, 21, 10, 1, 5, tzinfo=UTC)  # el watermark aún no cerró la ventana
    assert event_time_tardio_aceptable(temprano, settings, rng) is None
    demasiado = datetime(2026, 9, 21, 10, 1, 40, tzinfo=UTC)  # ya pasó la lateness
    assert event_time_tardio_aceptable(demasiado, settings, rng) is None
    muy = event_time_demasiado_tardio(ahora, settings, rng)
    assert (ahora - muy).total_seconds() > settings.horizonte_tardios_seconds


def test_simulador_etiqueta_cada_perturbacion():
    settings = Settings(window_seconds=60, allowed_lateness_seconds=30)
    reloj = [datetime(2026, 9, 21, 10, 1, 18, tzinfo=UTC)]
    sim = Simulador(
        settings,
        GeneradorVentas(1),
        Perturbaciones(duplicados=1, desorden=0, tardios=1, muy_tardios=1, invalidos=1),
        semilla=2,
        ahora_fn=lambda: reloj[0],
    )
    etiquetas = {m.etiqueta.split(":")[0] for m in sim.paso()} | {
        m.etiqueta.split(":")[0] for m in sim.drenar()
    }
    assert {"OK", "DUPLICADO", "TARDIO", "MUY_TARDIO", "INVALIDO"} <= etiquetas


def test_escenario_demo_cubre_todos_los_casos_adversos():
    settings = Settings(window_seconds=60, allowed_lateness_seconds=30)
    reloj = [datetime(2026, 9, 21, 10, 0, 30, tzinfo=UTC)]

    def avanzar():
        from datetime import timedelta

        reloj[0] = reloj[0] + timedelta(seconds=1)
        return reloj[0]

    class VigilanteFalso:
        def __init__(self):
            self.consultas = 0

        def pane_on_time_visto(self, window_end):
            self.consultas += 1
            return self.consultas >= 3  # el pane ON_TIME aparece a la tercera consulta

    sim = Simulador(settings, GeneradorVentas(1), Perturbaciones(), semilla=2, ahora_fn=avanzar)
    vigilante = VigilanteFalso()
    mensajes = [m for _, lote in escenario_demo(sim, vigilante) for m in lote]
    assert vigilante.consultas == 3
    etiquetas = [m.etiqueta.split(":")[0] for m in mensajes]
    assert etiquetas.count("DUPLICADO") == 3
    assert etiquetas.count("DESORDEN") == 5
    assert etiquetas.count("TARDIO") == 1
    assert etiquetas.count("MUY_TARDIO") == 1
    assert etiquetas.count("INVALIDO") == len(TIPOS_INVALIDOS)
    duplicados = [m for m in mensajes if m.etiqueta == "DUPLICADO"]
    ids_ok = {json.loads(m.value)["event_id"] for m in mensajes if m.etiqueta == "OK"}
    assert all(json.loads(m.value)["event_id"] in ids_ok for m in duplicados)


def test_escenario_demo_sin_vigilante_usa_la_heuristica_de_reloj():
    from datetime import timedelta

    settings = Settings(window_seconds=60, allowed_lateness_seconds=30)
    reloj = [datetime(2026, 9, 21, 10, 0, 30, tzinfo=UTC)]

    def avanzar():
        reloj[0] = reloj[0] + timedelta(seconds=1)
        return reloj[0]

    sim = Simulador(settings, GeneradorVentas(1), Perturbaciones(), semilla=2, ahora_fn=avanzar)
    mensajes = [m for _, lote in escenario_demo(sim, None) for m in lote]
    tardios = [m for m in mensajes if m.etiqueta == "TARDIO"]
    assert len(tardios) == 1
    assert tardios[0].event_time < datetime(2026, 9, 21, 10, 1, tzinfo=UTC)
