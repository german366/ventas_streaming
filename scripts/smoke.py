"""Smoke test acotado: fuente -> Kafka -> Beam/Flink -> Kafka -> SQLite, con verificación.

Crea tópicos efímeros, publica un lote conocido (ventas normales, duplicados,
una venta demasiado tardía y mensajes inválidos), ejecuta el pipeline en modo
acotado, materializa la salida y comprueba los invariantes:

* la suma de tickets de la salida es exactamente el número de ventas únicas válidas;
* los duplicados se descartan y quedan contabilizados;
* la DLQ contiene los inválidos y el demasiado tardío, cada uno con su motivo.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event

from confluent_kafka.admin import AdminClient, NewTopic

log = logging.getLogger("smoke")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    sufijo = uuid.uuid4().hex[:8]
    os.environ["KAFKA_TOPICO_VENTAS"] = f"smoke.ventas.{sufijo}"
    os.environ["KAFKA_TOPICO_AGREGADOS"] = f"smoke.agregados.{sufijo}"
    os.environ["KAFKA_TOPICO_DLQ"] = f"smoke.dlq.{sufijo}"
    os.environ["SQLITE_PATH"] = f"tmp/smoke-{sufijo}.db"
    os.environ["BEAM_JOB_NAME"] = f"ventas-smoke-{sufijo}"

    from ventas_streaming.config import Settings
    from ventas_streaming.materializador import AlmacenVentas, consumir, imprimir_reporte
    from ventas_streaming.productor import (
        Emisor,
        GeneradorVentas,
        Mensaje,
        Perturbaciones,
        Simulador,
        mensaje_de_evento,
    )

    settings = Settings.from_env()
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    futuros = admin.create_topics(
        [
            NewTopic(settings.topico_ventas, 4, 1),
            NewTopic(settings.topico_agregados, 4, 1),
            NewTopic(settings.topico_dlq, 2, 1),
        ]
    )
    for futuro in futuros.values():
        futuro.result(timeout=30)
    log.info("tópicos efímeros creados con sufijo %s", sufijo)

    # 1. Lote conocido
    generador = GeneradorVentas(semilla=42)
    simulador = Simulador(settings, generador, Perturbaciones(), semilla=43)
    ahora = datetime.now(UTC)
    normales = [
        mensaje_de_evento(generador.siguiente(ahora - timedelta(seconds=30 - i * 0.5)), "OK")
        for i in range(50)
    ]
    duplicados = [
        Mensaje("DUPLICADO", m.key, m.value, m.event_time, m.headers, m.event_id)
        for m in normales[:3]
    ]
    muy_tardio = mensaje_de_evento(
        generador.siguiente(ahora - timedelta(seconds=settings.horizonte_tardios_seconds + 240)),
        "MUY_TARDIO",
    )
    base = generador.siguiente(ahora)
    invalidos = [simulador.invalido(base) for _ in range(3)]
    lote = normales + duplicados + [muy_tardio] + invalidos

    emisor = Emisor(settings)
    for mensaje in lote:
        emisor.emitir(mensaje)
    produccion = emisor.cerrar()
    if produccion["confirmados"] != len(lote):
        raise RuntimeError(f"se esperaban {len(lote)} confirmaciones: {produccion}")
    log.info("lote publicado: %s", json.dumps(produccion, ensure_ascii=False))

    # 2. Pipeline acotado
    subprocess.run(
        [
            sys.executable,
            "-m",
            "ventas_streaming.pipeline",
            "--group-id",
            f"smoke-beam-{sufijo}",
            "--max-num-records",
            str(len(lote)),
            "--max-read-time",
            "45",
        ],
        check=True,
        timeout=420,
        env=os.environ,
    )

    # 3. Materialización y verificación
    almacen = AlmacenVentas(settings.sqlite_path)
    limite = time.monotonic() + 90
    while time.monotonic() < limite:
        consumir(
            almacen,
            settings,
            group_id=f"smoke-sink-{sufijo}",
            desde_inicio=True,
            timeout_inactividad=5,
            parar=Event(),
        )
        if almacen.filas() and len(almacen.rechazos()) >= 4:
            break
    imprimir_reporte(almacen)
    filas = almacen.filas()
    tickets = sum(f["ticket_count"] for f in filas)
    dup = sum(f["duplicates_discarded"] for f in filas)
    motivos = almacen.resumen()["rechazos_por_motivo"]
    resultado = {
        "sufijo": sufijo,
        "publicados": produccion,
        "filas_salida": len(filas),
        "tickets_unicos_en_salida": tickets,
        "duplicados_descartados": dup,
        "rechazos_por_motivo": motivos,
    }
    print(json.dumps(resultado, ensure_ascii=False, indent=2))
    Path("tmp").mkdir(exist_ok=True)
    Path(f"tmp/smoke-{sufijo}.json").write_text(json.dumps(resultado, ensure_ascii=False, indent=2))

    errores = []
    if tickets != 50:
        errores.append(f"tickets únicos esperados 50, obtenidos {tickets}")
    if dup != 3:
        errores.append(f"duplicados descartados esperados 3, obtenidos {dup}")
    if motivos.get("demasiado_tardio") != 1:
        errores.append(f"se esperaba 1 rechazo demasiado_tardio: {motivos}")
    if sum(v for k, v in motivos.items() if k != "demasiado_tardio") != 3:
        errores.append(f"se esperaban 3 rechazos por contrato: {motivos}")
    if errores:
        raise SystemExit("SMOKE FALLIDO: " + "; ".join(errores))
    log.info("SMOKE OK")


if __name__ == "__main__":
    main()
