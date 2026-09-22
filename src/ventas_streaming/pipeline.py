"""Pipeline Apache Beam Kafka -> Kafka ejecutado sobre Flink (PortableRunner).

Modo streaming (Docker Compose)::

    python -m ventas_streaming.pipeline

Modo acotado, usado por el smoke test (lee N registros o T segundos y termina)::

    python -m ventas_streaming.pipeline --max-num-records 200 --max-read-time 30

Modo local sin Kafka ni Flink (DirectRunner, lote, sobre un JSONL del productor)::

    python -m ventas_streaming.pipeline --archivo data/eventos_ejemplo.jsonl --salida tmp/agregados
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka, default_io_expansion_service
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.typehints import KV

from ventas_streaming.config import Settings
from ventas_streaming.contratos import codificar_agregado, codificar_rechazo
from ventas_streaming.transformaciones import agregado_a_json, construir_nucleo

log = logging.getLogger("pipeline")


def servicio_expansion_kafka():
    """KafkaIO es Java: sus etapas corren como proceso dentro de cada TaskManager."""
    return default_io_expansion_service(
        append_args=[
            "--defaultEnvironmentType=PROCESS",
            '--defaultEnvironmentConfig={"command":"/opt/apache/beam/boot"}',
        ]
    )


def opciones_flink(settings: Settings, *, job_name: str, acotado: bool = False) -> PipelineOptions:
    opciones = [
        "--runner=PortableRunner",
        f"--job_endpoint={settings.job_endpoint}",
        "--environment_type=PROCESS",
        '--environment_config={"command":"/opt/ventas/python-sdk/boot"}',
        "--streaming",
        f"--parallelism={settings.parallelism}",
        f"--job_name={job_name}",
        "--experiments=use_sdf_read",
        "--max_bundle_time_millis=1000",
        "--max_bundle_size=1000",
    ]
    if not acotado:
        # Opciones del runner Flink (las interpreta el Job Server). Con el
        # checkpointing declarado aquí, Beam mantiene vivas las fuentes (Impulse)
        # y cierra los bundles cada max_bundle_time_millis; sin él la fuente
        # termina y la salida solo se vacía cada 1000 elementos. En el modo
        # acotado (smoke test) se omite a propósito para que el job termine solo
        # cuando la lectura alcanza max_num_records / max_read_time.
        opciones += [
            f"--checkpointing_interval={settings.checkpoint_interval_ms}",
            "--checkpointing_mode=EXACTLY_ONCE",
        ]
    return PipelineOptions(opciones)


def construir_pipeline_kafka(
    pipeline: beam.Pipeline,
    settings: Settings,
    *,
    group_id: str,
    max_num_records: int | None = None,
    max_read_time: int | None = None,
):
    expansion = servicio_expansion_kafka()
    extra: dict[str, int] = {}
    if max_num_records is not None:
        extra["max_num_records"] = max_num_records
    if max_read_time is not None:
        extra["max_read_time"] = max_read_time

    registros = pipeline | "Leer ventas desde Kafka" >> ReadFromKafka(
        consumer_config={
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "true",
        },
        topics=[settings.topico_ventas],
        # El timestamp Kafka lo fija el productor = event_time; el watermark de
        # KafkaIO se deriva de él (máximo por partición, avanza en particiones ociosas).
        timestamp_policy=ReadFromKafka.create_time_policy,
        expansion_service=expansion,
        **extra,
    )
    agregados, invalidos, muy_tardios = construir_nucleo(registros, settings)

    productor_kafka = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "enable.idempotence": "true",
        "acks": "all",
    }
    (
        agregados
        | "Codificar agregados" >> beam.Map(codificar_agregado).with_output_types(KV[bytes, bytes])
        | "Publicar agregados"
        >> WriteToKafka(
            producer_config=productor_kafka,
            topic=settings.topico_agregados,
            expansion_service=expansion,
        )
    )
    (
        (invalidos, muy_tardios)
        | "Unir rechazos" >> beam.Flatten()
        | "Codificar rechazos" >> beam.Map(codificar_rechazo).with_output_types(KV[bytes, bytes])
        | "Publicar DLQ"
        >> WriteToKafka(
            producer_config=productor_kafka, topic=settings.topico_dlq, expansion_service=expansion
        )
    )
    return agregados, invalidos, muy_tardios


def ejecutar_flink(*, group_id: str, max_num_records: int | None, max_read_time: int | None):
    settings = Settings.from_env()
    job_name = os.getenv("BEAM_JOB_NAME", "ventas-por-ventana")
    log.info(
        "enviando job %s a %s: ventana=%ss lateness=%ss early=%ss paralelismo=%d grupo=%s",
        job_name,
        settings.job_endpoint,
        settings.window_seconds,
        settings.allowed_lateness_seconds,
        settings.early_firing_seconds,
        settings.parallelism,
        group_id,
    )
    acotado = max_num_records is not None or max_read_time is not None
    pipeline = beam.Pipeline(options=opciones_flink(settings, job_name=job_name, acotado=acotado))
    construir_pipeline_kafka(
        pipeline,
        settings,
        group_id=group_id,
        max_num_records=max_num_records,
        max_read_time=max_read_time,
    )
    resultado = pipeline.run()
    resultado.wait_until_finish()
    return resultado


def leer_jsonl_como_registros(linea: str) -> tuple[bytes, bytes]:
    registro = json.loads(linea)
    return (registro["key"].encode(), registro["value"].encode())


def instante_de_referencia(archivo: Path) -> datetime:
    """'Ahora' para el modo local: el último timestamp del archivo más unos segundos.

    Así la validación de plausibilidad del reloj no depende del día en que se
    ejecute el ejemplo y el resultado es reproducible.
    """
    maximo = 0
    with archivo.open(encoding="utf-8") as lineas:
        for linea in lineas:
            if linea.strip():
                maximo = max(maximo, int(json.loads(linea).get("timestamp_ms", 0)))
    return datetime.fromtimestamp(maximo / 1000, tz=UTC) + timedelta(seconds=5)


def ejecutar_local(archivo: Path, salida: Path) -> None:
    """DirectRunner en lote: mismo núcleo, sin Kafka ni Flink."""
    settings = Settings.from_env()
    salida.mkdir(parents=True, exist_ok=True)
    referencia = instante_de_referencia(archivo)
    log.info("modo local: instante de referencia %s", referencia.isoformat())
    with beam.Pipeline(options=PipelineOptions(["--runner=DirectRunner"])) as pipeline:
        registros = (
            pipeline
            | "Leer JSONL" >> beam.io.ReadFromText(str(archivo))
            | "A registros Kafka" >> beam.Map(leer_jsonl_como_registros)
        )
        agregados, invalidos, muy_tardios = construir_nucleo(
            registros, settings, triggers_streaming=False, ahora_fn=lambda: referencia
        )
        (
            agregados
            | "JSON agregados" >> beam.Map(agregado_a_json)
            | "Escribir agregados"
            >> beam.io.WriteToText(
                str(salida / "agregados"), file_name_suffix=".jsonl", shard_name_template=""
            )
        )
        (
            (invalidos, muy_tardios)
            | "Unir rechazos" >> beam.Flatten()
            | "JSON rechazos"
            >> beam.Map(lambda r: json.dumps(r, sort_keys=True, ensure_ascii=False))
            | "Escribir rechazos"
            >> beam.io.WriteToText(
                str(salida / "rechazos"), file_name_suffix=".jsonl", shard_name_template=""
            )
        )
    log.info("salida escrita en %s", salida)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--group-id", default="beam-ventas-por-ventana-v1")
    parser.add_argument("--max-num-records", type=int)
    parser.add_argument("--max-read-time", type=int, help="segundos")
    parser.add_argument(
        "--archivo", type=Path, help="modo local: JSONL producido con --salida-jsonl"
    )
    parser.add_argument(
        "--salida", type=Path, default=Path("tmp/local"), help="modo local: carpeta de salida"
    )
    args = parser.parse_args(argv)
    if args.archivo is not None:
        ejecutar_local(args.archivo, args.salida)
        return
    resultado = ejecutar_flink(
        group_id=args.group_id,
        max_num_records=args.max_num_records,
        max_read_time=args.max_read_time,
    )
    print(json.dumps({"state": str(resultado.state)}))


if __name__ == "__main__":
    main()
