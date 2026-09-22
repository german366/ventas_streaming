"""Configuración compartida por productor, pipeline, materializador y pruebas."""

from __future__ import annotations

import os
from dataclasses import dataclass

TOPICO_VENTAS = "ventas.confirmadas.v1"
TOPICO_AGREGADOS = "ventas.por_ventana.v1"
TOPICO_DLQ = "ventas.dlq.v1"


@dataclass(frozen=True)
class Settings:
    """Parámetros de ejecución con valores por defecto pensados para Docker Compose.

    Los tiempos están en segundos. En la demostración la ventana es de 60 s y la
    lateness permitida de 30 s; en producción los mismos parámetros valdrían
    3600 s y 1800 s (hora comercial y media hora de corrección) sin cambiar código.
    """

    kafka_bootstrap_servers: str = "kafka:9092"
    topico_ventas: str = TOPICO_VENTAS
    topico_agregados: str = TOPICO_AGREGADOS
    topico_dlq: str = TOPICO_DLQ
    window_seconds: int = 60
    allowed_lateness_seconds: int = 30
    early_firing_seconds: int = 10
    max_future_skew_seconds: int = 120
    max_past_skew_seconds: int = 24 * 3600
    parallelism: int = 2
    job_endpoint: str = "beam-job-server:8099"
    checkpoint_interval_ms: int = 30_000
    sqlite_path: str = "tmp/ventas.db"

    @property
    def horizonte_tardios_seconds(self) -> int:
        """Horizonte de corrección: fin de ventana + lateness permitida."""
        return self.window_seconds + self.allowed_lateness_seconds

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            topico_ventas=os.getenv("KAFKA_TOPICO_VENTAS", TOPICO_VENTAS),
            topico_agregados=os.getenv("KAFKA_TOPICO_AGREGADOS", TOPICO_AGREGADOS),
            topico_dlq=os.getenv("KAFKA_TOPICO_DLQ", TOPICO_DLQ),
            window_seconds=int(os.getenv("WINDOW_SECONDS", "60")),
            allowed_lateness_seconds=int(os.getenv("ALLOWED_LATENESS_SECONDS", "30")),
            early_firing_seconds=int(os.getenv("EARLY_FIRING_SECONDS", "10")),
            max_future_skew_seconds=int(os.getenv("MAX_FUTURE_SKEW_SECONDS", "120")),
            max_past_skew_seconds=int(os.getenv("MAX_PAST_SKEW_SECONDS", str(24 * 3600))),
            parallelism=int(os.getenv("BEAM_PARALLELISM", "2")),
            job_endpoint=os.getenv("BEAM_JOB_ENDPOINT", "beam-job-server:8099"),
            checkpoint_interval_ms=int(os.getenv("CHECKPOINT_INTERVAL_MS", "30000")),
            sqlite_path=os.getenv("SQLITE_PATH", "tmp/ventas.db"),
        )
