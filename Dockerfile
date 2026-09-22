# syntax=docker/dockerfile:1
# Imagen de aplicación: productor, envío del pipeline Beam, materializador y smoke test.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    HOME=/app

# Java: necesario para el servicio de expansión de KafkaIO (transformación cross-language).
RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY scripts ./scripts
COPY data ./data
RUN uv sync --frozen --no-dev

# Descarga y cachea el JAR del servicio de expansión de KafkaIO durante el build,
# para que el envío del job no dependa de Maven en tiempo de ejecución.
RUN /app/.venv/bin/python -c "from apache_beam.io.kafka import default_io_expansion_service; s = default_io_expansion_service(); s.__enter__(); s.__exit__(None, None, None)"

# Los contenedores corren con el uid del host (ver docker-compose.yml) para que
# ./tmp sea escribible desde ambos lados; por eso la caché y tmp son de acceso libre.
RUN mkdir -p /app/tmp && chmod -R a+rwX /app/.apache_beam /app/tmp
USER 1000

ENTRYPOINT ["/app/.venv/bin/python"]
CMD ["-m", "ventas_streaming.pipeline"]
