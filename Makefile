export UID := $(shell id -u)
export GID := $(shell id -g)

.PHONY: install test lint check up build down clean logs demo carga smoke reporte topicos datos local

install:            ## dependencias locales (uv)
	uv sync --frozen

test:               ## pruebas unitarias y de ventanas (DirectRunner, sin Docker)
	uv run pytest -q

lint:
	uv run ruff check .
	docker compose config --quiet

check: lint test

build:              ## construye las imágenes (app + flink con SDK harness)
	docker compose build

up:                 ## Kafka + Flink + Job Server + pipeline + materializador
	docker compose up -d --build
	docker compose logs -f pipeline

demo:               ## guion de demostración (normales, duplicados, desorden, tardíos, inválidos)
	docker compose --profile demo up --no-deps productor

carga:              ## carga continua con perturbaciones aleatorias (TASA, DURACION)
	docker compose --profile carga up --no-deps productor-carga

reporte:            ## tablas materializadas en SQLite (último pane por sucursal y ventana)
	docker compose run --rm --no-deps materializador -m ventas_streaming.materializador --reporte

smoke:              ## prueba end-to-end acotada y automática
	docker compose --profile smoke up --build --abort-on-container-exit --exit-code-from smoke smoke

topicos:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

logs:
	docker compose logs -f --tail=100 pipeline materializador

datos:              ## regenera data/eventos_ejemplo.jsonl con el productor (sin Kafka)
	uv run python -m ventas_streaming.productor --modo demo --salida-jsonl data/eventos_ejemplo.jsonl

local:              ## pipeline en DirectRunner sobre el archivo de ejemplo (sin Kafka ni Flink)
	uv run python -m ventas_streaming.pipeline --archivo data/eventos_ejemplo.jsonl --salida tmp/local

down:
	docker compose --profile demo --profile carga --profile smoke --profile ui down

clean:              ## detiene y borra volúmenes (log de Kafka) y la base SQLite
	docker compose --profile demo --profile carga --profile smoke --profile ui down --volumes
	rm -f tmp/ventas.db
