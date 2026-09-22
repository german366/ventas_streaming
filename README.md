# Ventas por sucursal y ventana — pipeline de streaming con Kafka, Beam y Flink

Proyecto integrador de **Streaming de datos y sus aplicaciones** (Maestría en
Inteligencia Artificial, FPUNA). Implementa un pipeline end-to-end que recibe
ventas confirmadas de terminales de punto de venta (POS) por Apache Kafka, las
procesa con Apache Beam sobre Flink usando **tiempo de evento**, **ventanas
fijas**, **deduplicación** y **panes tardíos**, y materializa el resultado en una
**salida idempotente** (tópico compactado + SQLite con upsert).

```text
Productor POS ──> ventas.confirmadas.v1 ──> Beam / Flink ──> ventas.por_ventana.v1 ──> SQLite (upsert)
 (sintético)        4 particiones             ventanas 60 s      compactado           ventas_por_ventana
                    clave store_id            dedup event_id     clave aggregate_id
                                              └─ inválidos / demasiado tardíos ──> ventas.dlq.v1 ──> rechazos
```

Documento técnico: [`docs/documento_tecnico.md`](docs/documento_tecnico.md) ·
Diagrama: [`docs/diagrama/arquitectura.png`](docs/diagrama/arquitectura.png) ·
Evidencia de ejecución: [`docs/evidencia/`](docs/evidencia/) ·
Guion de la demostración: [`docs/guion_demo.md`](docs/guion_demo.md) ·
Integrantes y contribuciones: [`docs/integrantes.md`](docs/integrantes.md).

## 1. Caso de uso en una frase

**¿Cuánto vendió cada sucursal en cada ventana de tiempo (monto total y cantidad
de tickets), atribuyendo cada venta al momento en que el cliente pagó y no al
momento en que el evento llegó?** El resultado alimenta un tablero de gerencia
(valores provisionales rápidos), un reporte comparable entre sucursales y horas
(valores corregidos durante un horizonte acotado) y la conciliación contable
(recibe lo que queda fuera del horizonte vía DLQ).

En la demostración la ventana es de **60 s** y la lateness permitida de **30 s**
para que todo se observe en minutos; en producción los mismos parámetros valen
3600 s y 1800 s (hora comercial y media hora de corrección) y se cambian por
variables de entorno sin tocar código.

## 2. Prerrequisitos

| Requisito | Versión probada |
|---|---|
| Docker Engine + Docker Compose v2 | 29.x / v5.5 |
| Memoria libre para Docker | 8 GB |
| Conexión a Internet | solo para el primer `build` (imágenes y JAR de KafkaIO) |
| Opcional, para pruebas locales sin Docker: Python 3.12 + [uv](https://docs.astral.sh/uv/) | 3.12.3 / 0.12 |

Las imágenes de Beam se ejecutan como `linux/amd64`; en Apple Silicon Docker las
emula y el primer arranque es más lento.

## 3. Ejecución en cuatro comandos

```bash
make up        # 1. Kafka + Flink + Beam Job Server + pipeline + materializador (deja logs del pipeline)
make demo      # 2. productor con el guion de demostración (~2,5 min), en otra terminal
make reporte   # 3. tablas materializadas: último pane por sucursal y ventana + DLQ
make down      # 4. detener (make clean borra también el log de Kafka y la base SQLite)
```

`make up` equivale a `docker compose up -d --build && docker compose logs -f pipeline`.
El job tarda entre 1 y 2 minutos en pasar a RUNNING (envío del JAR de KafkaIO al
Job Server y arranque de los SDK harness) y aparece en la UI de Flink en
<http://localhost:8081> como `ventas-por-ventana`. Los servicios que escriben
en `./tmp` corren con el uid/gid del host, que el Makefile exporta (`UID`, `GID`);
si se usa `docker compose` directamente, exportarlos antes o dejar los valores
por defecto (1000:1000).
Interfaz opcional para inspeccionar tópicos: `docker compose --profile ui up -d` → <http://localhost:8080>.

Qué se ve en cada componente:

- `docker compose logs -f productor`: cada mensaje con su etiqueta (`OK`,
  `DUPLICADO`, `DESORDEN`, `TARDIO`, `MUY_TARDIO`, `INVALIDO:<tipo>`), la clave,
  el `event_time` y la partición/offset donde quedó.
- `docker compose logs -f materializador`: cada pane recibido (`UPSERT` o
  `IGNORADO`), con `pane_index`, `pane_timing` (`EARLY`/`ON_TIME`/`LATE`),
  tickets, total y duplicados descartados; y cada rechazo de la DLQ con su motivo.
- `make reporte`: estado final de `ventas_por_ventana` y `rechazos` en SQLite.

### Tolerancia a fallos (opcional)

Con el job en ejecución, `docker compose restart taskmanager` mata los SDK
harness y las subtareas; Flink restaura el último checkpoint completado y el job
vuelve a RUNNING solo (ver `docs/evidencia/10_tolerancia_fallos.txt`). Los panes
reemitidos no alteran el estado del sink.

### Carga continua en lugar del guion

```bash
TASA=6 DURACION=300 make carga     # 6 ventas/s durante 5 min con perturbaciones aleatorias (semilla fija)
```

Parámetros del productor (`python -m ventas_streaming.productor --help`):
`--dup-rate`, `--desorden-rate`, `--tardios-rate`, `--muy-tardios-rate`,
`--invalidos-rate`, `--semilla`, `--solo-validos`.

## 4. Pruebas

```bash
make install   # uv sync --frozen
make test      # 33 pruebas: contrato, combinador, ventanas/panes con TestStream, sink, productor
make lint      # ruff + validación del compose
make smoke     # prueba end-to-end acotada y automática sobre Docker (crea tópicos efímeros)
```

| Prueba | Archivo | Qué demuestra |
|---|---|---|
| Contrato y evolución de esquema | `tests/test_contratos.py` | v1 se actualiza a v2; cada error tiene un motivo estable |
| Lógica de agregación | `tests/test_transformaciones.py` | `CombineFn` incremental, dedup por `event_id`, fusión de acumuladores, salidas laterales |
| Tiempo de evento y ventanas | `tests/test_ventanas.py` | con `TestStream`: pane `ON_TIME`, pane `LATE` por un evento tardío dentro de la lateness, descarte fuera de ella, duplicado tardío que no altera el total, desorden dentro de la ventana |
| Sink idempotente | `tests/test_materializador.py` | upsert con versión `(ticket_count, pane_index)` monótona; releer todo el tópico da el mismo estado |
| Productor | `tests/test_productor.py` | determinismo con semilla; el guion demo cubre todos los casos adversos |
| Smoke end-to-end | `scripts/smoke.py` | fuente → Kafka → Beam/Flink → Kafka → SQLite con verificación de invariantes |

### Sin Docker: pipeline local sobre un archivo

El mismo núcleo Beam corre en DirectRunner (lote) sobre el archivo de ejemplo
generado por el productor, útil para revisar la lógica sin levantar el stack:

```bash
make local     # lee data/eventos_ejemplo.jsonl y escribe tmp/local/{agregados,rechazos}.jsonl
make datos     # regenera el archivo de ejemplo con el guion demo (sin Kafka)
```

## 5. Estructura del repositorio

```text
ventas-streaming/
├── docker-compose.yml            Kafka (KRaft), Flink (JM + TM), Beam Job Server, pipeline, materializador,
│                                 productor (perfiles demo / carga), smoke, kafka-ui (perfil ui)
├── Dockerfile                    imagen de aplicación (Python 3.12 + Java 17 para el servicio de expansión de KafkaIO)
├── docker/flink/                 imagen Flink 1.19 con los SDK harness de Beam (Java y Python) como procesos
├── Makefile                      comandos de inicio, prueba, demostración y detención
├── src/ventas_streaming/
│   ├── config.py                 parámetros (tópicos, ventana, lateness, paralelismo) desde variables de entorno
│   ├── contratos.py              contrato del evento (v1/v2), validación, claves de salida
│   ├── productor.py              productor sintético reproducible con perturbaciones y guion demo
│   ├── transformaciones.py       DoFns, CombineFn con dedup, ventanas y triggers (independiente de Kafka)
│   ├── pipeline.py               KafkaIO → núcleo → KafkaIO sobre Flink; modo acotado; modo local
│   └── materializador.py         consumidor Kafka → SQLite con upsert idempotente; reporte
├── scripts/smoke.py              prueba end-to-end acotada
├── tests/                        pruebas con pytest, TestPipeline y TestStream
├── data/eventos_ejemplo.jsonl    156 mensajes del guion demo (válidos, duplicados, tardíos, inválidos)
└── docs/                         documento técnico, diagrama, evidencia, guion de la demo, integrantes
```

## 6. Decisiones principales (resumen; detalle en el documento técnico)

- **Contrato**: `event_id` estable generado en el POS (`store-pos-secuencia`),
  `key = store_id`, `event_time` del POS separado del tiempo de procesamiento,
  `payload` con monto/ítems/medio de pago y `schema_version` con evolución aditiva
  (v1 → v2 agrega `channel`; el pipeline acepta ambas y rechaza versiones futuras).
- **Kafka**: `ventas.confirmadas.v1` con 4 particiones y clave `store_id`
  (orden por sucursal, paralelismo de lectura hasta 4, skew analizado);
  `ventas.por_ventana.v1` compactado por `aggregate_id`; `ventas.dlq.v1` con motivo.
  Productor con `enable.idempotence=true`, `acks=all` y `timestamp = event_time`.
- **Beam sobre Flink** (PortableRunner, streaming, paralelismo 2, checkpoints
  cada 30 s): `ReadFromKafka` con política de timestamp *create time* (watermark
  por partición), validación con salidas laterales, `CombinePerKey` con un
  `CombineFn` incremental que deduplica por `event_id`, ventana fija de 60 s,
  `AfterWatermark(early=10 s de processing time, late=AfterCount(1))`,
  `allowed_lateness=30 s`, acumulación `ACCUMULATING`, `WriteToKafka` con clave
  estable `aggregate_id = store_id|window_start`.
- **Semántica de entrega**: *al menos una vez* entre Kafka y el sink, convertida
  en *efectivamente una sola vez* por el upsert idempotente (clave estable +
  versión `(ticket_count, pane_index)` monótona). No se afirma exactly-once end-to-end; el documento
  técnico delimita cada tramo.
- **Límites declarados**: un solo broker y un solo TaskManager (sin alta
  disponibilidad); el estado de la ventana vive en memoria del TaskManager
  (checkpoints locales); la clasificación de "demasiado tardío" antes de la
  ventana usa un watermark heurístico local y la decisión final es del watermark
  de Beam; `sales.confirmed` no modela anulaciones ni devoluciones.
