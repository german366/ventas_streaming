# Evidencia de ejecución end-to-end

Salidas reales, sin editar (solo recortadas), de una ejecución completa del
stack con `make up` + `make demo` el 21 de septiembre de 2026 (hora UTC en los
logs). Todo se regenera con los comandos indicados en cada archivo.

| Archivo | Qué muestra |
|---|---|
| `01_topicos.txt` | Creación de los tres tópicos con particiones y configuración (`cleanup.policy=compact` en la salida, retención en entrada y DLQ). |
| `02_productor_demo.txt` | Guion de demostración completo: cada mensaje etiquetado (`OK`, `DUPLICADO`, `DESORDEN`, `TARDIO`, `MUY_TARDIO`, `INVALIDO:<tipo>`) con clave, `event_time` y partición/offset donde quedó (misma clave → misma partición). En la fase 4 el productor espera el pane `ON_TIME` de la ventana objetivo y recién entonces envía la venta tardía. |
| `03_materializador.txt` | Cada pane recibido desde `ventas.por_ventana.v1` (`UPSERT` con `pane_index`, `pane_timing`, tickets, total, duplicados descartados) y cada rechazo de la DLQ con su motivo. Buscar `S1|2026-09-21T23:58` para ver la corrección tardía: `ON_TIME` con 35 tickets → `LATE` con 36. |
| `04_reporte_sqlite.txt` | Estado final en SQLite: 18 filas (6 sucursales × 3 ventanas) con el último pane de cada una, tabla `rechazos` con los 7 motivos y resumen. |
| `05_kafka_grupos_offsets_particiones.txt` | Grupos de consumidores con lag 0, fin de log por partición y distribución real de mensajes por partición y clave (evidencia del skew: 47 % en una partición frente a 8 % en otra). |
| `06_flink_estado.txt`, `06_flink_ui_job.png`, `06_flink_ui_checkpoints.png` | Job `ventas-por-ventana` en RUNNING con 4 vértices y paralelismo 2, registros por vértice, checkpoints completados cada 30 s, contenedores y memoria. |
| `07_idempotencia_replay.txt` | Un segundo consumidor con grupo nuevo relee ambos tópicos desde el inicio hacia otra base y obtiene exactamente las mismas 18 filas y 7 rechazos, aunque por la compactación ve 37 mensajes en lugar de los 90 panes originales. |
| `08_smoke.txt` | `make smoke`: tópicos efímeros, lote conocido (50 ventas + 3 duplicados + 1 demasiado tardía + 3 inválidas), pipeline acotado, materialización y verificación automática de invariantes. |
| `09_pytest.txt` | 33 pruebas en verde: contrato, combinador, ventanas/panes con `TestStream`, sink idempotente y productor. |
| `10_tolerancia_fallos.txt` | Reinicio del TaskManager con el job en ejecución: Flink restaura desde el último checkpoint completado, el job vuelve a RUNNING y las ventanas siguientes se materializan; los panes reemitidos no alteran el estado. |

Cómo leer los panes en `03_materializador.txt`:

- `EARLY`: valor provisional emitido cada 10 s de processing time mientras la ventana está abierta;
- `ON_TIME`: primer valor completo, cuando el watermark cruza el fin de la ventana;
- `LATE`: corrección por un evento tardío dentro de la lateness, o el pane de cierre que el runner emite al expirar la ventana (mismo valor, `pane_index` mayor);
- `IGNORADO`: pane cuya versión `(ticket_count, pane_index)` no supera la ya aplicada (por ejemplo, un pane vacío fuera de orden), descartado por la guarda de idempotencia.
