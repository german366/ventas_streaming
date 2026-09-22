# Guion de la demostración (video breve o en vivo, 6 a 8 minutos)

Objetivo: mostrar el recorrido completo fuente → Kafka → Beam/Flink → Kafka → SQLite
y, sobre todo, cómo responde el sistema ante **duplicados**, **desorden**,
**eventos tardíos** y **demasiado tardíos**, que es lo que pide la consigna.

Preparación (antes de grabar): `make clean && make build` para partir de cero con
las imágenes ya construidas. Tener cuatro terminales abiertas en la carpeta del
proyecto y el navegador en <http://localhost:8081> (Flink).

| Min | Terminal / pantalla | Qué mostrar | Qué decir |
|---|---|---|---|
| 0:00 | Diagrama `docs/diagrama/arquitectura.png` | Los cuatro bloques y los tres tópicos. | Caso de uso (ventas por sucursal y ventana, atribuidas al momento del pago), por qué tiempo de evento, ventana 60 s / lateness 30 s escaladas de 60 min / 30 min. |
| 0:45 | T1: `make up` | Kafka sano, tópicos creados (`kafka-init` describe particiones y `cleanup.policy=compact`), pipeline enviando el job. | Paralelismo 2 sobre 4 particiones; KafkaIO como transformación cross-language ejecutada como proceso en el TaskManager. |
| 1:30 | Navegador: Flink UI | Job `ventas-por-ventana` en RUNNING, los operadores y los checkpoints cada 30 s. | Runner elegido y modo de ejecución (PortableRunner + Flink, streaming). |
| 2:00 | T2: `docker compose logs -f materializador` | Vacío por ahora; explicar qué va a aparecer. | Sink idempotente: upsert por `aggregate_id` con versión `(ticket_count, pane_index)` monótona. |
| 2:15 | T3: `make demo` | Fase 1: ventas `[OK]` con clave, `event_time`, partición y offset. | Misma clave → misma partición (orden por sucursal). |
| 2:45 | T2 | Primeros `UPSERT ... pane=0 EARLY` y luego `ON_TIME` cuando el watermark cruza el fin de la ventana. | Panes EARLY (processing time, cada 10 s) para el tablero; ON_TIME cuando el watermark cruza el fin. |
| 3:15 | T3 → T2 | Fase 2: tres `[DUPLICADO]` con el mismo `event_id`; en T2 el pane siguiente muestra `dup_descartados=3` y el total no cambia. | Dedup dentro del `CombineFn`: horizonte = ventana + lateness. |
| 3:45 | T3 → T2 | Fase 3: cinco `[DESORDEN]` emitidos en orden inverso. En T2 la ventana sigue creciendo sin panes extra. | Desorden dentro de la ventana se absorbe; el timestamp lo pone `event_time`, no la llegada. |
| 4:15 | T3 → T2 | Fase 4: `[TARDIO]` con `event_time` de la ventana anterior. En T2 aparece `UPSERT ... LATE` para esa ventana con un ticket más. | Lateness permitida 30 s: la corrección reemplaza la fila; el consumidor ve `pane_timing=LATE`. |
| 4:45 | T3 → T2 | Fase 5: `[MUY_TARDIO]`. En T2: `DLQ motivo=demasiado_tardio`. | Fuera del horizonte no se descarta en silencio: va a la DLQ para conciliación. |
| 5:15 | T3 → T2 | Fase 6: seis `[INVALIDO:<tipo>]`; en T2 seis `DLQ motivo=...` distintos. | Validación del contrato con salidas laterales; versión 99 rechazada, v1 aceptada. |
| 5:45 | T4: `make reporte` | Tabla `ventas_por_ventana` (una fila por sucursal y ventana, `pane_timing`, `panes_recibidos`, `duplicates_discarded`) y tabla `rechazos`. | Resultado consumible; estado final idéntico aunque se relea el tópico. |
| 6:30 | T4: `docker compose run --rm -e SQLITE_PATH=/app/tmp/ventas_replay.db materializador -m ventas_streaming.materializador --group-id verificacion --timeout 15 --reporte-al-final` | Un consumidor nuevo relee ambos tópicos desde el inicio hacia otra base y obtiene las mismas filas (comparar con `make reporte`). | Idempotencia demostrada, no declarada: clave estable + versión monótona. |
| 7:00 | Cierre | Semántica alcanzada por tramo y límites conocidos. | Al menos una vez + upsert = efectivamente una sola vez en el sink; sin exactly-once end-to-end. |

Alternativa corta si se corta el tiempo: `make smoke` (2 a 3 minutos) imprime al
final el JSON con tickets únicos, duplicados descartados y rechazos por motivo, y
termina con `SMOKE OK`.
