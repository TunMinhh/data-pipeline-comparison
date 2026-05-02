# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

A wearable health data platform implementing a **medallion architecture** (Bronze → Silver → Gold) using:
- **Kafka** (KRaft, no ZooKeeper) — message broker for 11 topics
- **Spark 3.5.1** — structured streaming (Bronze) and batch jobs (Silver, Gold)
- **Delta Lake 3.2.0** — storage format on HDFS
- **HDFS** — distributed storage (namenode + datanode)
- **Airflow 2.7.1** — orchestration (`orchestration/dags/pipeline_a.py` — daily Silver + Gold)
- **PostgreSQL** — shared metadata DB (hosts both `airflow_db` and `mlflow_db`)
- **MinIO** — S3-compatible object store for MLflow artifacts
- **MLflow 2.10.2** — experiment tracking + model registry
- **FastAPI** (`ml_module/`) — AttentionLSTM training + inference service (`/train`, `/predict`, `/health`)
- **PostgreSQL sink** — second Postgres instance holding Gold tables for dashboard queries (kept separate from Airflow/MLflow metadata DB)
- **Grafana 10.4** — dashboards over the Postgres sink (auto-provisioned datasource)

All infrastructure runs in Docker via `docker-compose.yml`. Python scripts run on the host using `.venv`.

### Ports (host)

| Service | Host port | Notes |
|---------|-----------|-------|
| HDFS NameNode UI | 9870 | |
| Kafka | 9092 | |
| Spark Master UI | 8080 | |
| Airflow UI | 8081 | admin / pwd12345 |
| MLflow UI | 5000 | |
| MinIO S3 API | 9100 | minioadmin / minioadmin123 |
| MinIO Console | 9101 | |
| FastAPI | 8000 | `/train`, `/predict`, `/health` |
| Grafana | 3000 | admin / admin123 |
| PostgreSQL (metadata) | **5433** | airflow_db + mlflow_db (5432 collides with a locally-installed Postgres) |
| PostgreSQL (sink) | **5434** | wellness DB — Gold tables for Grafana |

All ports bind to `127.0.0.1` only — use SSH tunneling for remote access to a VPS deployment.

## Common commands

```bash
# Start all infrastructure
docker compose up -d

# One-time setup (HDFS dirs + Kafka topics)
make init

# Run the full pipeline in order:
make bronze          # Terminal 1 — keep running (streaming)
make producer        # Terminal 2 — replay hourly CSV into Kafka
make producer-daily  # Terminal 2 — replay daily CSV into Kafka
make silver          # After Bronze has flushed at least one batch (30s)
make gold            # After Silver completes
make export-gold     # Phase 4 — Gold Delta -> PostgreSQL sink for Grafana

# Check data at each layer
make check-hdfs      # Bronze
make check-silver    # Silver
make check-gold      # Gold

# Wipe everything and start fresh
make reset
make init
```

## Architecture

### Data flow

```
CSV files → Kafka topics → Bronze Delta (HDFS) → Silver Delta (HDFS) → Gold Delta (HDFS)
```

- **Ingestion** (`ingestion/`): Three producers replay CSV data into Kafka. `producer.py` handles hourly vitals/activity/context/profile. `producer_daily.py` handles daily sleep/HRV/breathing/vitals. `producer_realtime.py` generates synthetic intraday signals.
- **Bronze** (`processing/spark_bronze.py`): Spark Structured Streaming, 30-second micro-batch trigger. Reads all 11 Kafka topics, writes raw Delta tables to `hdfs://namenode:9000/data/bronze/wearable/{folder}`.
- **Silver** (`processing/spark_silver.py`): Batch job. Parses JSON payloads using per-topic schemas, casts types, applies range filters, and MERGEs into Silver (dedup on `user_id + event_timestamp`). Intraday topics use append-only (MERGE too expensive at per-second volume).
- **Gold** (`processing/spark_gold.py`): Batch job. Aggregates Silver to 7 daily summary tables: `daily_vitals_summary`, `daily_activity_summary`, `daily_context_summary`, `daily_sleep_summary`, `daily_vitals_daily_summary`, `daily_intraday_summary`, `daily_wellness_profile`. Always overwrites.

### Kafka topics (all use underscores — no dots)

Hourly: `wearable_vitals`, `wearable_activity`, `wearable_context`, `wearable_profile`
Daily: `wearable_sleep`, `wearable_hrv_summary`, `wearable_breathing_summary`, `wearable_vitals_daily`
Intraday: `wearable_heart_rate_intraday`, `wearable_hrv_intraday`, `wearable_breathing_intraday`

### HDFS layout

```
/data/bronze/wearable/{vitals,activity,context,profile,sleep,...}
/data/silver/wearable/{vitals,activity,context,profile,sleep,...}
/data/gold/wearable/{daily_vitals_summary,daily_activity_summary,...}
/checkpoints/bronze/wearable/main
```

### Critical producer detail

Numeric payload fields must be cast to `float` (via `_num()` helper), not left as raw CSV strings. `from_json` with `FloatType` schema returns null for JSON string values like `"72.5"` — it requires JSON numbers like `72.5`. String fields (age, gender, bmi, mindfulness_session) correctly use `_val()`.

## Memory constraints (local dev)

Spark worker container is 3g. Running Bronze and Silver/Gold simultaneously risks OOM — stop Bronze before running Silver or Gold if memory is tight. Driver memory minimum is 512m (Spark 3.5 enforces a floor of 450m).

## Querying data (interactive)

```bash
docker exec -it pipeline_a-spark-master-1 /opt/spark/bin/pyspark \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

```python
gold = "hdfs://namenode:9000/data/gold/wearable"
spark.read.format("delta").load(gold + "/daily_vitals_summary").show(5)
```

## AI module (Phase 3)

Location: `ml_module/`.

- `model/attention_lstm.py` — PyTorch AttentionLSTM autoencoder. Input shape `(batch, 7, 7)` (7-day window of 7 features). Returns `(reconstruction, attention_weights)`.
- `train.py` — standalone training script. Uses PySpark in local mode to read Silver Delta tables (`vitals_daily`, `sleep`, `activity`), joins per `(user_id, event_date)`, builds 7-day sliding windows, trains the autoencoder, z-scores reconstruction error, classifies into green/yellow/red using paper thresholds (1.04 / 1.29 / 1.65), logs everything to MLflow (experiment `wellness-anomaly-detector`, registered model `wellness-detector`), and writes a `ai_insights` Gold Delta table.
- `serve.py` — FastAPI app. `POST /train` queues a training run in a background thread (non-blocking so the Airflow DAG doesn't time out). `POST /predict` loads the latest registered model from MLflow and returns a reconstruction error for a supplied `(seq_len, n_features)` sequence. `GET /health` + `GET /train/{job_id}` for status.
- `Dockerfile` — Python 3.10 + JDK-17 (for PySpark) + CPU-only PyTorch wheel.

Features used by the model (must all exist in Silver):
`resting_hr`, `stress_score`, `spo2`, `minutes_asleep`, `sleep_efficiency`, `steps`, `calories`.

### Training flow

```
Airflow DAG → curl POST http://fastapi:8000/train
           → FastAPI spawns thread → train.main()
                                      → Spark read Silver
                                      → pandas + sklearn StandardScaler
                                      → PyTorch AttentionLSTM (autoencoder)
                                      → MLflow log model + metrics
                                      → Spark write Gold /ai_insights
```

Anomaly thresholds come from the DeltaTrace paper (z-score on reconstruction error):
`green ≥ 1.04`, `yellow ≥ 1.29`, `red ≥ 1.65`. Below 1.04 is `normal`.

## Phase 4 — Data Exposition (Grafana)

Gold Delta → PostgreSQL sink → Grafana. Implemented by `processing/spark_gold_to_postgres.py`:
reads each of the 8 Gold tables (7 daily summaries + `ai_insights`) and overwrites the
matching table in the sink Postgres via JDBC (`org.postgresql:postgresql:42.7.3`).
Truncate + insert, so table schemas survive across runs.

The Airflow DAG's `data_exposition` task now invokes this job (no longer an echo placeholder).

Grafana datasource is auto-provisioned from `grafana/provisioning/datasources/postgres.yml`
using `${PG_SINK_*}` env vars. Dashboards drop into `grafana/provisioning/dashboards/`.

```bash
make export-gold     # run the export manually
make check-pg-sink   # list tables + row counts in the sink
make check-grafana   # liveness check
```

## What's not yet implemented

- **Phase 3 TODOs**: `if_new_data` branch currently always proceeds (it should check if Silver has rows newer than the last MLflow run). `/predict` accepts raw feature arrays — no user_id / date-range lookup yet.
- **Grafana dashboards**: datasource is auto-provisioned but no JSON dashboards are committed yet — build them in the UI then export to `grafana/provisioning/dashboards/`.
