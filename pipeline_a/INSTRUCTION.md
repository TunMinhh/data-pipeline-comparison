# Pipeline A — What Is This and What Does It Do?

A beginner-friendly guide to understanding this wearable-health data pipeline.

---

## The One-Sentence Summary

This pipeline takes raw **Fitbit/wearable CSV files**, streams them through a series of processing stages, trains an AI model to detect health anomalies, and finally displays daily wellness summaries on a **Grafana dashboard**.

---

## What Problem Does It Solve?

Imagine you have years of smartwatch data for many users — heart rate, sleep, steps, stress, SpO2, breathing rate, and more. The pipeline:

1. **Ingests** that raw CSV data and turns it into a live data stream.
2. **Stores** every raw message so nothing is lost.
3. **Cleans and enriches** the data (fix types, remove bad values, deduplicate).
4. **Aggregates** it into easy-to-query daily summaries per user.
5. **Trains an AI model** to flag users whose health metrics look unusual (anomalies).
6. **Exports** everything to a dashboard so a human can see it.

---

## The Architecture at a Glance

```
CSV files
   │
   ▼
Kafka (message bus, 11 topics)
   │
   ▼
Bronze Layer (HDFS / Delta Lake) ← raw JSON, nothing changed
   │
   ▼
Silver Layer (HDFS / Delta Lake) ← cleaned, typed, deduplicated
   │
   ▼
Gold Layer (HDFS / Delta Lake)  ← daily aggregated summaries
   │                              + AI anomaly insights
   ▼
PostgreSQL Sink
   │
   ▼
Grafana Dashboard
```

This pattern is called a **Medallion Architecture** (Bronze → Silver → Gold) — each layer is more refined and trustworthy than the one before it.

---

## Input Data — What Goes In?

Two CSV files sit at the root of the project:

| File | Description |
|------|-------------|
| `hourly_fitbit_sema_df_unprocessed.csv` | Per-user, per-hour readings: heart rate (bpm), temperature, skin conductance (SCL), steps, calories, activity type, heart-rate zones, mood labels, location labels, mindfulness sessions, demographics |
| `daily_fitbit_sema_df_unprocessed.csv` | Per-user, per-day readings: sleep duration/stages/efficiency, HRV (RMSSD), breathing rate, SpO2, stress score, resting HR, VO2Max, nightly temperature |

These are **real-world research data** from participants wearing Fitbit devices.

---

## Stage 1 — Ingestion (Kafka Producers)

**Files:** `ingestion/producer.py`, `ingestion/producer_daily.py`, `ingestion/producer_realtime.py`

The producers read the CSV rows one by one and **publish each row as a JSON message** into Kafka topics. Think of Kafka as a postal sorting office — messages arrive, get labeled by topic, and sit in a queue until something downstream picks them up.

### The 11 Kafka Topics

| Category | Topic | What it carries |
|----------|-------|-----------------|
| Hourly | `wearable_vitals` | bpm, temperature, skin conductance |
| Hourly | `wearable_activity` | steps, calories, distance, activity type, zone minutes |
| Hourly | `wearable_context` | mood flags (HAPPY, SAD, ALERT …), location flags (HOME, GYM …), mindfulness |
| Hourly | `wearable_profile` | user demographics (age, gender, BMI, goals) — sent once per user |
| Daily | `wearable_sleep` | total sleep time, light/deep/REM stages, efficiency |
| Daily | `wearable_hrv_summary` | nightly heart-rate variability (RMSSD) |
| Daily | `wearable_breathing_summary` | average breathing rate during sleep |
| Daily | `wearable_vitals_daily` | resting HR, stress score, SpO2, VO2Max, temperature variation |
| Intraday | `wearable_heart_rate_intraday` | second-by-second HR |
| Intraday | `wearable_hrv_intraday` | second-by-second HRV |
| Intraday | `wearable_breathing_intraday` | second-by-second breathing rate |

---

## Stage 2 — Bronze Layer (Raw Storage)

**File:** `processing/spark_bronze.py`  
**Storage:** `hdfs://namenode:9000/data/bronze/wearable/<topic>/`

Spark reads from **all 11 Kafka topics simultaneously** using Structured Streaming. Every 30 seconds it flushes whatever arrived into Delta Lake files on HDFS.

**What changes here:** Almost nothing. The JSON payload is stored as-is. Only the Kafka envelope metadata is extracted (topic name, user ID, ingest timestamp, event date). This is the archive — a faithful copy of everything that was ever published.

---

## Stage 3 — Silver Layer (Clean & Deduplicate)

**File:** `processing/spark_silver.py`  
**Storage:** `hdfs://namenode:9000/data/silver/wearable/<topic>/`

Spark reads the Bronze Delta tables in **batch mode** and applies transformations per topic:

- **Parses** the raw JSON string into typed columns (FloatType, IntegerType, BooleanType, DateType …)
- **Filters out bad rows** — e.g., `bpm` outside 30–250, `steps` < 0, `spo2` outside 70–100
- **Casts types** correctly — CSV exports often store numbers as strings like `"72.5"`; Silver converts these to actual numbers
- **Deduplicates** — if the same `(user_id, event_timestamp)` pair arrives twice, only one row is kept (MERGE operation on Delta Lake)
- **Intraday topics** use append-only (MERGE is too expensive for per-second volumes)

After Silver you can trust the data: no garbage values, no duplicates, correct types.

---

## Stage 4 — Gold Layer (Aggregate & Summarize)

**File:** `processing/spark_gold.py`  
**Storage:** `hdfs://namenode:9000/data/gold/wearable/<table>/`

Spark reads Silver tables and produces **7 daily summary tables** — one row per user per day:

| Gold Table | What it contains |
|------------|-----------------|
| `daily_vitals_summary` | Avg/min/max bpm, avg temperature, avg SCL per user per day |
| `daily_activity_summary` | Total steps, calories, distance; dominant activity type; total zone minutes |
| `daily_context_summary` | Dominant mood, dominant location, mindfulness count per day |
| `daily_sleep_summary` | Total sleep minutes, sleep efficiency, light/deep/REM breakdown |
| `daily_vitals_daily_summary` | Resting HR, stress score, SpO2, VO2Max, HRV (from daily sensors) |
| `daily_intraday_summary` | Avg/max/stddev of intraday HR, HRV, breathing (from per-second streams) |
| `daily_wellness_profile` | All of the above joined into one wide row — the "master" wellness record |

Gold is always **fully overwritten** each run — it is 100% derived from Silver, so it is safe to regenerate.

---

## Stage 5 — AI Anomaly Detection

**Files:** `ml_module/train.py`, `ml_module/model/attention_lstm.py`, `ml_module/serve.py`

An **AttentionLSTM autoencoder** (PyTorch) is trained on the Silver data to learn what "normal" health patterns look like over 7-day rolling windows.

### Features used by the model (7 features × 7 days = input shape 7×7)

| Feature | Source |
|---------|--------|
| `resting_hr` | vitals_daily |
| `stress_score` | vitals_daily |
| `spo2` | vitals_daily |
| `minutes_asleep` | sleep |
| `sleep_efficiency` | sleep |
| `steps` | activity |
| `calories` | activity |

### How anomaly detection works

1. The autoencoder tries to **reconstruct** each 7-day window.
2. A high **reconstruction error** means the window looks unusual compared to what the model learned.
3. Errors are z-scored per user and classified into health status levels:

| Label | Z-score threshold | Meaning |
|-------|------------------|---------|
| `normal` | < 1.04 | Everything looks typical |
| `green` | ≥ 1.04 | Slightly elevated — monitor |
| `yellow` | ≥ 1.29 | Moderately anomalous — pay attention |
| `red` | ≥ 1.65 | Strongly anomalous — may need action |

Results are written to a Gold Delta table called **`ai_insights`** and logged to MLflow (experiment: `wellness-anomaly-detector`, model: `wellness-detector`).

Training is triggered via the FastAPI service at `POST http://localhost:8000/train`.

---

## Stage 6 — Data Export to PostgreSQL

**File:** `processing/spark_gold_to_postgres.py`

All 8 Gold tables (7 summaries + `ai_insights`) are exported from HDFS Delta into a dedicated **PostgreSQL database** (`wellness` DB on port 5434). Each export truncates and refills the target table — a clean overwrite.

This PostgreSQL instance exists purely as a **Grafana datasource** and is kept separate from the Airflow/MLflow metadata database.

---

## Stage 7 — Grafana Dashboard

Grafana (port 3000) connects to the PostgreSQL sink and visualizes the Gold tables. The datasource is auto-configured on startup. You build dashboards in the UI and export them to `grafana/provisioning/dashboards/`.

---

## Orchestration — Airflow

**File:** `orchestration/dags/pipeline_a.py`

Airflow runs the pipeline on a **daily schedule**. The DAG task order:

```
check_bronze
    └─► preprocess_data (Silver)
            └─► check_data
                    └─► if_new_data
                              ├─► request_ai_training (POST /train)
                              └─► compute_statistics (Gold)
                                        └─► check_gold_quality
                                                  └─► data_exposition (export-gold)
                                                            └─► notify_done
```

> **Note:** Bronze streaming is NOT managed by Airflow — it runs continuously as a long-lived Spark job (`make bronze`). The DAG only verifies that Bronze has data before proceeding.

---

## Services Overview

| Service | Port | Role |
|---------|------|------|
| HDFS NameNode | 9870 | Distributed file storage (stores Bronze/Silver/Gold data) |
| Kafka | 9092 | Message bus between producers and Bronze |
| Spark Master UI | 8080 | Monitor Spark jobs |
| Airflow UI | 8081 | Pipeline scheduler (`admin` / `pwd12345`) |
| MLflow UI | 5000 | AI experiment tracking and model registry |
| MinIO Console | 9101 | S3-compatible storage for MLflow model artifacts |
| FastAPI | 8000 | Trigger AI training (`/train`) and inference (`/predict`) |
| Grafana | 3000 | Dashboard UI (`admin` / `admin123`) |
| PostgreSQL (metadata) | 5433 | Airflow + MLflow internal database |
| PostgreSQL (sink) | 5434 | Gold data for Grafana queries |

---

## How to Run the Full Pipeline (Step by Step)

```bash
# 1. Start all Docker services
docker compose up -d

# 2. Create HDFS directories and Kafka topics (one-time setup)
make init

# 3. Start Bronze streaming job — keep this terminal open
make bronze

# 4. In a second terminal, push CSV data into Kafka
make producer          # hourly data (vitals, activity, context, profile)
make producer-daily    # daily data (sleep, HRV, breathing, vitals_daily)

# 5. After Bronze has flushed at least one batch (~30 seconds), run Silver
make silver

# 6. Run Gold aggregation
make gold

# 7. Train the AI model
make train-ai

# 8. Export Gold tables to PostgreSQL for Grafana
make export-gold

# 9. Open Grafana at http://localhost:3000 and build dashboards
```

---

## Output Summary

| Output | Format | Location | Used by |
|--------|--------|----------|---------|
| Bronze tables | Delta Lake (Parquet + transaction log) | HDFS `/data/bronze/wearable/` | Silver job |
| Silver tables | Delta Lake | HDFS `/data/silver/wearable/` | Gold job, AI training |
| Gold summary tables (×7) | Delta Lake | HDFS `/data/gold/wearable/` | Export job |
| AI insights table | Delta Lake | HDFS `/data/gold/wearable/ai_insights/` | Export job |
| All 8 Gold tables | PostgreSQL tables | `wellness` DB, port 5434 | Grafana |
| Trained AI model | PyTorch model artifact | MinIO / MLflow registry | FastAPI `/predict` |

---

## Glossary for Beginners

| Term | Plain-English meaning |
|------|-----------------------|
| **Delta Lake** | A file format layered on top of Parquet that supports ACID transactions, schema enforcement, and time-travel queries |
| **HDFS** | Hadoop Distributed File System — a distributed storage layer, here used as a local single-node store |
| **Kafka topic** | A named channel/queue in Kafka; producers write to it, consumers read from it |
| **Medallion Architecture** | Bronze = raw, Silver = clean, Gold = aggregated — each layer adds trust and structure |
| **Structured Streaming** | Spark's API for processing live data streams as if they were batch DataFrames |
| **MERGE** | A Delta Lake operation that upserts rows — insert if new, update if existing |
| **Autoencoder** | A neural network trained to compress then reconstruct its input; high reconstruction error signals an unusual input |
| **Reconstruction error** | How different the model's reconstructed output is from the original input — high = anomalous |
| **Z-score** | How many standard deviations a value is from the user's own mean — used to normalize anomaly scores per person |
