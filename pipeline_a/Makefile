# =============================================================================
# CONFIG
# =============================================================================

PYTHON          := /home/minhubuntu/projects/data-pipeline-comparison/pipeline_a/.venv/bin/python
CSV_HOURLY      := ./hourly_fitbit_sema_df_unprocessed.csv
CSV_DAILY       := ./daily_fitbit_sema_df_unprocessed.csv
KAFKA_BOOTSTRAP := localhost:9092

# Docker container IDs (resolved at runtime)
C_SPARK    := $(shell docker compose ps -q spark-master)
C_KAFKA    := $(shell docker compose ps -q kafka)
C_HDFS     := $(shell docker compose ps -q namenode)
C_AIRFLOW  := $(shell docker compose ps -q airflow)
C_FASTAPI  := $(shell docker compose ps -q fastapi)
C_PGSINK   := $(shell docker compose ps -q postgres-sink)
C_GRAFANA  := $(shell docker compose ps -q grafana)

# Spark packages (Maven coordinates)
PKG_KAFKA    := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
PKG_DELTA    := io.delta:delta-spark_2.12:3.2.0
PKG_POSTGRES := org.postgresql:postgresql:42.7.3

# -----------------------------------------------------------------------------
# Spark memory — driver runs in spark-master (640m container)
#                executor + overhead runs in spark-worker (3g container)
#
#   Bronze : 512m exec + 512m overhead = 1024m in worker
#   Silver : 512m exec + 512m overhead = 1024m in worker
#   Gold   : 1g   exec + 768m overhead = 1792m in worker  (heaviest)
# -----------------------------------------------------------------------------
DRIVER_MEM := 512m

BRONZE_EXEC     := 512m
BRONZE_OVERHEAD := 512m

SILVER_EXEC     := 512m
SILVER_OVERHEAD := 512m

GOLD_EXEC     := 1g
GOLD_OVERHEAD := 768m

# Kafka topics
ALL_TOPICS := \
	wearable_vitals \
	wearable_activity \
	wearable_context \
	wearable_profile \
	wearable_sleep \
	wearable_hrv_summary \
	wearable_breathing_summary \
	wearable_vitals_daily \
	wearable_heart_rate_intraday \
	wearable_hrv_intraday \
	wearable_breathing_intraday

# Realtime producer tuning  (req/s = USERS_PER_TICK × 3 / REALTIME_DELAY)
USERS_PER_TICK ?= 50
REALTIME_DELAY ?= 0.1

# Benchmark settings
# Override REQUEST_RATES for different load profiles, e.g.:
#   make benchmark REQUEST_RATES=50,500,1500
REQUEST_RATES  ?= 50,500,1500
N_RUNS         ?= 3
WARMUP_SECS    ?= 30

# Shuffle partitions — set to 2× executor cores for best performance
# Local dev (1 worker, 2 cores):  4
# GCP n2-standard-8 (8 vCPU):    16
SHUFFLE_PARTITIONS ?= 4

# =============================================================================
# PHONY
# =============================================================================

.PHONY: \
	init init-hdfs init-kafka \
	producer producer-daily producer-realtime \
	bronze silver gold export-gold \
	check-kafka check-hdfs check-silver check-gold \
	check-mlflow check-fastapi check-airflow \
	check-grafana check-pg-sink \
	train-ai \
	build-fastapi pyspark \
	benchmark benchmark-quick \
	reset reset-silver

# =============================================================================
# INIT
# =============================================================================

init: init-hdfs init-kafka

init-hdfs:
	docker exec $(C_HDFS) hdfs dfs -mkdir -p /data/bronze/wearable
	docker exec $(C_HDFS) hdfs dfs -mkdir -p /data/silver/wearable
	docker exec $(C_HDFS) hdfs dfs -mkdir -p /data/gold/wearable
	docker exec $(C_HDFS) hdfs dfs -mkdir -p /checkpoints/bronze/wearable
	docker exec $(C_HDFS) hdfs dfs -chmod -R 777 /data
	docker exec $(C_HDFS) hdfs dfs -chmod -R 777 /checkpoints

init-kafka:
	docker exec $(C_KAFKA) bash -lc '\
		cd /opt/kafka/bin && \
		for t in $(ALL_TOPICS); do \
			./kafka-topics.sh --bootstrap-server localhost:19092 \
				--create --if-not-exists \
				--topic "$$t" --partitions 3 --replication-factor 1; \
		done'

# =============================================================================
# PRODUCERS  (run on host, stream CSV data into Kafka)
# =============================================================================

producer:
	CSV_PATH=$(CSV_HOURLY) \
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
	$(PYTHON) ingestion/producer.py

producer-daily:
	DAILY_CSV_PATH=$(CSV_DAILY) \
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
	$(PYTHON) ingestion/producer_daily.py

producer-realtime:
	HOURLY_CSV_PATH=$(CSV_HOURLY) \
	DAILY_CSV_PATH=$(CSV_DAILY) \
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
	USERS_PER_TICK=$(USERS_PER_TICK) \
	DELAY=$(REALTIME_DELAY) \
	$(PYTHON) ingestion/producer_realtime.py

# =============================================================================
# SPARK JOBS
# =============================================================================

# Shared spark-submit flags injected into every job
define SPARK_SUBMIT
	docker exec $(C_SPARK) sh -lc ' \
		mkdir -p /tmp/.ivy2 && exec /opt/spark/bin/spark-submit \
			--master spark://spark-master:7077 \
			--driver-memory $(DRIVER_MEM) \
			--conf spark.jars.ivy=/tmp/.ivy2 \
			--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
			--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
			--conf spark.sql.ansi.enabled=false \
			--conf spark.sql.shuffle.partitions=$(SHUFFLE_PARTITIONS)
endef

# Bronze — streaming: Kafka → Bronze Delta (30s micro-batch)
bronze: init-hdfs init-kafka
	$(SPARK_SUBMIT) \
		--executor-memory $(BRONZE_EXEC) \
		--conf spark.executor.memoryOverhead=$(BRONZE_OVERHEAD) \
		--packages $(PKG_KAFKA),$(PKG_DELTA) \
		/opt/spark/work-dir/spark_bronze.py'

# Silver — batch: Bronze Delta → Silver Delta (MERGE dedup on user_id + timestamp)
#          Stop bronze first if memory is tight.
silver:
	$(SPARK_SUBMIT) \
		--executor-memory $(SILVER_EXEC) \
		--conf spark.executor.memoryOverhead=$(SILVER_OVERHEAD) \
		--packages $(PKG_DELTA) \
		/opt/spark/work-dir/spark_silver.py'

# Gold — batch: Silver Delta → 7 daily summary tables (overwrite, partitioned by date)
#        Heaviest job — run with bronze and silver stopped.
gold:
	$(SPARK_SUBMIT) \
		--executor-memory $(GOLD_EXEC) \
		--conf spark.executor.memoryOverhead=$(GOLD_OVERHEAD) \
		--packages $(PKG_DELTA) \
		/opt/spark/work-dir/spark_gold.py'

# Phase 4 — Export Gold Delta tables -> PostgreSQL sink for Grafana.
#           Lightweight (small daily aggregates) — same memory budget as Silver.
export-gold:
	$(SPARK_SUBMIT) \
		--executor-memory $(SILVER_EXEC) \
		--conf spark.executor.memoryOverhead=$(SILVER_OVERHEAD) \
		--packages $(PKG_DELTA),$(PKG_POSTGRES) \
		--conf spark.driverEnv.PG_SINK_HOST=postgres-sink \
		/opt/spark/work-dir/spark_gold_to_postgres.py'

# =============================================================================
# CHECKS
# =============================================================================

check-kafka:
	docker exec $(C_KAFKA) bash -lc \
		'cd /opt/kafka/bin && ./kafka-topics.sh --bootstrap-server localhost:19092 --list'

check-hdfs:
	docker exec $(C_HDFS) hdfs dfs -ls /data/bronze/wearable

check-silver:
	docker exec $(C_HDFS) hdfs dfs -ls /data/silver/wearable

check-gold:
	docker exec $(C_HDFS) hdfs dfs -ls /data/gold/wearable

check-mlflow:
	@curl -sf http://localhost:5000/health && echo "[mlflow] OK" || echo "[mlflow] UNREACHABLE"
	@curl -sf http://localhost:5000/api/2.0/mlflow/experiments/list \
		| python3 -c "import sys,json; exps=json.load(sys.stdin).get('experiments',[]); \
		  print(f'[mlflow] {len(exps)} experiment(s): {[e[\"name\"] for e in exps]}')" 2>/dev/null || true

check-fastapi:
	@curl -sf http://localhost:8000/health | python3 -m json.tool || echo "[fastapi] UNREACHABLE"

check-airflow:
	docker exec -u airflow $(C_AIRFLOW) airflow dags list
	@echo "---"
	docker exec -u airflow $(C_AIRFLOW) airflow tasks list pipeline_a

check-grafana:
	@curl -sf http://localhost:3000/api/health | python3 -m json.tool || echo "[grafana] UNREACHABLE"

check-pg-sink:
	docker exec $(C_PGSINK) psql -U $${PG_SINK_USER:-grafana} -d $${PG_SINK_DB:-wellness} \
		-c "\dt" \
		-c "SELECT 'daily_wellness_profile' AS t, COUNT(*) FROM daily_wellness_profile UNION ALL \
		    SELECT 'ai_insights', COUNT(*) FROM ai_insights;"

# Trigger AI training via FastAPI (non-blocking — returns job_id immediately)
train-ai:
	@curl -sf -X POST http://localhost:8000/train | python3 -m json.tool

# Build the FastAPI Docker image (run once before `docker compose up -d fastapi`)
build-fastapi:
	docker compose build fastapi

# Interactive PySpark shell with Delta Lake (spark-master must be running)
pyspark:
	docker exec -it $(C_SPARK) \
		/opt/spark/bin/pyspark \
		--conf spark.jars.ivy=/tmp/.ivy2 \
		--packages io.delta:delta-spark_2.12:3.2.0 \
		--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
		--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# =============================================================================
# RESET
# =============================================================================

reset:
	docker exec $(C_KAFKA) bash -lc '\
		cd /opt/kafka/bin && \
		for t in $$(./kafka-topics.sh --bootstrap-server localhost:19092 --list); do \
			./kafka-topics.sh --bootstrap-server localhost:19092 --delete --if-exists --topic "$$t"; \
		done'
	docker exec $(C_HDFS) hdfs dfs -rm -r -f /data/bronze/wearable/*
	docker exec $(C_HDFS) hdfs dfs -rm -r -f /data/silver/wearable/*
	docker exec $(C_HDFS) hdfs dfs -rm -r -f /data/gold/wearable/*
	docker exec $(C_HDFS) hdfs dfs -rm -r -f /checkpoints/*

reset-silver:
	docker exec $(C_HDFS) hdfs dfs -rm -r -f /data/silver/wearable/*
	@echo "[reset] Silver cleared — run: make silver"

# =============================================================================
# BENCHMARK
# =============================================================================

# Full benchmark: runs at REQUEST_RATES, N_RUNS times each
# Prerequisites: make bronze + make producer must have already run
benchmark:
	REQUEST_RATES=$(REQUEST_RATES) \
	N_RUNS=$(N_RUNS) \
	WARMUP_SECS=$(WARMUP_SECS) \
	bash benchmark/run_benchmark.sh

# Quick single-rate smoke test (50 users/tick, 1 run, 30s warmup)
benchmark-quick:
	REQUEST_RATES=50 N_RUNS=1 WARMUP_SECS=30 bash benchmark/run_benchmark.sh
