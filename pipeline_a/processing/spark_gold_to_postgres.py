"""
PHASE 4 — Data Exposition

Export Gold Delta tables -> PostgreSQL sink (read by Grafana for dashboards).

Each Gold table becomes one PostgreSQL table of the same name. Mode is
overwrite (truncate + insert) — Gold is fully re-derived each pipeline run,
so there's no state to preserve in the sink.

Driver: PostgreSQL JDBC 42.7.3, supplied to spark-submit via --packages
        org.postgresql:postgresql:42.7.3

Run:
    make export-gold
"""

from __future__ import annotations

import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

# ── Config ────────────────────────────────────────────────────────────────────
HDFS_GOLD_BASE = os.getenv("HDFS_GOLD_BASE", "hdfs://namenode:9000/data/gold/wearable")

PG_HOST = os.getenv("PG_SINK_HOST", "postgres-sink")
PG_PORT = os.getenv("PG_SINK_PORT", "5432")
PG_DB   = os.getenv("PG_SINK_DB",   "wellness")
PG_USER = os.getenv("PG_SINK_USER", "grafana")
PG_PASS = os.getenv("PG_SINK_PASSWORD", "grafana123")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {
    "user": PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified",   # let PG infer types for jsonb / etc.
}

GOLD_TABLES = [
    "daily_vitals_summary",
    "daily_activity_summary",
    "daily_context_summary",
    "daily_sleep_summary",
    "daily_vitals_daily_summary",
    "daily_intraday_summary",
    "daily_wellness_profile",
    "ai_insights",
]

# ── Session ───────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder.appName("WearableGoldToPostgres")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def export_table(name: str) -> None:
    path = f"{HDFS_GOLD_BASE}/{name}"
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"[export] {name} — Gold table missing, skipping.")
        return

    df = spark.read.format("delta").load(path)
    n = df.count()
    print(f"[export] {name} — {n} rows -> {JDBC_URL} (table: {name})")

    (
        df.write.mode("overwrite")
        .option("truncate", "true")           # keep table schema; just refill rows
        .jdbc(JDBC_URL, name, properties=JDBC_PROPS)
    )


if __name__ == "__main__":
    print(f"[export] Starting Gold -> Postgres export ({JDBC_URL}) …")
    for table in GOLD_TABLES:
        export_table(table)
    print("[export] All Gold tables exported.")
    spark.stop()
