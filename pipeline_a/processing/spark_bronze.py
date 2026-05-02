"""
BRONZE LAYER — Spark Structured Streaming ingest from Kafka to HDFS (Delta Lake).
See processing/README.md for full documentation.
"""

import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, get_json_object
from pyspark.sql.types import StringType

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
HDFS_BRONZE_BASE = os.getenv("HDFS_BRONZE_BASE", "hdfs://namenode:9000/data/bronze/wearable")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "hdfs://namenode:9000/checkpoints/bronze/wearable")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")

TOPICS = (
    "wearable_vitals,wearable_activity,wearable_context,wearable_profile,"
    "wearable_sleep,wearable_hrv_summary,wearable_breathing_summary,wearable_vitals_daily,"
    "wearable_heart_rate_intraday,wearable_hrv_intraday,wearable_breathing_intraday"
)
# Default 4 suits a single-node local setup. On a VPS set SHUFFLE_PARTITIONS
# to 2× the total number of executor cores across all workers.
SHUFFLE_PARTITIONS = os.getenv("SHUFFLE_PARTITIONS", "4")

# ── Session ───────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder.appName("WearableBronzeIngest")
    .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── [Bronze] Read raw stream from Kafka ──────────────────────────────────────
raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPICS)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .load()
)

# ── [Bronze] Promote envelope fields; payload stays as raw JSON string ────────
parsed_df = raw_df.select(
    col("topic"),
    col("key").cast(StringType()).alias("user_id"),
    col("value").cast(StringType()).alias("raw_json"),
    col("timestamp").alias("kafka_ingest_time"),
)

enriched_df = parsed_df.select(
    col("topic"),
    col("user_id"),
    col("kafka_ingest_time"),
    get_json_object(col("raw_json"), "$.event_date").alias("event_date"),
    get_json_object(col("raw_json"), "$.event_hour").alias("event_hour"),
    get_json_object(col("raw_json"), "$.event_timestamp").alias("event_timestamp"),
    get_json_object(col("raw_json"), "$.source_timestamp").alias("source_timestamp"),
    get_json_object(col("raw_json"), "$.trace_id").alias("trace_id"),
    get_json_object(col("raw_json"), "$.event_type").alias("event_type"),
    # payload is kept as a raw JSON string — Silver layer is responsible for parsing it
    get_json_object(col("raw_json"), "$.payload").alias("payload"),
)

# ── [Bronze] Write raw partitioned Parquet to HDFS Bronze zone ───────────────
TOPIC_NAMES = {
    # Hourly producer
    "wearable_vitals":              "vitals",
    "wearable_activity":            "activity",
    "wearable_context":             "context",
    "wearable_profile":             "profile",
    # Daily producer
    "wearable_sleep":               "sleep",
    "wearable_hrv_summary":         "hrv_summary",
    "wearable_breathing_summary":   "breathing_summary",
    "wearable_vitals_daily":        "vitals_daily",
    # Synthetic real-time producer
    "wearable_heart_rate_intraday": "heart_rate_intraday",
    "wearable_hrv_intraday":        "hrv_intraday",
    "wearable_breathing_intraday":  "breathing_intraday",
}


def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    # Cache once; we filter it 4 times below
    batch_df.cache()

    for topic, folder in TOPIC_NAMES.items():
        subset = batch_df.filter(col("topic") == topic).drop("topic")
        if not subset.rdd.isEmpty():
            (
                subset.write.format("delta")
                .mode("append")
                .partitionBy("event_date")
                .save(f"{HDFS_BRONZE_BASE}/{folder}")
            )

    batch_df.unpersist()
    print(f"[bronze] Wrote batch {batch_id}")


query = (
    enriched_df.writeStream.foreachBatch(write_batch)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/main")
    .trigger(processingTime="30 seconds")
    .start()
)

print("[bronze] Streaming started. Waiting for data …")
query.awaitTermination()
