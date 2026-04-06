"""
BRONZE LAYER — Spark Structured Streaming ingest from Kafka to HDFS (Delta Lake).

Responsibility (Bronze):
  - Read raw events from Kafka with no business logic applied.
  - Promote envelope fields (user_id, timestamps, event_type) for partitioning.
  - Preserve `payload` as a raw JSON string — intentionally unparsed.
  - Write append-only, partitioned Delta tables to the Bronze zone on HDFS.

Reads from:
  wearable.vitals / wearable.activity / wearable.context / wearable.profile

Writes to:
  hdfs://namenode:9000/data/bronze/wearable/<topic>/   (Delta format)

Why Delta over plain Parquet:
  - 30-second micro-batches produce many small files; Delta OPTIMIZE compacts them.
  - Transaction log rolls back partial batch failures automatically.
  - Time travel (VERSION AS OF) allows replay or audit of any past batch.
  - Consistent format with Silver and Gold — no format boundary to cross.

What this layer does NOT do (handled by downstream Silver job):
  - Parse or type-cast the `payload` JSON fields.
  - Validate, deduplicate, or clean records.
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

TOPICS = "wearable.vitals,wearable.activity,wearable.context,wearable.profile"

# ── Session ───────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder.appName("WearableBronzeIngest")
    .config("spark.sql.shuffle.partitions", "4") 
    # fewer partitions for small data volume (Change to 72 for future benchmarking with larger datasets)
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
    get_json_object(col("raw_json"), "$.event_type").alias("event_type"),
    # payload is kept as a raw JSON string — Silver layer is responsible for parsing it
    get_json_object(col("raw_json"), "$.payload").alias("payload"),
)

# ── [Bronze] Write raw partitioned Parquet to HDFS Bronze zone ───────────────
TOPIC_NAMES = {
    "wearable.vitals": "vitals",
    "wearable.activity": "activity",
    "wearable.context": "context",
    "wearable.profile": "profile",
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
