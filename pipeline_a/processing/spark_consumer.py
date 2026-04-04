"""
Spark Structured Streaming consumer for wearable Kafka topics.

Reads from:
  wearable.vitals / wearable.activity / wearable.context / wearable.profile

Writes partitioned Parquet to HDFS under /data/wearable/<topic_name>/.

Submit from the spark-master container:
    docker exec -it <spark-master-container> \
      /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        /opt/spark/work-dir/spark_consumer.py

Or override settings with env vars:
    KAFKA_BOOTSTRAP=kafka:19092 HDFS_BASE=hdfs://namenode:9000/data/wearable python spark_consumer.py
"""

import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, get_json_object
from pyspark.sql.types import StringType

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
HDFS_BASE = os.getenv("HDFS_BASE", "hdfs://namenode:9000/data/wearable")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "hdfs://namenode:9000/checkpoints/wearable")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")

TOPICS = "wearable.vitals,wearable.activity,wearable.context,wearable.profile"

# ── Session ───────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder.appName("WearableStreamProcessor")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Ingest raw stream ─────────────────────────────────────────────────────────
raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPICS)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .load()
)

# ── Parse fields ──────────────────────────────────────────────────────────────
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
    get_json_object(col("raw_json"), "$.payload").alias("payload"),
)

# ── Write to HDFS via foreachBatch ────────────────────────────────────────────
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
                subset.write.mode("append")
                .partitionBy("event_date")
                .parquet(f"{HDFS_BASE}/{folder}")
            )

    batch_df.unpersist()
    print(f"[consumer] Wrote batch {batch_id}")


query = (
    enriched_df.writeStream.foreachBatch(write_batch)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/main")
    .trigger(processingTime="30 seconds")
    .start()
)

print("[consumer] Streaming started. Waiting for data …")
query.awaitTermination()
