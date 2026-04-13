"""
SILVER LAYER — Batch transformation: Bronze Delta → Silver Delta.
See processing/README.md for full documentation.
"""

import os
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    BooleanType, DateType, FloatType, IntegerType, StringType,
    StructField, StructType, TimestampType,
)

# ── Config ────────────────────────────────────────────────────────────────────
HDFS_BRONZE_BASE = os.getenv("HDFS_BRONZE_BASE", "hdfs://namenode:9000/data/bronze/wearable")
HDFS_SILVER_BASE = os.getenv("HDFS_SILVER_BASE", "hdfs://namenode:9000/data/silver/wearable")
SHUFFLE_PARTITIONS = os.getenv("SHUFFLE_PARTITIONS", "4")

# ── Session ───────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder.appName("WearableSilverTransform")
    .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Spark 3.5 enables ANSI mode by default — invalid casts (e.g. ">=30" → INT)
    # throw exceptions instead of returning NULL.  Turning it off makes bad casts
    # return NULL, which is then caught by dropna / range filters downstream.
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Payload schemas (must match keys emitted by ingestion/producer.py) ────────
VITALS_SCHEMA = StructType([
    StructField("bpm",         FloatType()),
    StructField("temperature", FloatType()),
    StructField("scl_avg",     FloatType()),
])

ACTIVITY_SCHEMA = StructType([
    StructField("calories",                   FloatType()),
    StructField("distance",                   FloatType()),
    # steps and zone minutes are float64 in the CSV ("134.0"), so FloatType here;
    # they are cast to IntegerType in the select below.
    StructField("steps",                      FloatType()),
    StructField("activityType",               StringType()),
    StructField("minutes_in_default_zone_1",  FloatType()),
    StructField("minutes_in_default_zone_2",  FloatType()),
    StructField("minutes_in_default_zone_3",  FloatType()),
    StructField("minutes_below_default_zone_1", FloatType()),
])

# Context flags are stored as "0.0"/"1.0" strings in the JSON (CSV origin).
# FloatType is used here so from_json can parse those strings correctly.
# The select below then casts each float to BooleanType (0.0→false, 1.0→true).
#
# mindfulness_session is different — CSV stores "False"/"True" strings (object dtype).
# StringType is used so from_json preserves them; the select casts via .cast(BooleanType())
# which handles "True"/"False" strings correctly (case-insensitive).
CONTEXT_SCHEMA = StructType([
    StructField("mindfulness_session", StringType()),
    StructField("ALERT",        FloatType()),
    StructField("HAPPY",        FloatType()),
    StructField("NEUTRAL",      FloatType()),
    StructField("RESTED_RELAXED", FloatType()),
    StructField("SAD",          FloatType()),
    StructField("TENSE_ANXIOUS", FloatType()),
    StructField("TIRED",        FloatType()),
    StructField("ENTERTAINMENT", FloatType()),
    StructField("GYM",          FloatType()),
    StructField("HOME",         FloatType()),
    StructField("HOME_OFFICE",  FloatType()),
    StructField("OTHER",        FloatType()),
    StructField("OUTDOORS",     FloatType()),
    StructField("TRANSIT",      FloatType()),
    StructField("WORK_SCHOOL",  FloatType()),
])

PROFILE_SCHEMA = StructType([
    StructField("badgeType",       StringType()),
    # age and bmi are string categories in the CSV (e.g. "<30", "<19") — not numeric.
    StructField("age",             StringType()),
    StructField("gender",          StringType()),
    StructField("bmi",             StringType()),
    # step_goal is object dtype in the CSV — kept as string.
    StructField("step_goal",       StringType()),
    # min_goal and max_goal are float64 in the CSV.
    StructField("min_goal",        FloatType()),
    StructField("max_goal",        FloatType()),
    StructField("step_goal_label", StringType()),
])

# ── Schemas for daily producer topics ────────────────────────────────────────
SLEEP_SCHEMA = StructType([
    StructField("sleep_duration",         FloatType()),   # milliseconds
    StructField("minutes_to_fall_asleep", FloatType()),
    StructField("minutes_asleep",         FloatType()),
    StructField("minutes_awake",          FloatType()),
    StructField("minutes_after_wakeup",   FloatType()),
    StructField("sleep_efficiency",       FloatType()),   # %
    StructField("sleep_deep_ratio",       FloatType()),
    StructField("sleep_wake_ratio",       FloatType()),
    StructField("sleep_light_ratio",      FloatType()),
    StructField("sleep_rem_ratio",        FloatType()),
])

HRV_SUMMARY_SCHEMA = StructType([
    StructField("nremhr", FloatType()),   # heart rate during non-REM sleep
    StructField("rmssd",  FloatType()),   # ms — root mean square of successive differences
])

BREATHING_SUMMARY_SCHEMA = StructType([
    StructField("full_sleep_breathing_rate", FloatType()),   # breaths per minute
])

VITALS_DAILY_SCHEMA = StructType([
    StructField("spo2",                         FloatType()),   # blood oxygen %
    StructField("stress_score",                 FloatType()),
    StructField("resting_hr",                   FloatType()),   # bpm
    StructField("filtered_demographic_vo2max",  FloatType()),   # mL/kg/min
    StructField("nightly_temperature",          FloatType()),   # °C relative
    StructField("daily_temperature_variation",  FloatType()),
    StructField("sleep_points_pct",             FloatType()),
    StructField("exertion_points_pct",          FloatType()),
    StructField("responsiveness_points_pct",    FloatType()),
])

# ── Schemas for synthetic real-time producer topics ───────────────────────────
HEART_RATE_INTRADAY_SCHEMA = StructType([
    StructField("bpm", FloatType()),
])

HRV_INTRADAY_SCHEMA = StructType([
    StructField("rmssd", FloatType()),
])

BREATHING_INTRADAY_SCHEMA = StructType([
    StructField("breaths_per_minute", FloatType()),
])


# ── Helpers ───────────────────────────────────────────────────────────────────
def read_bronze(topic: str) -> Optional[DataFrame]:
    """Return Bronze Delta DataFrame for a topic, or None if it doesn't exist yet."""
    path = f"{HDFS_BRONZE_BASE}/{topic}"
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"[silver] Bronze table not found at {path} — skipping.")
        return None
    return spark.read.format("delta").load(path)


def merge_to_silver(
    df: DataFrame,
    silver_path: str,
    merge_condition: str = "t.user_id = s.user_id AND t.event_timestamp = s.event_timestamp",
) -> None:
    """MERGE new rows into Silver Delta using merge_condition.
    On first run the table doesn't exist yet — falls back to a plain write.
    Re-runs are fully idempotent: only rows absent from Silver are inserted.
    """
    if DeltaTable.isDeltaTable(spark, silver_path):
        target = DeltaTable.forPath(spark, silver_path)
        (
            target.alias("t")
            .merge(
                df.alias("s"),
                merge_condition,
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # First run — table does not exist yet
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy("event_date")
            .save(silver_path)
        )


def base_columns(df: DataFrame) -> DataFrame:
    """Select the envelope columns that every topic shares, with proper types."""
    return df.select(
        col("user_id"),
        col("kafka_ingest_time"),
        # event_date arrives as a "YYYY-MM-DD" string from get_json_object; cast to DateType.
        col("event_date").cast(DateType()).alias("event_date"),
        # event_hour arrives as a "0.0"–"23.0" string; cast via float to avoid null from "0.0" → int.
        col("event_hour").cast(FloatType()).cast(IntegerType()).alias("event_hour"),
        # event_timestamp arrives as "YYYY-MM-DDTHH:00:00"; cast to TimestampType so that
        # malformed values become null and are caught by the dropna below.
        col("event_timestamp").cast(TimestampType()).alias("event_timestamp"),
        # source_timestamp is produced by producer_realtime so end-to-end timing can
        # use the original generation time for the synthetic intraday benchmark path.
        col("source_timestamp").cast(TimestampType()).alias("source_timestamp"),
        col("trace_id"),
        col("event_type"),
        col("payload"),
    )


# ── Per-topic transform functions ─────────────────────────────────────────────
def process_vitals() -> None:
    bronze = read_bronze("vitals")
    if bronze is None:
        return

    parsed = base_columns(bronze).withColumn("p", from_json(col("payload"), VITALS_SCHEMA))

    clean = (
        parsed.select(
            col("user_id"),
            col("kafka_ingest_time"),
            col("event_date"),
            col("event_hour"),
            col("event_timestamp"),
            col("event_type"),
            col("p.bpm").alias("bpm"),
            col("p.temperature").alias("temperature"),
            col("p.scl_avg").alias("scl_avg"),
            current_timestamp().alias("processed_at"),
        )
        # Drop rows missing identity fields
        .dropna(subset=["user_id", "event_timestamp"])
        # Range checks — null values are allowed (device may not record all sensors).
        # temperature is a relative wrist measurement (can be negative) — no range check applied.
        .filter(
            (col("bpm").isNull()     | col("bpm").between(30, 220)) &
            (col("scl_avg").isNull() | col("scl_avg").between(0.0, 30.0))
        )
        # Deduplicate within this batch before MERGE
        .dropDuplicates(["user_id", "event_timestamp"])
    )

    merge_to_silver(clean, f"{HDFS_SILVER_BASE}/vitals")
    print("[silver] vitals — done")


def process_activity() -> None:
    bronze = read_bronze("activity")
    if bronze is None:
        return

    parsed = base_columns(bronze).withColumn("p", from_json(col("payload"), ACTIVITY_SCHEMA))

    clean = (
        parsed.select(
            col("user_id"),
            col("kafka_ingest_time"),
            col("event_date"),
            col("event_hour"),
            col("event_timestamp"),
            col("event_type"),
            col("p.calories").alias("calories"),
            col("p.distance").alias("distance"),
            col("p.steps").cast(IntegerType()).alias("steps"),
            col("p.activityType").alias("activity_type"),
            col("p.minutes_in_default_zone_1").cast(IntegerType()).alias("minutes_zone_1"),
            col("p.minutes_in_default_zone_2").cast(IntegerType()).alias("minutes_zone_2"),
            col("p.minutes_in_default_zone_3").cast(IntegerType()).alias("minutes_zone_3"),
            col("p.minutes_below_default_zone_1").cast(IntegerType()).alias("minutes_below_zone_1"),
            current_timestamp().alias("processed_at"),
        )
        .dropna(subset=["user_id", "event_timestamp"])
        .filter(
            (col("calories").isNull() | (col("calories") >= 0)) &
            # distance is in metres (Fitbit hourly); sample data shows 98.3 m for 134 steps.
            # Only check non-negative — no realistic upper bound needed at hourly granularity.
            (col("distance").isNull() | (col("distance") >= 0.0)) &
            (col("steps").isNull()    | col("steps").between(0, 30000))
        )
        .dropDuplicates(["user_id", "event_timestamp"])
    )

    merge_to_silver(clean, f"{HDFS_SILVER_BASE}/activity")
    print("[silver] activity — done")


def process_context() -> None:
    bronze = read_bronze("context")
    if bronze is None:
        return

    parsed = base_columns(bronze).withColumn("p", from_json(col("payload"), CONTEXT_SCHEMA))

    clean = (
        parsed.select(
            col("user_id"),
            col("kafka_ingest_time"),
            col("event_date"),
            col("event_hour"),
            col("event_timestamp"),
            col("event_type"),
            col("p.mindfulness_session").cast(BooleanType()).alias("mindfulness_session"),
            col("p.ALERT").cast(BooleanType()).alias("alert"),
            col("p.HAPPY").cast(BooleanType()).alias("happy"),
            col("p.NEUTRAL").cast(BooleanType()).alias("neutral"),
            col("p.RESTED_RELAXED").cast(BooleanType()).alias("rested_relaxed"),
            col("p.SAD").cast(BooleanType()).alias("sad"),
            col("p.TENSE_ANXIOUS").cast(BooleanType()).alias("tense_anxious"),
            col("p.TIRED").cast(BooleanType()).alias("tired"),
            col("p.ENTERTAINMENT").cast(BooleanType()).alias("loc_entertainment"),
            col("p.GYM").cast(BooleanType()).alias("loc_gym"),
            col("p.HOME").cast(BooleanType()).alias("loc_home"),
            col("p.HOME_OFFICE").cast(BooleanType()).alias("loc_home_office"),
            col("p.OTHER").cast(BooleanType()).alias("loc_other"),
            col("p.OUTDOORS").cast(BooleanType()).alias("loc_outdoors"),
            col("p.TRANSIT").cast(BooleanType()).alias("loc_transit"),
            col("p.WORK_SCHOOL").cast(BooleanType()).alias("loc_work_school"),
            current_timestamp().alias("processed_at"),
        )
        .dropna(subset=["user_id", "event_timestamp"])
        .dropDuplicates(["user_id", "event_timestamp"])
    )

    merge_to_silver(clean, f"{HDFS_SILVER_BASE}/context")
    print("[silver] context — done")


def process_profile() -> None:
    bronze = read_bronze("profile")
    if bronze is None:
        return

    parsed = base_columns(bronze).withColumn("p", from_json(col("payload"), PROFILE_SCHEMA))

    clean = (
        parsed.select(
            col("user_id"),
            col("kafka_ingest_time"),
            col("event_date"),
            col("event_hour"),
            col("event_timestamp"),
            col("event_type"),
            col("p.badgeType").alias("badge_type"),
            col("p.age").alias("age"),
            col("p.gender").alias("gender"),
            col("p.bmi").alias("bmi"),
            col("p.step_goal").alias("step_goal"),
            col("p.min_goal").alias("min_goal"),
            col("p.max_goal").alias("max_goal"),
            col("p.step_goal_label").alias("step_goal_label"),
            current_timestamp().alias("processed_at"),
        )
        .dropna(subset=["user_id"])
        # age and bmi are string categories ("<30", "<19") — no numeric range check possible.
        # Profile is sent once per user; dedup on user_id alone
        .dropDuplicates(["user_id"])
    )

    # Profile is sent once per user — key on user_id alone so reruns don't create duplicates.
    merge_to_silver(clean, f"{HDFS_SILVER_BASE}/profile", merge_condition="t.user_id = s.user_id")
    print("[silver] profile — done")


# ── Daily topic process functions ─────────────────────────────────────────────
def process_sleep() -> None:
    bronze = read_bronze("sleep")
    if bronze is None:
        return

    parsed = base_columns(bronze).withColumn("p", from_json(col("payload"), SLEEP_SCHEMA))

    clean = (
        parsed.select(
            col("user_id"),
            col("kafka_ingest_time"),
            col("event_date"),
            col("event_hour"),
            col("event_timestamp"),
            col("event_type"),
            col("p.sleep_duration").alias("sleep_duration_ms"),
            col("p.minutes_to_fall_asleep").alias("minutes_to_fall_asleep"),
            col("p.minutes_asleep").alias("minutes_asleep"),
            col("p.minutes_awake").alias("minutes_awake"),
            col("p.minutes_after_wakeup").alias("minutes_after_wakeup"),
            col("p.sleep_efficiency").alias("sleep_efficiency"),
            col("p.sleep_deep_ratio").alias("sleep_deep_ratio"),
            col("p.sleep_wake_ratio").alias("sleep_wake_ratio"),
            col("p.sleep_light_ratio").alias("sleep_light_ratio"),
            col("p.sleep_rem_ratio").alias("sleep_rem_ratio"),
            current_timestamp().alias("processed_at"),
        )
        .dropna(subset=["user_id", "event_timestamp"])
        .filter(
            (col("sleep_efficiency").isNull() | col("sleep_efficiency").between(0.0, 100.0)) &
            (col("minutes_asleep").isNull()   | (col("minutes_asleep") >= 0))
        )
        .dropDuplicates(["user_id", "event_timestamp"])
    )

    merge_to_silver(clean, f"{HDFS_SILVER_BASE}/sleep")
    print("[silver] sleep — done")


def process_hrv_summary() -> None:
    bronze = read_bronze("hrv_summary")
    if bronze is None:
        return

    parsed = base_columns(bronze).withColumn("p", from_json(col("payload"), HRV_SUMMARY_SCHEMA))

    clean = (
        parsed.select(
            col("user_id"),
            col("kafka_ingest_time"),
            col("event_date"),
            col("event_hour"),
            col("event_timestamp"),
            col("event_type"),
            col("p.nremhr").alias("nremhr"),
            col("p.rmssd").alias("rmssd"),
            current_timestamp().alias("processed_at"),
        )
        .dropna(subset=["user_id", "event_timestamp"])
        .filter(
            (col("nremhr").isNull() | col("nremhr").between(30.0, 150.0)) &
            (col("rmssd").isNull()  | col("rmssd").between(0.0, 300.0))
        )
        .dropDuplicates(["user_id", "event_timestamp"])
    )

    merge_to_silver(clean, f"{HDFS_SILVER_BASE}/hrv_summary")
    print("[silver] hrv_summary — done")


def process_breathing_summary() -> None:
    bronze = read_bronze("breathing_summary")
    if bronze is None:
        return

    parsed = base_columns(bronze).withColumn("p", from_json(col("payload"), BREATHING_SUMMARY_SCHEMA))

    clean = (
        parsed.select(
            col("user_id"),
            col("kafka_ingest_time"),
            col("event_date"),
            col("event_hour"),
            col("event_timestamp"),
            col("event_type"),
            col("p.full_sleep_breathing_rate").alias("breathing_rate"),
            current_timestamp().alias("processed_at"),
        )
        .dropna(subset=["user_id", "event_timestamp"])
        .filter(col("breathing_rate").isNull() | col("breathing_rate").between(5.0, 40.0))
        .dropDuplicates(["user_id", "event_timestamp"])
    )

    merge_to_silver(clean, f"{HDFS_SILVER_BASE}/breathing_summary")
    print("[silver] breathing_summary — done")


def process_vitals_daily() -> None:
    bronze = read_bronze("vitals_daily")
    if bronze is None:
        return

    parsed = base_columns(bronze).withColumn("p", from_json(col("payload"), VITALS_DAILY_SCHEMA))

    clean = (
        parsed.select(
            col("user_id"),
            col("kafka_ingest_time"),
            col("event_date"),
            col("event_hour"),
            col("event_timestamp"),
            col("event_type"),
            col("p.spo2").alias("spo2"),
            col("p.stress_score").alias("stress_score"),
            col("p.resting_hr").alias("resting_hr"),
            col("p.filtered_demographic_vo2max").alias("vo2max"),
            col("p.nightly_temperature").alias("nightly_temperature"),
            col("p.daily_temperature_variation").alias("daily_temperature_variation"),
            col("p.sleep_points_pct").alias("sleep_points_pct"),
            col("p.exertion_points_pct").alias("exertion_points_pct"),
            col("p.responsiveness_points_pct").alias("responsiveness_points_pct"),
            current_timestamp().alias("processed_at"),
        )
        .dropna(subset=["user_id", "event_timestamp"])
        .filter(
            (col("spo2").isNull()       | col("spo2").between(70.0, 100.0)) &
            (col("resting_hr").isNull() | col("resting_hr").between(30.0, 150.0)) &
            (col("stress_score").isNull()| col("stress_score").between(0.0, 100.0))
        )
        .dropDuplicates(["user_id", "event_timestamp"])
    )

    merge_to_silver(clean, f"{HDFS_SILVER_BASE}/vitals_daily")
    print("[silver] vitals_daily — done")


# ── Intraday topic process functions (append-only — high volume) ──────────────
def append_to_silver(df, silver_path: str) -> None:
    """
    Append-only write for high-frequency intraday data.
    MERGE would be too expensive for per-second volume; we accept
    potential duplicate records on re-runs (synthetic data, acceptable).
    Partitioned by event_date so downstream Gold reads are efficient.
    """
    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("event_date")
        .save(silver_path)
    )


def process_heart_rate_intraday() -> None:
    bronze = read_bronze("heart_rate_intraday")
    if bronze is None:
        return

    parsed = base_columns(bronze).withColumn("p", from_json(col("payload"), HEART_RATE_INTRADAY_SCHEMA))

    clean = (
        parsed.select(
            col("user_id"),
            col("kafka_ingest_time"),
            col("event_date"),
            col("event_hour"),
            col("event_timestamp"),
            col("event_type"),
            col("p.bpm").alias("bpm"),
            current_timestamp().alias("processed_at"),
        )
        .dropna(subset=["user_id", "event_timestamp"])
        .filter(col("bpm").isNull() | col("bpm").between(30.0, 220.0))
    )

    append_to_silver(clean, f"{HDFS_SILVER_BASE}/heart_rate_intraday")
    print("[silver] heart_rate_intraday — done")


def process_hrv_intraday() -> None:
    bronze = read_bronze("hrv_intraday")
    if bronze is None:
        return

    parsed = base_columns(bronze).withColumn("p", from_json(col("payload"), HRV_INTRADAY_SCHEMA))

    clean = (
        parsed.select(
            col("user_id"),
            col("kafka_ingest_time"),
            col("event_date"),
            col("event_hour"),
            col("event_timestamp"),
            col("event_type"),
            col("p.rmssd").alias("rmssd"),
            current_timestamp().alias("processed_at"),
        )
        .dropna(subset=["user_id", "event_timestamp"])
        .filter(col("rmssd").isNull() | col("rmssd").between(0.0, 300.0))
    )

    append_to_silver(clean, f"{HDFS_SILVER_BASE}/hrv_intraday")
    print("[silver] hrv_intraday — done")


def process_breathing_intraday() -> None:
    bronze = read_bronze("breathing_intraday")
    if bronze is None:
        return

    parsed = base_columns(bronze).withColumn("p", from_json(col("payload"), BREATHING_INTRADAY_SCHEMA))

    clean = (
        parsed.select(
            col("user_id"),
            col("kafka_ingest_time"),
            col("event_date"),
            col("event_hour"),
            col("event_timestamp"),
            col("event_type"),
            col("p.breaths_per_minute").alias("breaths_per_minute"),
            current_timestamp().alias("processed_at"),
        )
        .dropna(subset=["user_id", "event_timestamp"])
        .filter(col("breaths_per_minute").isNull() | col("breaths_per_minute").between(5.0, 40.0))
    )

    append_to_silver(clean, f"{HDFS_SILVER_BASE}/breathing_intraday")
    print("[silver] breathing_intraday — done")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("[silver] Starting Silver transform …")
    # Hourly topics
    process_vitals()
    process_activity()
    process_context()
    process_profile()
    # Daily topics
    process_sleep()
    process_hrv_summary()
    process_breathing_summary()
    process_vitals_daily()
    # Intraday synthetic topics
    process_heart_rate_intraday()
    process_hrv_intraday()
    process_breathing_intraday()
    print("[silver] All topics processed.")
    spark.stop()
