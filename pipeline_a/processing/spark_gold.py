"""
GOLD LAYER — Batch transformation: Silver Delta → Gold Delta.
See processing/README.md for full documentation.

Four output tables (all partitioned by event_date):
  daily_vitals_summary    – daily BPM / temperature / SCL stats per patient
  daily_activity_summary  – daily steps / calories / zone minutes + step-goal compliance
  daily_context_summary   – daily mood burden + mindfulness sessions
  daily_wellness_profile  – one row per patient per day joining all three above + demographics

Run:
    make run-gold
"""

import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    current_timestamp,
    desc,
    greatest,
    lit,
    max as spark_max,
    min as spark_min,
    row_number,
    stddev,
    sum as spark_sum,
    when,
)
from pyspark.sql.types import BooleanType, FloatType, IntegerType
from pyspark.sql.window import Window

# ── Config ────────────────────────────────────────────────────────────────────
HDFS_SILVER_BASE   = os.getenv("HDFS_SILVER_BASE", "hdfs://namenode:9000/data/silver/wearable")
HDFS_GOLD_BASE     = os.getenv("HDFS_GOLD_BASE",   "hdfs://namenode:9000/data/gold/wearable")
SHUFFLE_PARTITIONS = os.getenv("SHUFFLE_PARTITIONS", "4")

# ── Session ───────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder.appName("WearableGoldTransform")
    .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ── Helpers ───────────────────────────────────────────────────────────────────
def read_silver(topic: str):
    """Return Silver Delta DataFrame, or None if the table doesn't exist yet."""
    path = f"{HDFS_SILVER_BASE}/{topic}"
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"[gold] Silver/{topic} not found — skipping.")
        return None
    return spark.read.format("delta").load(path)


def read_gold(name: str):
    """Return Gold Delta DataFrame, or None if the table doesn't exist yet."""
    path = f"{HDFS_GOLD_BASE}/{name}"
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"[gold] Gold/{name} not found — skipping.")
        return None
    return spark.read.format("delta").load(path)


def write_gold(df: DataFrame, name: str) -> None:
    """Overwrite Gold Delta table. Gold is fully re-derived from Silver each run."""
    path = f"{HDFS_GOLD_BASE}/{name}"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("event_date")
        .save(path)
    )


def _dominant(df: DataFrame, hour_col_prefix: str, candidates: list[str]) -> DataFrame:
    """
    Add a `dominant_<hour_col_prefix>` column holding the candidate name with
    the highest hours count for that row. Returns null when all counts are 0 or null.

    Expects columns named f"{hour_col_prefix}{c}" for each c in candidates.
    """
    prefixed = [f"{hour_col_prefix}{c}" for c in candidates]
    max_val = greatest(*[coalesce(col(c), lit(0)) for c in prefixed])

    expr = when(max_val == 0, None)
    for c, label in zip(prefixed, candidates):
        expr = expr.when(col(c) == max_val, label)
    expr = expr.otherwise(None)

    return df.withColumn(f"dominant_{hour_col_prefix.rstrip('_')}", expr)


def _mode_col(df: DataFrame, group_cols: list, value_col: str, out_alias: str) -> DataFrame:
    """
    Return a DataFrame (group_cols + out_alias) holding the most frequent
    value_col per group. Ties are broken alphabetically (deterministic).
    """
    w = Window.partitionBy(*group_cols).orderBy(desc("_cnt"), value_col)
    return (
        df.filter(col(value_col).isNotNull())
        .groupBy(*group_cols, value_col)
        .agg(count("*").alias("_cnt"))
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_cnt", "_rn")
        .withColumnRenamed(value_col, out_alias)
    )


# ── Gold Table 1: daily_vitals_summary ───────────────────────────────────────
def build_daily_vitals_summary() -> None:
    silver = read_silver("vitals")
    if silver is None:
        return

    # One row per (user_id, event_date, event_hour) from Silver — aggregate directly to daily.
    daily = (
        silver.groupBy("user_id", "event_date")
        .agg(
            avg("bpm").alias("avg_bpm"),
            spark_min("bpm").alias("min_bpm"),
            spark_max("bpm").alias("max_bpm"),
            stddev("bpm").alias("stddev_bpm"),
            avg("temperature").alias("avg_temperature"),
            spark_min("temperature").alias("min_temperature"),
            spark_max("temperature").alias("max_temperature"),
            avg("scl_avg").alias("avg_scl"),
            spark_max("scl_avg").alias("max_scl"),
            count("event_hour").alias("vitals_hours_recorded"),
        )
        .withColumn("gold_updated_at", current_timestamp())
    )

    write_gold(daily, "daily_vitals_summary")
    print("[gold] daily_vitals_summary — done")


# ── Gold Table 2: daily_activity_summary ─────────────────────────────────────
def build_daily_activity_summary() -> None:
    silver_activity = read_silver("activity")
    silver_profile  = read_silver("profile")
    if silver_activity is None:
        return

    # Dominant activity type for the day (mode across hours)
    dominant_type = _mode_col(
        silver_activity, ["user_id", "event_date"], "activity_type", "dominant_activity_type"
    )

    # One row per (user_id, event_date, event_hour) — aggregate directly to daily.
    daily = (
        silver_activity.groupBy("user_id", "event_date")
        .agg(
            spark_sum("steps").cast(IntegerType()).alias("total_steps"),
            spark_sum("calories").alias("total_calories"),
            spark_sum("distance").alias("total_distance_m"),
            spark_sum("minutes_zone_1").cast(IntegerType()).alias("total_minutes_zone_1"),
            spark_sum("minutes_zone_2").cast(IntegerType()).alias("total_minutes_zone_2"),
            spark_sum("minutes_zone_3").cast(IntegerType()).alias("total_minutes_zone_3"),
            spark_sum("minutes_below_zone_1").cast(IntegerType()).alias("total_minutes_below_zone_1"),
            count("event_hour").alias("activity_hours_recorded"),
        )
        .withColumn(
            "total_active_minutes",
            (
                coalesce(col("total_minutes_zone_1"), lit(0))
                + coalesce(col("total_minutes_zone_2"), lit(0))
                + coalesce(col("total_minutes_zone_3"), lit(0))
            ).cast(IntegerType()),
        )
        .join(dominant_type, on=["user_id", "event_date"], how="left")
    )

    # Step-goal compliance — only for users who have a step_goal set in profile
    if silver_profile is not None:
        goals = (
            silver_profile
            .select("user_id", col("step_goal").cast(FloatType()).alias("step_goal_f"))
            .filter(col("step_goal_f").isNotNull())
        )
        daily = (
            daily.join(goals, on="user_id", how="left")
            .withColumn(
                "goal_met_steps",
                when(
                    col("step_goal_f").isNotNull(),
                    (col("total_steps") >= col("step_goal_f")).cast(BooleanType()),
                ).otherwise(lit(None).cast(BooleanType())),
            )
            .drop("step_goal_f")
        )
    else:
        daily = daily.withColumn("goal_met_steps", lit(None).cast(BooleanType()))

    daily = daily.withColumn("gold_updated_at", current_timestamp())
    write_gold(daily, "daily_activity_summary")
    print("[gold] daily_activity_summary — done")


# ── Gold Table 3: daily_context_summary ──────────────────────────────────────
def build_daily_context_summary() -> None:
    silver = read_silver("context")
    if silver is None:
        return

    mood_cols = ["alert", "happy", "neutral", "rested_relaxed", "sad", "tense_anxious", "tired"]
    loc_cols  = ["loc_gym", "loc_home", "loc_work_school", "loc_outdoors", "loc_transit"]

    # One row per (user_id, event_date, event_hour) — SUM of boolean (0/1) = hours-per-day.
    daily_exprs = [spark_sum(col(c).cast(IntegerType())).alias(f"hours_{c}") for c in mood_cols]
    daily_exprs += [spark_sum(col(c).cast(IntegerType())).alias(f"hours_{c}") for c in loc_cols]
    daily_exprs += [
        spark_sum(col("mindfulness_session").cast(IntegerType())).alias("mindfulness_sessions"),
        count("event_hour").alias("sema_readings_count"),
    ]
    daily = silver.groupBy("user_id", "event_date").agg(*daily_exprs)

    # Dominant mood: mood with the highest hours count for the day
    daily = _dominant(daily, "hours_", mood_cols)

    daily = daily.withColumn("gold_updated_at", current_timestamp())
    write_gold(daily, "daily_context_summary")
    print("[gold] daily_context_summary — done")


# ── Gold Table 4: daily_wellness_profile ─────────────────────────────────────
def build_daily_wellness_profile() -> None:
    """
    Main doctor dashboard table. Joins the three intermediate Gold tables plus
    profile demographics into one row per patient per day.

    Reads from Gold Delta (not in-memory DataFrames) so this function can be
    run independently if only one upstream Gold table needs refreshing.
    """
    vitals   = read_gold("daily_vitals_summary")
    activity = read_gold("daily_activity_summary")
    context  = read_gold("daily_context_summary")
    profile  = read_silver("profile")

    if vitals is None and activity is None:
        print("[gold] daily_wellness_profile — no upstream Gold data, skipping.")
        return

    # Spine: every (user_id, event_date) across all three Gold tables
    # so a day with activity but no vitals is still represented
    spine_dfs = [df.select("user_id", "event_date") for df in [vitals, activity, context] if df is not None]
    spine = spine_dfs[0]
    for df in spine_dfs[1:]:
        spine = spine.union(df)
    spine = spine.distinct()

    # Left-join vitals signals
    if vitals is not None:
        spine = spine.join(
            vitals.select(
                "user_id", "event_date",
                "avg_bpm", "min_bpm", "max_bpm",
                "avg_temperature", "avg_scl",
                "vitals_hours_recorded",
            ),
            on=["user_id", "event_date"],
            how="left",
        )

    # Left-join activity signals
    if activity is not None:
        spine = spine.join(
            activity.select(
                "user_id", "event_date",
                "total_steps", "total_calories", "total_active_minutes",
                "total_minutes_zone_2", "total_minutes_zone_3",
                "goal_met_steps", "activity_hours_recorded",
            ),
            on=["user_id", "event_date"],
            how="left",
        )

    # Left-join context signals
    if context is not None:
        spine = spine.join(
            context.select(
                "user_id", "event_date",
                "dominant_mood", "mindfulness_sessions",
                "hours_tense_anxious", "hours_sad",
                "sema_readings_count",
            ),
            on=["user_id", "event_date"],
            how="left",
        )

    # Left-join profile demographics (user_id only — one row per user, no event_date)
    if profile is not None:
        spine = spine.join(
            profile.select("user_id", "age", "gender", "bmi"),
            on="user_id",
            how="left",
        )

    wellness = spine.withColumn("gold_updated_at", current_timestamp())
    write_gold(wellness, "daily_wellness_profile")
    print("[gold] daily_wellness_profile — done")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("[gold] Starting Gold transform …")
    build_daily_vitals_summary()
    build_daily_activity_summary()
    build_daily_context_summary()
    build_daily_wellness_profile()
    print("[gold] All Gold tables written.")
    spark.stop()
