"""
GOLD LAYER — Batch transformation: Silver Delta -> Gold Delta.
See processing/README.md for full documentation.

Outputs are partitioned by event_date and include daily summary tables for
vitals, activity, context, sleep, intraday signals, daily vitals, plus a
consolidated daily_wellness_profile for dashboarding and downstream use.

Run:
        make run-gold
"""

from __future__ import annotations

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
USER_DAY_KEYS      = ["user_id", "event_date"]

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


def _aggregate_daily(df: DataFrame, *agg_exprs) -> DataFrame:
    """Aggregate a Silver or Gold DataFrame to one row per user per day."""
    return df.groupBy(*USER_DAY_KEYS).agg(*agg_exprs)


def _finalize_gold(df: DataFrame, name: str) -> None:
    """Attach the audit timestamp, write the Gold table, and emit a completion log."""
    audited = df.withColumn("gold_updated_at", current_timestamp())
    write_gold(audited, name)
    print(f"[gold] {name} — done")


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


def _build_spine(dfs: list[DataFrame | None]) -> DataFrame:
    """Return distinct (user_id, event_date) keys across all provided DataFrames."""
    key_dfs = [df.select(*USER_DAY_KEYS) for df in dfs if df is not None]
    if not key_dfs:
        raise ValueError("At least one DataFrame is required to build a Gold spine")

    spine = key_dfs[0]
    for df in key_dfs[1:]:
        spine = spine.union(df)
    return spine.distinct()


def _join_daily_metrics(base_df: DataFrame, source_df: DataFrame | None, metric_cols: list[str]) -> DataFrame:
    """Left-join event_date scoped metrics when the upstream table exists."""
    if source_df is None:
        return base_df

    return base_df.join(
        source_df.select(*USER_DAY_KEYS, *metric_cols),
        on=USER_DAY_KEYS,
        how="left",
    )


def _join_user_profile(base_df: DataFrame, profile_df: DataFrame | None, profile_cols: list[str]) -> DataFrame:
    """Left-join user profile attributes when the profile Silver table exists."""
    if profile_df is None:
        return base_df

    return base_df.join(
        profile_df.select("user_id", *profile_cols),
        on="user_id",
        how="left",
    )


# ── Gold Table 1: daily_vitals_summary ───────────────────────────────────────
def build_daily_vitals_summary() -> None:
    silver = read_silver("vitals")
    if silver is None:
        return

    # One row per (user_id, event_date, event_hour) from Silver — aggregate directly to daily.
    daily = (
        _aggregate_daily(
            silver,
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
    )

    _finalize_gold(daily, "daily_vitals_summary")


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
        _aggregate_daily(
            silver_activity,
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
        .join(dominant_type, on=USER_DAY_KEYS, how="left")
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

    _finalize_gold(daily, "daily_activity_summary")


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
    daily = _aggregate_daily(silver, *daily_exprs)

    # Dominant mood: mood with the highest hours count for the day
    daily = _dominant(daily, "hours_", mood_cols)

    _finalize_gold(daily, "daily_context_summary")


# ── Gold Table 4: daily_sleep_summary ────────────────────────────────────────
def build_daily_sleep_summary() -> None:
    silver = read_silver("sleep")
    if silver is None:
        return

    # Silver/sleep already has one row per (user_id, event_date) — just select
    # and add the audit timestamp.
    daily = (
        _aggregate_daily(
            silver,
            avg("sleep_duration_ms").alias("avg_sleep_duration_ms"),
            avg("sleep_efficiency").alias("avg_sleep_efficiency"),
            avg("minutes_asleep").alias("avg_minutes_asleep"),
            avg("minutes_awake").alias("avg_minutes_awake"),
            avg("sleep_deep_ratio").alias("avg_sleep_deep_ratio"),
            avg("sleep_rem_ratio").alias("avg_sleep_rem_ratio"),
            avg("sleep_light_ratio").alias("avg_sleep_light_ratio"),
        )
    )

    _finalize_gold(daily, "daily_sleep_summary")


# ── Gold Table 5: daily_intraday_summary ─────────────────────────────────────
def build_daily_intraday_summary() -> None:
    """
    Aggregate per-second synthetic readings (heart rate, HRV, breathing) to
    daily stats per user.  This mirrors the paper's intraday stress-test data
    processed through the full medallion pipeline.
    """
    hr  = read_silver("heart_rate_intraday")
    hrv = read_silver("hrv_intraday")
    br  = read_silver("breathing_intraday")

    if hr is None and hrv is None and br is None:
        print("[gold] daily_intraday_summary — no intraday silver data, skipping.")
        return

    # Heart rate daily stats
    if hr is not None:
        hr_daily = (
            _aggregate_daily(
                hr,
                avg("bpm").alias("intraday_avg_bpm"),
                spark_min("bpm").alias("intraday_min_bpm"),
                spark_max("bpm").alias("intraday_max_bpm"),
                stddev("bpm").alias("intraday_stddev_bpm"),
                count("bpm").alias("intraday_hr_readings"),
            )
        )
    else:
        hr_daily = None

    # HRV daily stats
    if hrv is not None:
        hrv_daily = (
            _aggregate_daily(
                hrv,
                avg("rmssd").alias("intraday_avg_rmssd"),
                spark_min("rmssd").alias("intraday_min_rmssd"),
                spark_max("rmssd").alias("intraday_max_rmssd"),
            )
        )
    else:
        hrv_daily = None

    # Breathing daily stats
    if br is not None:
        br_daily = (
            _aggregate_daily(
                br,
                avg("breaths_per_minute").alias("intraday_avg_breathing"),
                spark_min("breaths_per_minute").alias("intraday_min_breathing"),
                spark_max("breaths_per_minute").alias("intraday_max_breathing"),
            )
        )
    else:
        br_daily = None

    spine = _build_spine([hr_daily, hrv_daily, br_daily])
    spine = _join_daily_metrics(
        spine,
        hr_daily,
        [
            "intraday_avg_bpm",
            "intraday_min_bpm",
            "intraday_max_bpm",
            "intraday_stddev_bpm",
            "intraday_hr_readings",
        ],
    )
    spine = _join_daily_metrics(
        spine,
        hrv_daily,
        ["intraday_avg_rmssd", "intraday_min_rmssd", "intraday_max_rmssd"],
    )
    spine = _join_daily_metrics(
        spine,
        br_daily,
        ["intraday_avg_breathing", "intraday_min_breathing", "intraday_max_breathing"],
    )

    _finalize_gold(spine, "daily_intraday_summary")


# ── Gold Table 6: daily_vitals_daily_summary ──────────────────────────────────
def build_daily_vitals_daily_summary() -> None:
    """Aggregate daily vitals (SpO2, stress, resting HR, VO2Max) per user per day."""
    silver = read_silver("vitals_daily")
    if silver is None:
        return

    daily = (
        _aggregate_daily(
            silver,
            avg("spo2").alias("avg_spo2"),
            avg("stress_score").alias("avg_stress_score"),
            avg("resting_hr").alias("avg_resting_hr"),
            avg("vo2max").alias("avg_vo2max"),
            avg("nightly_temperature").alias("avg_nightly_temperature"),
            avg("daily_temperature_variation").alias("avg_daily_temp_variation"),
        )
    )

    _finalize_gold(daily, "daily_vitals_daily_summary")


WELLNESS_SOURCE_COLUMNS = {
    "daily_vitals_summary": [
        "avg_bpm",
        "min_bpm",
        "max_bpm",
        "avg_temperature",
        "avg_scl",
        "vitals_hours_recorded",
    ],
    "daily_activity_summary": [
        "total_steps",
        "total_calories",
        "total_active_minutes",
        "total_minutes_zone_2",
        "total_minutes_zone_3",
        "goal_met_steps",
        "activity_hours_recorded",
    ],
    "daily_context_summary": [
        "dominant_mood",
        "mindfulness_sessions",
        "hours_tense_anxious",
        "hours_sad",
        "sema_readings_count",
    ],
    "daily_sleep_summary": [
        "avg_sleep_duration_ms",
        "avg_sleep_efficiency",
        "avg_sleep_deep_ratio",
        "avg_sleep_rem_ratio",
    ],
    "daily_intraday_summary": [
        "intraday_avg_bpm",
        "intraday_stddev_bpm",
        "intraday_avg_rmssd",
        "intraday_avg_breathing",
        "intraday_hr_readings",
    ],
    "daily_vitals_daily_summary": [
        "avg_spo2",
        "avg_stress_score",
        "avg_resting_hr",
        "avg_vo2max",
    ],
}
WELLNESS_PROFILE_COLUMNS = ["age", "gender", "bmi"]


# ── Gold Table 7: daily_wellness_profile ─────────────────────────────────────
def build_daily_wellness_profile() -> None:
    """
    Main doctor dashboard table. Joins all Gold tables plus profile demographics
    into one row per patient per day.

    Reads from Gold Delta so it can be re-run independently after refreshing
    one or more upstream Gold tables.
    """
    gold_sources = {
        source_name: read_gold(source_name)
        for source_name in WELLNESS_SOURCE_COLUMNS
    }
    profile = read_silver("profile")

    upstream_tables = list(gold_sources.values())
    if not any(df is not None for df in upstream_tables):
        print("[gold] daily_wellness_profile — no upstream Gold data, skipping.")
        return

    spine = _build_spine(upstream_tables)
    for source_name, metric_cols in WELLNESS_SOURCE_COLUMNS.items():
        spine = _join_daily_metrics(spine, gold_sources[source_name], metric_cols)
    spine = _join_user_profile(spine, profile, WELLNESS_PROFILE_COLUMNS)

    _finalize_gold(spine, "daily_wellness_profile")


GOLD_BUILDERS = (
    build_daily_vitals_summary,
    build_daily_activity_summary,
    build_daily_context_summary,
    build_daily_sleep_summary,
    build_daily_vitals_daily_summary,
    build_daily_intraday_summary,
    build_daily_wellness_profile,
)


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("[gold] Starting Gold transform …")
    for builder in GOLD_BUILDERS:
        builder()
    print("[gold] All Gold tables written.")
    spark.stop()
