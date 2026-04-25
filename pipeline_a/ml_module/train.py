"""
AI training pipeline — Silver -> AttentionLSTM -> MLflow + Gold `ai_insights`.

Flow:
    1. Read Silver Delta tables from HDFS (vitals_daily, activity, sleep).
    2. Join per user per day, forward-fill gaps, standardize features.
    3. Build 7-day sliding windows per user.
    4. Train AttentionLSTM autoencoder.
    5. Score every window (reconstruction error).
    6. Z-score errors per user and classify: green/yellow/red.
    7. Log params/metrics/model to MLflow.
    8. Write `ai_insights` Gold Delta table.

Runs in local Spark mode inside the FastAPI container. Heavy but keeps the
container self-contained (no spark-submit indirection).
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime

import mlflow
import mlflow.pytorch
import numpy as np
import torch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, FloatType, StringType, StructField, StructType,
)
from sklearn.preprocessing import StandardScaler
from torch.utils.data import DataLoader, TensorDataset

from model import AttentionLSTM, reconstruction_error

# ── Config (env-driven) ───────────────────────────────────────────────────────
HDFS_SILVER = os.getenv("HDFS_SILVER_BASE", "hdfs://namenode:9000/data/silver/wearable")
HDFS_GOLD   = os.getenv("HDFS_GOLD_BASE",   "hdfs://namenode:9000/data/gold/wearable")
MLFLOW_URI  = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
EXPERIMENT  = os.getenv("MLFLOW_EXPERIMENT", "wellness-anomaly-detector")
MODEL_NAME  = os.getenv("MLFLOW_MODEL_NAME", "wellness-detector")

# Hyperparameters
SEQ_LEN      = 7
HIDDEN_SIZE  = 64
NUM_LAYERS   = 2
BATCH_SIZE   = 32
EPOCHS       = int(os.getenv("EPOCHS", "15"))
LR           = 1e-3

# Anomaly thresholds from the DeltaTrace paper (z-score on reconstruction error)
THRESH_GREEN  = 1.04
THRESH_YELLOW = 1.29
THRESH_RED    = 1.65

FEATURE_COLS = [
    # vitals_daily
    "resting_hr", "stress_score", "spo2",
    # sleep
    "minutes_asleep", "sleep_efficiency",
    # activity
    "steps", "calories",
]


# ── Spark ─────────────────────────────────────────────────────────────────────
def make_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("WellnessAITrain")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


# ── Feature extraction ────────────────────────────────────────────────────────
def build_feature_matrix(spark: SparkSession):
    """Join Silver tables -> pandas DataFrame indexed by (user_id, event_date)."""
    vitals = spark.read.format("delta").load(f"{HDFS_SILVER}/vitals_daily")
    sleep  = spark.read.format("delta").load(f"{HDFS_SILVER}/sleep")
    activity = (
        spark.read.format("delta").load(f"{HDFS_SILVER}/activity")
        .groupBy("user_id", "event_date")
        .agg(F.sum("steps").alias("steps"),
             F.sum("calories").alias("calories"))
    )

    joined = (
        vitals.select("user_id", "event_date",
                      "resting_hr", "stress_score", "spo2")
        .join(sleep.select("user_id", "event_date",
                           "minutes_asleep", "sleep_efficiency"),
              on=["user_id", "event_date"], how="outer")
        .join(activity, on=["user_id", "event_date"], how="outer")
        .dropna(subset=["user_id", "event_date"])
    )
    return joined.toPandas()


def build_sequences(df, scaler: StandardScaler | None = None):
    """Return (X, index_rows, scaler).  X shape = (N, SEQ_LEN, features)."""
    df = df.sort_values(["user_id", "event_date"]).reset_index(drop=True)

    # Forward-fill gaps per user, then mean-impute residual NaNs
    df[FEATURE_COLS] = df.groupby("user_id")[FEATURE_COLS].ffill().bfill()
    df[FEATURE_COLS] = df[FEATURE_COLS].fillna(df[FEATURE_COLS].mean())

    if scaler is None:
        scaler = StandardScaler().fit(df[FEATURE_COLS].values)
    df[FEATURE_COLS] = scaler.transform(df[FEATURE_COLS].values)

    X, index_rows = [], []
    for user_id, g in df.groupby("user_id"):
        vals = g[FEATURE_COLS].values
        dates = g["event_date"].tolist()
        for i in range(SEQ_LEN, len(g) + 1):
            X.append(vals[i - SEQ_LEN : i])
            index_rows.append((user_id, dates[i - 1]))     # window ends at this date

    return np.asarray(X, dtype=np.float32), index_rows, scaler


# ── Training ──────────────────────────────────────────────────────────────────
def train_model(X: np.ndarray) -> tuple[AttentionLSTM, list[float]]:
    tensor = torch.from_numpy(X)
    loader = DataLoader(TensorDataset(tensor), batch_size=BATCH_SIZE, shuffle=True)

    model = AttentionLSTM(
        input_size=len(FEATURE_COLS),
        hidden_size=HIDDEN_SIZE,
        num_layers=NUM_LAYERS,
    )
    opt = torch.optim.Adam(model.parameters(), lr=LR)
    loss_fn = torch.nn.MSELoss()

    losses = []
    for epoch in range(EPOCHS):
        model.train()
        running = 0.0
        for (batch,) in loader:
            opt.zero_grad()
            recon, _ = model(batch)
            loss = loss_fn(recon, batch)
            loss.backward()
            opt.step()
            running += loss.item() * batch.size(0)
        avg = running / len(tensor)
        losses.append(avg)
        print(f"[train] epoch {epoch + 1:02d}/{EPOCHS}  loss={avg:.5f}")

    return model, losses


# ── Anomaly scoring ───────────────────────────────────────────────────────────
def score_anomalies(model: AttentionLSTM, X: np.ndarray) -> np.ndarray:
    model.eval()
    tensor = torch.from_numpy(X)
    with torch.no_grad():
        recon, _ = model(tensor)
        errs = reconstruction_error(tensor, recon).numpy()
    return errs


def classify(z_scores: np.ndarray) -> list[str]:
    out = []
    for z in z_scores:
        if z >= THRESH_RED:       out.append("red")
        elif z >= THRESH_YELLOW:  out.append("yellow")
        elif z >= THRESH_GREEN:   out.append("green")
        else:                     out.append("normal")
    return out


# ── Write Gold ────────────────────────────────────────────────────────────────
def write_ai_insights(spark: SparkSession, index_rows, errs, z_scores, labels, run_id: str):
    schema = StructType([
        StructField("user_id",            StringType(), False),
        StructField("event_date",         DateType(),   False),
        StructField("reconstruction_error", FloatType(), False),
        StructField("z_score",            FloatType(),  False),
        StructField("severity",           StringType(), False),
        StructField("mlflow_run_id",      StringType(), False),
    ])
    rows = [
        (uid, d, float(e), float(z), lbl, run_id)
        for (uid, d), e, z, lbl in zip(index_rows, errs, z_scores, labels)
    ]
    df = spark.createDataFrame(rows, schema=schema)
    (df.write.format("delta").mode("overwrite")
       .partitionBy("event_date")
       .save(f"{HDFS_GOLD}/ai_insights"))
    print(f"[gold] wrote {len(rows)} ai_insights rows")


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> str:
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)

    spark = make_spark()
    try:
        pdf = build_feature_matrix(spark)
        print(f"[data] loaded {len(pdf)} user-day rows")

        X, index_rows, _scaler = build_sequences(pdf)
        print(f"[data] built {len(X)} windows of shape {X.shape[1:]}")

        if len(X) < 50:
            raise RuntimeError(
                f"Only {len(X)} training windows — need Silver data across "
                "at least a week per user."
            )

        with mlflow.start_run() as run:
            mlflow.log_params({
                "seq_len": SEQ_LEN, "hidden_size": HIDDEN_SIZE,
                "num_layers": NUM_LAYERS, "epochs": EPOCHS,
                "batch_size": BATCH_SIZE, "lr": LR,
                "n_features": len(FEATURE_COLS), "n_windows": len(X),
            })

            model, losses = train_model(X)
            mlflow.log_metric("final_train_loss", losses[-1])
            for i, l in enumerate(losses):
                mlflow.log_metric("train_loss", l, step=i)

            errs = score_anomalies(model, X)
            mu, sigma = errs.mean(), errs.std() + 1e-8
            z = (errs - mu) / sigma
            labels = classify(z)

            mlflow.log_metric("anomaly_rate_red",    (np.array(labels) == "red").mean())
            mlflow.log_metric("anomaly_rate_yellow", (np.array(labels) == "yellow").mean())
            mlflow.log_metric("anomaly_rate_green",  (np.array(labels) == "green").mean())

            mlflow.pytorch.log_model(
                pytorch_model=model,
                artifact_path="model",
                registered_model_name=MODEL_NAME,
            )

            write_ai_insights(spark, index_rows, errs, z, labels, run.info.run_id)
            print(f"[done] MLflow run_id={run.info.run_id}")
            return run.info.run_id
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
