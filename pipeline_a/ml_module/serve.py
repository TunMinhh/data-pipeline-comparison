"""
FastAPI serving layer for the wellness anomaly model.

Endpoints
---------
POST /train
    Kick off a training job in a background thread.
    Returns immediately with a job_id. Non-blocking so the Airflow DAG
    doesn't time out on a long curl.

GET /train/{job_id}
    Poll training job status.

POST /predict
    Load latest registered model from MLflow and run inference on
    the request's feature sequence. Body:
        { "sequence": [[f1, f2, ...], [f1, f2, ...], ...] }
    Shape must be (SEQ_LEN, n_features) matching training-time.

GET /health
    Liveness check + cached model version.
"""

from __future__ import annotations

import os
import threading
import traceback
import uuid
from datetime import datetime
from typing import Any

import mlflow
import mlflow.pytorch
import numpy as np
import torch
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from model import reconstruction_error

MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.getenv("MLFLOW_MODEL_NAME", "wellness-detector")

mlflow.set_tracking_uri(MLFLOW_URI)

app = FastAPI(title="Wellness AI", version="0.1.0")

# ── In-memory job registry ────────────────────────────────────────────────────
_jobs: dict[str, dict[str, Any]] = {}
_model_cache: dict[str, Any] = {"model": None, "version": None}


# ── Helpers ───────────────────────────────────────────────────────────────────
def _run_training(job_id: str) -> None:
    _jobs[job_id]["status"] = "running"
    _jobs[job_id]["started_at"] = datetime.utcnow().isoformat()
    try:
        # Import here so FastAPI boots fast even if Spark is slow to import.
        from train import main as train_main
        run_id = train_main()
        _jobs[job_id].update(status="succeeded", mlflow_run_id=run_id)
    except Exception as e:
        _jobs[job_id].update(
            status="failed",
            error=str(e),
            traceback=traceback.format_exc(),
        )
    finally:
        _jobs[job_id]["finished_at"] = datetime.utcnow().isoformat()


def _load_latest_model():
    if _model_cache["model"] is not None:
        return _model_cache["model"], _model_cache["version"]

    client = mlflow.MlflowClient()
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    if not versions:
        raise HTTPException(503, f"No registered model named '{MODEL_NAME}' yet. Call /train first.")
    latest = max(versions, key=lambda v: int(v.version))
    model = mlflow.pytorch.load_model(f"models:/{MODEL_NAME}/{latest.version}")
    model.eval()
    _model_cache["model"] = model
    _model_cache["version"] = latest.version
    return model, latest.version


# ── Schemas ───────────────────────────────────────────────────────────────────
class PredictRequest(BaseModel):
    sequence: list[list[float]]         # (seq_len, n_features)


class PredictResponse(BaseModel):
    reconstruction_error: float
    model_version: str


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "status": "ok",
        "mlflow_uri": MLFLOW_URI,
        "cached_model_version": _model_cache["version"],
        "active_jobs": [j for j, info in _jobs.items() if info["status"] == "running"],
    }


@app.post("/train")
def trigger_train():
    job_id = uuid.uuid4().hex[:12]
    _jobs[job_id] = {
        "status": "queued",
        "created_at": datetime.utcnow().isoformat(),
    }
    threading.Thread(target=_run_training, args=(job_id,), daemon=True).start()
    # Also invalidate the cached model so next /predict picks up the new version.
    _model_cache["model"] = None
    _model_cache["version"] = None
    return {"job_id": job_id, "status": "queued"}


@app.get("/train/{job_id}")
def get_job(job_id: str):
    if job_id not in _jobs:
        raise HTTPException(404, f"job_id {job_id} not found")
    return _jobs[job_id]


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    model, version = _load_latest_model()
    x = np.asarray(req.sequence, dtype=np.float32)
    if x.ndim != 2:
        raise HTTPException(400, "sequence must be 2-D (seq_len, n_features)")
    tensor = torch.from_numpy(x).unsqueeze(0)   # add batch dim
    with torch.no_grad():
        recon, _ = model(tensor)
        err = reconstruction_error(tensor, recon).item()
    return PredictResponse(reconstruction_error=err, model_version=str(version))
