FROM python:3.10-slim

# ── OS deps ──────────────────────────────────────────────────────────────────
# - openjdk-21-jre-headless : PySpark requires a JVM
# - curl                    : healthcheck
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-21-jre-headless curl \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PYSPARK_PYTHON=python3
# Download Spark packages once at build time into the ivy cache the jobs will use.
ENV SPARK_HOME=/usr/local/lib/python3.10/site-packages/pyspark

WORKDIR /app

# ── Python deps ──────────────────────────────────────────────────────────────
# torch is big; install it separately from the CPU-only wheel index to keep
# the image small (no CUDA libs).
COPY requirements.txt .
RUN pip install --no-cache-dir torch==2.2.2 \
      --index-url https://download.pytorch.org/whl/cpu \
 && pip install --no-cache-dir -r requirements.txt

# ── App code ─────────────────────────────────────────────────────────────────
COPY . /app

EXPOSE 8000

# Use uvicorn directly (no gunicorn in this project) — single worker because
# training jobs run in-process via a background thread.
CMD ["uvicorn", "serve:app", "--host", "0.0.0.0", "--port", "8000"]
