#!/usr/bin/env bash
# =============================================================================
# Pipeline A — benchmark runner
# Run from the pipeline_a/ directory: bash benchmark/run_benchmark.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

echo "============================================================"
echo "  Pipeline A Benchmark"
echo "  $(date)"
echo "============================================================"

# ── 1. Verify containers are running ────────────────────────────────────────
echo "[check] Verifying required containers..."
for svc in spark-master spark-worker namenode datanode kafka; do
  id=$(docker compose ps -q $svc 2>/dev/null || true)
  if [ -z "$id" ]; then
    echo "[ERROR] $svc container not running. Run: docker compose up -d"
    exit 1
  fi
  echo "  ✓ $svc"
done

# ── 2. Verify Bronze has data (streaming job must be running) ────────────────
echo "[check] Verifying Bronze has data..."
docker exec pipeline_a-namenode-1 hdfs dfs -ls /data/bronze/wearable/vitals > /dev/null 2>&1 || {
  echo "[ERROR] Bronze/vitals not found. Run: make bronze and make producer first."
  exit 1
}
echo "  ✓ Bronze data present"

# ── 3. Install Python deps if needed ────────────────────────────────────────
if [ -f ".venv/bin/python" ]; then
  PYTHON=".venv/bin/python"
else
  PYTHON="python3"
fi
$PYTHON -c "import requests" 2>/dev/null || {
  echo "[install] Installing requests..."
  $PYTHON -m pip install requests -q
}

# ── 4. Run the benchmark ─────────────────────────────────────────────────────
echo ""
# ── Load GCP Makefile overrides if present ───────────────────────────────────
if [ -f "Makefile.gcp" ]; then
  echo "[config] Loading GCP Makefile overrides from Makefile.gcp"
  # Extract KEY := VALUE lines and export them
  while IFS= read -r line; do
    if [[ "$line" =~ ^([A-Z_]+)[[:space:]]*:=[[:space:]]*(.+)$ ]]; then
      key="${BASH_REMATCH[1]}"
      val="${BASH_REMATCH[2]}"
      export "$key"="$val"
    fi
  done < Makefile.gcp
fi

echo "[benchmark] Starting..."
echo "  REQUEST_RATES:     ${REQUEST_RATES:-50,500,1500}"
echo "  N_RUNS:            ${N_RUNS:-3}"
echo "  WARMUP_SECS:       ${WARMUP_SECS:-30}"
echo "  SILVER_EXEC:       ${SILVER_EXEC:-512m}"
echo "  GOLD_EXEC:         ${GOLD_EXEC:-1g}"
echo "  SHUFFLE_PARTITIONS:${SHUFFLE_PARTITIONS:-4}"
echo ""

REQUEST_RATES="${REQUEST_RATES:-50,500,1500}" \
N_RUNS="${N_RUNS:-3}" \
WARMUP_SECS="${WARMUP_SECS:-30}" \
DELAY="${DELAY:-0.1}" \
SILVER_EXEC="${SILVER_EXEC:-512m}" \
SILVER_OVERHEAD="${SILVER_OVERHEAD:-512m}" \
GOLD_EXEC="${GOLD_EXEC:-1g}" \
GOLD_OVERHEAD="${GOLD_OVERHEAD:-768m}" \
SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-4}" \
$PYTHON benchmark/benchmark.py 2>&1 | tee benchmark/benchmark_$(date +%Y%m%d_%H%M%S).log

echo ""
echo "============================================================"
echo "  Done! Results saved in benchmark/"
echo "  Copy to local machine:"
echo "  gcloud compute scp <vm-name>:~/data-pipeline-comparison/pipeline_a/benchmark/ ./ --recurse --zone=us-central1-a"
echo "============================================================"
