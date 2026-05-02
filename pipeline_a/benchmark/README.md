# Pipeline A — Benchmark Guide

Measures end-to-end latency matching the DeltaTrace paper (Figures 2 & 4).

## What is measured

| Metric | Description |
|--------|-------------|
| `silver_duration_s` | Time Spark Silver job runs (Bronze → Silver) |
| `silver_e2e_s` | Time from first producer message → Silver ready |
| `gold_duration_s` | Time Spark Gold job runs (Silver → Gold) |
| `gold_e2e_s` | Time from first producer message → Gold ready |
| `worker_ram` | Spark worker peak RAM during Silver |

## How to run (GCP)

### Step 1 — Set SHUFFLE_PARTITIONS for GCP (8 vCPU)
```bash
# Edit Makefile line:
SHUFFLE_PARTITIONS ?= 16   # 2 × 8 cores
```

### Step 2 — Start everything
```bash
docker compose up -d
make init
```

### Step 3 — Start Bronze streaming (Terminal 1 — leave running)
```bash
make bronze
```

### Step 4 — Feed initial data (Terminal 2)
```bash
make producer
make producer-daily
# Wait for producers to finish (~2-3 min)
```

### Step 5 — Run benchmark (Terminal 2)
```bash
# Full benchmark: 50, 500, 1500 users/tick × 3 runs each
make benchmark

# Or custom rates:
make benchmark REQUEST_RATES=50,500,1500,3000 N_RUNS=3

# Quick smoke test first:
make benchmark-quick
```

### Step 6 — Copy results to local machine
```bash
gcloud compute scp <vm-name>:~/data-pipeline-comparison/pipeline_a/benchmark/ ./ \
  --recurse --zone=us-central1-a
```

## Output

Results saved to `benchmark/results_YYYYMMDD_HHMMSS.csv`:

```
timestamp, run, users_per_tick, req_per_sec, bronze_ready_s,
silver_duration_s, silver_e2e_s, gold_duration_s, gold_e2e_s,
silver_ok, gold_ok, worker_ram, namenode_ram
```

## Mapping to paper figures

| Paper Figure | This benchmark |
|-------------|----------------|
| Figure 2A — Silver latency vs req/s | `silver_e2e_s` vs `req_per_sec` |
| Figure 4A — Gold (anomaly) latency | `gold_e2e_s` vs `req_per_sec` |
| Figure 4B — Gold (stats) latency | `gold_duration_s` vs `req_per_sec` |

## Request rate mapping

| USERS_PER_TICK | DELAY | ≈ req/s |
|---------------|-------|---------|
| 2 | 0.1 | 60 |
| 17 | 0.1 | 510 |
| 50 | 0.1 | 1500 |
| 100 | 0.1 | 3000 |
