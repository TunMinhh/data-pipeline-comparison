"""
Pipeline A — End-to-end latency benchmark.

Replicates the DeltaTrace paper evaluation (Figure 2 & 4):
  - Measures time from first Kafka message → Silver available in HDFS
  - Measures time from first Kafka message → Gold available in HDFS
  - Runs at configurable request rates: USERS_PER_TICK × 3 / DELAY req/s
  - Repeats N_RUNS times and reports mean ± SD

Usage (run from pipeline_a/ directory):
    python benchmark/benchmark.py

Env vars:
    NAMENODE_URL    default http://localhost:9870
    KAFKA_BOOTSTRAP default localhost:9092
    SPARK_MASTER    default pipeline_a-spark-master-1
    N_RUNS          default 3
    WARMUP_SECS     default 30   (how long to run producer before Silver)
    REQUEST_RATES   default "50,500,1500"  (comma-separated USERS_PER_TICK values)
    DELAY           default 0.1  (seconds between ticks)
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from statistics import mean, stdev

import requests

# ── Config ────────────────────────────────────────────────────────────────────
NAMENODE_URL       = os.getenv("NAMENODE_URL",       "http://localhost:9870")
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP",    "localhost:9092")
SPARK_CONTAINER    = os.getenv("SPARK_MASTER",       "pipeline_a-spark-master-1")
N_RUNS             = int(os.getenv("N_RUNS",         "3"))
WARMUP_SECS        = int(os.getenv("WARMUP_SECS",    "30"))
DELAY              = float(os.getenv("DELAY",        "0.1"))
SILVER_EXEC        = os.getenv("SILVER_EXEC",        "512m")
SILVER_OVERHEAD    = os.getenv("SILVER_OVERHEAD",    "512m")
GOLD_EXEC          = os.getenv("GOLD_EXEC",          "1g")
GOLD_OVERHEAD      = os.getenv("GOLD_OVERHEAD",      "768m")
SHUFFLE_PARTITIONS = int(os.getenv("SHUFFLE_PARTITIONS", "4"))

# Request rates to test (USERS_PER_TICK values)
_rates_env = os.getenv("REQUEST_RATES", "50,500,1500")
REQUEST_RATES = [int(x.strip()) for x in _rates_env.split(",")]

# Spark submit base command (shared flags)
SPARK_SUBMIT_BASE = [
    "docker", "exec", SPARK_CONTAINER,
    "sh", "-lc",
    (
        "mkdir -p /tmp/.ivy2 && /opt/spark/bin/spark-submit "
        "--master spark://spark-master:7077 "
        "--driver-memory 512m "
        "--conf spark.jars.ivy=/tmp/.ivy2 "
        "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
        "--conf spark.sql.ansi.enabled=false "
        "--conf spark.sql.shuffle.partitions={partitions} "
        "--executor-memory {exec_mem} "
        "--conf spark.executor.memoryOverhead={overhead} "
        "--packages {packages} "
        "{script}"
    )
]

PKG_DELTA    = "io.delta:delta-spark_2.12:3.2.0"
PKG_POSTGRES = "org.postgresql:postgresql:42.7.3"

HDFS_SILVER = "hdfs://namenode:9000/data/silver/wearable"
HDFS_GOLD   = "hdfs://namenode:9000/data/gold/wearable"
HDFS_BRONZE = "hdfs://namenode:9000/data/bronze/wearable"
NAMENODE_CONTAINER = "pipeline_a-namenode-1"

RESULTS_FILE = f"benchmark/results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"


# ── Helpers ───────────────────────────────────────────────────────────────────
def now() -> float:
    return time.time()


def elapsed(start: float) -> float:
    return round(now() - start, 2)


def hdfs_has_data(path: str) -> bool:
    """Check if an HDFS path has at least one subdirectory (partition)."""
    result = subprocess.run(
        ["docker", "exec", NAMENODE_CONTAINER,
         "hdfs", "dfs", "-ls", path],
        capture_output=True, text=True
    )
    return result.returncode == 0 and len(result.stdout.strip()) > 0


def wait_for_hdfs(path: str, timeout: int = 600) -> float:
    """Wait until HDFS path has data. Returns seconds waited."""
    start = now()
    while elapsed(start) < timeout:
        if hdfs_has_data(path):
            return elapsed(start)
        time.sleep(5)
    raise TimeoutError(f"HDFS {path} never got data within {timeout}s")


def reset_silver_gold():
    """Clear Silver and Gold layers for a clean run."""
    print("[reset] Clearing Silver and Gold...")
    subprocess.run([
        "docker", "exec", NAMENODE_CONTAINER,
        "hdfs", "dfs", "-rm", "-r", "-f",
        "/data/silver/wearable/*",
        "/data/gold/wearable/*"
    ], capture_output=True)


def get_memory_usage() -> dict:
    """Get current memory usage of key containers."""
    result = subprocess.run(
        ["docker", "stats", "--no-stream", "--format",
         "{{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}"],
        capture_output=True, text=True
    )
    usage = {}
    for line in result.stdout.strip().split("\n"):
        parts = line.split("\t")
        if len(parts) == 3:
            name = parts[0].replace("pipeline_a-", "").replace("-1", "")
            usage[name] = {"usage": parts[1], "pct": parts[2]}
    return usage


def spark_submit(script: str, exec_mem: str, overhead: str,
                 packages: str, partitions: int = 8) -> tuple[float, bool]:
    """Run a spark-submit job. Returns (duration_seconds, success)."""
    cmd_str = (
        f"mkdir -p /tmp/.ivy2 && /opt/spark/bin/spark-submit "
        f"--master spark://spark-master:7077 "
        f"--driver-memory 512m "
        f"--conf spark.jars.ivy=/tmp/.ivy2 "
        f"--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        f"--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
        f"--conf spark.sql.ansi.enabled=false "
        f"--conf spark.sql.shuffle.partitions={partitions} "
        f"--executor-memory {exec_mem} "
        f"--conf spark.executor.memoryOverhead={overhead} "
        f"--packages {packages} "
        f"{script}"
    )
    start = now()
    result = subprocess.run(
        ["docker", "exec", SPARK_CONTAINER, "sh", "-lc", cmd_str],
        capture_output=True, text=True
    )
    duration = elapsed(start)
    success = result.returncode == 0
    if not success:
        print(f"  [ERROR] Job failed:\n{result.stderr[-500:]}")
    return duration, success


def start_producer(users_per_tick: int, delay: float) -> subprocess.Popen:
    """Start producer_realtime.py as a background process."""
    env = os.environ.copy()
    env.update({
        "KAFKA_BOOTSTRAP": KAFKA_BOOTSTRAP,
        "HOURLY_CSV_PATH": "./hourly_fitbit_sema_df_unprocessed.csv",
        "DAILY_CSV_PATH":  "./daily_fitbit_sema_df_unprocessed.csv",
        "USERS_PER_TICK":  str(users_per_tick),
        "DELAY":           str(delay),
    })
    venv_python = ".venv/bin/python"
    if not os.path.exists(venv_python):
        venv_python = "python3"
    proc = subprocess.Popen(
        [venv_python, "ingestion/producer_realtime.py"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    print(f"  [producer] Started PID={proc.pid}  "
          f"users_per_tick={users_per_tick}  delay={delay}s")
    return proc


def approx_req_per_sec(users_per_tick: int, delay: float) -> float:
    """Approximate requests/sec = users × 3 topics / delay."""
    return round(users_per_tick * 3 / delay, 1)


# ── Single benchmark run ──────────────────────────────────────────────────────
def run_once(users_per_tick: int, run_idx: int) -> dict:
    req_s = approx_req_per_sec(users_per_tick, DELAY)
    print(f"\n{'='*60}")
    print(f"Run {run_idx}  |  users_per_tick={users_per_tick}  "
          f"≈{req_s} req/s  delay={DELAY}s")
    print(f"{'='*60}")

    reset_silver_gold()
    time.sleep(3)  # let HDFS settle

    # ── Start producer ────────────────────────────────────────────────────────
    t_start = now()
    producer = start_producer(users_per_tick, DELAY)

    print(f"  [wait] Letting Bronze accumulate for {WARMUP_SECS}s...")
    time.sleep(WARMUP_SECS)

    # ── Confirm Bronze has data ───────────────────────────────────────────────
    if not hdfs_has_data(f"{HDFS_BRONZE}/vitals"):
        print("  [WARN] Bronze/vitals not found — waiting extra 30s...")
        time.sleep(30)

    t_bronze_ready = elapsed(t_start)
    print(f"  [bronze] Data confirmed at {t_bronze_ready}s")
    mem_during_bronze = get_memory_usage()

    # ── Silver ────────────────────────────────────────────────────────────────
    print("  [silver] Starting Silver job...")
    t_silver_start = now()
    silver_duration, silver_ok = spark_submit(
        script="/opt/spark/work-dir/spark_silver.py",
        exec_mem=SILVER_EXEC, overhead=SILVER_OVERHEAD,
        packages=PKG_DELTA, partitions=SHUFFLE_PARTITIONS
    )
    t_silver_done = elapsed(t_start)
    mem_during_silver = get_memory_usage()
    print(f"  [silver] Done in {silver_duration}s  (end-to-end from producer: {t_silver_done}s)")

    # ── Gold ──────────────────────────────────────────────────────────────────
    print("  [gold] Starting Gold job...")
    gold_duration, gold_ok = spark_submit(
        script="/opt/spark/work-dir/spark_gold.py",
        exec_mem=GOLD_EXEC, overhead=GOLD_OVERHEAD,
        packages=PKG_DELTA, partitions=SHUFFLE_PARTITIONS
    )
    t_gold_done = elapsed(t_start)
    mem_during_gold = get_memory_usage()
    print(f"  [gold] Done in {gold_duration}s  (end-to-end from producer: {t_gold_done}s)")

    # ── Stop producer ─────────────────────────────────────────────────────────
    producer.terminate()
    producer.wait()
    print("  [producer] Stopped.")

    # ── Peak RAM ──────────────────────────────────────────────────────────────
    # Extract spark-worker RAM (executor) as it's the most relevant
    worker_mem = mem_during_silver.get("spark-worker", {}).get("usage", "N/A")
    namenode_mem = mem_during_silver.get("namenode", {}).get("usage", "N/A")

    return {
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "run":              run_idx,
        "users_per_tick":   users_per_tick,
        "req_per_sec":      req_s,
        "warmup_secs":      WARMUP_SECS,
        "bronze_ready_s":   t_bronze_ready,
        "silver_duration_s": round(silver_duration, 2),
        "silver_e2e_s":     round(t_silver_done, 2),   # producer start → Silver done
        "gold_duration_s":  round(gold_duration, 2),
        "gold_e2e_s":       round(t_gold_done, 2),     # producer start → Gold done
        "silver_ok":        silver_ok,
        "gold_ok":          gold_ok,
        "worker_ram":       worker_mem,
        "namenode_ram":     namenode_mem,
    }


# ── Multi-rate benchmark ──────────────────────────────────────────────────────
def main():
    os.makedirs("benchmark", exist_ok=True)
    all_results = []

    print(f"Pipeline A Benchmark")
    print(f"Request rates: {REQUEST_RATES} users/tick  ≈ "
          f"{[approx_req_per_sec(r, DELAY) for r in REQUEST_RATES]} req/s")
    print(f"Runs per rate: {N_RUNS}")
    print(f"Results → {RESULTS_FILE}")

    for users_per_tick in REQUEST_RATES:
        rate_results = []
        for run_idx in range(1, N_RUNS + 1):
            try:
                result = run_once(users_per_tick, run_idx)
                rate_results.append(result)
                all_results.append(result)
                # Save after each run so we don't lose data on crash
                _save_csv(all_results)
            except Exception as e:
                print(f"  [ERROR] Run {run_idx} failed: {e}")

        # Summary for this rate
        if rate_results:
            silver_times = [r["silver_duration_s"] for r in rate_results if r["silver_ok"]]
            gold_times   = [r["gold_duration_s"]   for r in rate_results if r["gold_ok"]]
            e2e_times    = [r["silver_e2e_s"]       for r in rate_results if r["silver_ok"]]

            print(f"\n--- Rate {users_per_tick} users/tick summary ---")
            if silver_times:
                print(f"  Silver:  mean={mean(silver_times):.1f}s  "
                      f"sd={stdev(silver_times) if len(silver_times)>1 else 0:.2f}s")
            if gold_times:
                print(f"  Gold:    mean={mean(gold_times):.1f}s  "
                      f"sd={stdev(gold_times) if len(gold_times)>1 else 0:.2f}s")
            if e2e_times:
                print(f"  E2E:     mean={mean(e2e_times):.1f}s  "
                      f"sd={stdev(e2e_times) if len(e2e_times)>1 else 0:.2f}s")

    print(f"\n✅ Benchmark complete. Results saved to {RESULTS_FILE}")
    _print_summary_table(all_results)


def _save_csv(results: list[dict]):
    if not results:
        return
    with open(RESULTS_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)


def _print_summary_table(results: list[dict]):
    print("\n" + "="*80)
    print(f"{'req/s':>8} | {'Silver mean':>12} | {'Silver SD':>10} | "
          f"{'Gold mean':>10} | {'Gold SD':>8} | {'E2E mean':>10}")
    print("-"*80)

    by_rate: dict[float, list] = {}
    for r in results:
        by_rate.setdefault(r["req_per_sec"], []).append(r)

    for rate in sorted(by_rate):
        runs = by_rate[rate]
        silvers = [r["silver_duration_s"] for r in runs if r["silver_ok"]]
        golds   = [r["gold_duration_s"]   for r in runs if r["gold_ok"]]
        e2es    = [r["silver_e2e_s"]       for r in runs if r["silver_ok"]]
        s_mean  = f"{mean(silvers):.1f}s" if silvers else "N/A"
        s_sd    = f"{stdev(silvers):.2f}s" if len(silvers) > 1 else "N/A"
        g_mean  = f"{mean(golds):.1f}s" if golds else "N/A"
        g_sd    = f"{stdev(golds):.2f}s" if len(golds) > 1 else "N/A"
        e_mean  = f"{mean(e2es):.1f}s" if e2es else "N/A"
        print(f"{rate:>8} | {s_mean:>12} | {s_sd:>10} | "
              f"{g_mean:>10} | {g_sd:>8} | {e_mean:>10}")


if __name__ == "__main__":
    main()
