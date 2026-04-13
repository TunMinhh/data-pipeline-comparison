"""
Synthetic real-time intraday producer.

Replicates the paper's synthetic dataset: simulates 1 record/second/user for
high-frequency physiological signals that the static CSVs only provide as
hourly/daily summaries.

Strategy:
  1. Load base values from the hourly CSV (bpm) and daily CSV (rmssd, breathing
     rate) to derive realistic per-user signal ranges.
  2. Cycle through all (user, date, hour) entries in round-robin order.
  3. For each tick, emit one synthetic reading per user to all three intraday
     topics, adding small Gaussian noise around the base value.

Topics produced:
  wearable.heart_rate_intraday  – bpm per second
  wearable.hrv_intraday         – rmssd per second
  wearable.breathing_intraday   – breaths per minute, per second

Run:
    python producer_realtime.py
    DELAY=0.01 USERS_PER_TICK=10 python producer_realtime.py

Env vars:
  KAFKA_BOOTSTRAP   default localhost:9092
  HOURLY_CSV_PATH   default ../hourly_fitbit_sema_df_unprocessed.csv
  DAILY_CSV_PATH    default ../daily_fitbit_sema_df_unprocessed.csv
  DELAY             seconds between ticks (default 0.1 → 10 ticks/s)
  USERS_PER_TICK    users emitted per tick (default: all loaded users)
"""

import csv
import itertools
import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "localhost:9092")
HOURLY_CSV_PATH  = os.getenv("HOURLY_CSV_PATH",  "../hourly_fitbit_sema_df_unprocessed.csv")
DAILY_CSV_PATH   = os.getenv("DAILY_CSV_PATH",   "../daily_fitbit_sema_df_unprocessed.csv")
DELAY            = float(os.getenv("DELAY",       "0.1"))   # seconds between ticks
USERS_PER_TICK   = os.getenv("USERS_PER_TICK",   None)      # None = all users

TOPICS = [
    "wearable.heart_rate_intraday",
    "wearable.hrv_intraday",
    "wearable.breathing_intraday",
]

# Physiologically plausible fallback ranges (paper ref [34,35])
FALLBACK_BPM       = (60.0, 100.0)
FALLBACK_RMSSD     = (20.0, 80.0)
FALLBACK_BREATHING = (12.0, 20.0)

# Gaussian noise std-dev applied on top of each base value
NOISE_BPM       = 3.0   # ± bpm
NOISE_RMSSD     = 5.0   # ± ms
NOISE_BREATHING = 0.5   # ± breaths/min


# ── Topic initialisation ──────────────────────────────────────────────────────
def ensure_topics(bootstrap: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    new_topics = [NewTopic(t, num_partitions=3, replication_factor=1) for t in TOPICS]
    fs = admin.create_topics(new_topics)
    for topic, future in fs.items():
        try:
            future.result()
            print(f"[init] Created topic: {topic}")
        except Exception as e:
            if "TOPIC_ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
                pass
            else:
                raise


# ── Data loading ──────────────────────────────────────────────────────────────
def _safe_float(val: str, fallback: float) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return fallback


def load_user_pool() -> list[dict]:
    """
    Build a pool of (user_id, base_bpm, base_rmssd, base_breathing) entries
    from the hourly and daily CSVs.  One entry per (user, date, hour) so the
    round-robin naturally replays the temporal pattern of the original data.
    """
    # Step 1: load daily values keyed by (user_id, date)
    daily_lookup: dict[tuple, dict] = {}
    try:
        with open(DAILY_CSV_PATH, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                uid  = row.get("id", "")
                date = row.get("date", "")
                if not uid or not date:
                    continue
                daily_lookup[(uid, date)] = {
                    "rmssd":    _safe_float(row.get("rmssd", ""), random.uniform(*FALLBACK_RMSSD)),
                    "breathing": _safe_float(row.get("full_sleep_breathing_rate", ""),
                                             random.uniform(*FALLBACK_BREATHING)),
                }
        print(f"[realtime-producer] Loaded {len(daily_lookup)} daily (user, date) entries")
    except FileNotFoundError:
        print(f"[realtime-producer] Daily CSV not found at {DAILY_CSV_PATH}, using fallback values")

    # Step 2: build pool from hourly CSV
    pool: list[dict] = []
    try:
        with open(HOURLY_CSV_PATH, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                uid  = row.get("id", "")
                date = row.get("date", "")
                hour = row.get("hour", "0")
                if not uid or not date:
                    continue

                base_bpm = _safe_float(row.get("bpm", ""), random.uniform(*FALLBACK_BPM))
                daily    = daily_lookup.get((uid, date), {})
                base_rmssd     = daily.get("rmssd",     random.uniform(*FALLBACK_RMSSD))
                base_breathing = daily.get("breathing", random.uniform(*FALLBACK_BREATHING))

                pool.append({
                    "user_id":        uid,
                    "event_date":     date,
                    "event_hour":     hour,
                    "base_bpm":       base_bpm,
                    "base_rmssd":     base_rmssd,
                    "base_breathing": base_breathing,
                })
    except FileNotFoundError:
        print(f"[realtime-producer] Hourly CSV not found at {HOURLY_CSV_PATH}")

    print(f"[realtime-producer] Pool size: {len(pool)} entries across "
          f"{len({e['user_id'] for e in pool})} users")
    return pool


# ── Event builders ────────────────────────────────────────────────────────────
def _noisy(base: float, std: float, lo: float, hi: float) -> float:
    return round(max(lo, min(hi, random.gauss(base, std))), 3)


def build_intraday_events(entry: dict, ts: str) -> dict[str, dict]:
    base = {
        "user_id":         entry["user_id"],
        "event_date":      entry["event_date"],
        "event_hour":      entry["event_hour"],
        "event_timestamp": ts,
        "source":          "synthetic_intraday",
    }

    bpm       = _noisy(entry["base_bpm"],       NOISE_BPM,       30.0, 220.0)
    rmssd     = _noisy(entry["base_rmssd"],     NOISE_RMSSD,      0.0, 200.0)
    breathing = _noisy(entry["base_breathing"], NOISE_BREATHING,  5.0,  40.0)

    return {
        "wearable.heart_rate_intraday": {
            **base,
            "event_type": "heart_rate_intraday",
            "payload":    {"bpm": bpm},
        },
        "wearable.hrv_intraday": {
            **base,
            "event_type": "hrv_intraday",
            "payload":    {"rmssd": rmssd},
        },
        "wearable.breathing_intraday": {
            **base,
            "event_type": "breathing_intraday",
            "payload":    {"breaths_per_minute": breathing},
        },
    }


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    print(f"[realtime-producer] Kafka: {KAFKA_BOOTSTRAP}  delay: {DELAY}s/tick")

    ensure_topics(KAFKA_BOOTSTRAP)

    pool = load_user_pool()
    if not pool:
        print("[realtime-producer] No data loaded — exiting.")
        return

    users_per_tick = int(USERS_PER_TICK) if USERS_PER_TICK else len(
        {e["user_id"] for e in pool}
    )
    print(f"[realtime-producer] Emitting {users_per_tick} user(s) per tick")

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "retries": 3,
    })

    def serialize(v: dict) -> bytes:
        return json.dumps(v).encode("utf-8")

    # Round-robin through pool entries; each full cycle = one pass of the data
    pool_cycle = itertools.cycle(pool)
    tick = 0

    try:
        while True:
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

            for _ in range(users_per_tick):
                entry  = next(pool_cycle)
                events = build_intraday_events(entry, ts)
                uid    = entry["user_id"]

                for topic, event in events.items():
                    producer.produce(topic, key=uid, value=serialize(event))

            tick += 1
            if tick % 100 == 0:
                producer.flush()
                print(f"[realtime-producer] Tick {tick}  ts={ts}  "
                      f"msgs_this_tick={users_per_tick * len(TOPICS)}")

            producer.poll(0)
            time.sleep(DELAY)

    except KeyboardInterrupt:
        print(f"\n[realtime-producer] Interrupted at tick {tick}.")
    finally:
        producer.flush()
        print(f"[realtime-producer] Done. Total ticks: {tick}")


if __name__ == "__main__":
    main()
