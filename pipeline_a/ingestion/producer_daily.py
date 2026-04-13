"""
Daily wearable data producer.

Reads daily_fitbit_sema_df_unprocessed.csv and publishes to four NEW Kafka topics
that cover fields not present in the hourly CSV:

  wearable.sleep            – sleep duration, stages, efficiency
  wearable.hrv_summary      – nremhr, rmssd (nightly HRV)
  wearable.breathing_summary– full_sleep_breathing_rate
  wearable.vitals_daily     – spo2, stress_score, resting_hr, VO2Max,
                               nightly_temperature, daily_temperature_variation

Does NOT re-publish calories/steps/bpm/SEMA — those come from the hourly producer.
Profile events are deduplicated (sent once per unique user).

Run:
    python producer_daily.py
    KAFKA_BOOTSTRAP=localhost:9092 DELAY=1.0 DAILY_CSV_PATH=../daily_fitbit_sema_df_unprocessed.csv python producer_daily.py
"""

import csv
import json
import os
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "localhost:9092")
DAILY_CSV_PATH   = os.getenv("DAILY_CSV_PATH",   "../daily_fitbit_sema_df_unprocessed.csv")
# Daily data is slow-changing — 1 s between rows is fine
DELAY = float(os.getenv("DELAY", "1.0"))

TOPICS = [
    "wearable.sleep",
    "wearable.hrv_summary",
    "wearable.breathing_summary",
    "wearable.vitals_daily",
    "wearable.profile",          # shared with hourly producer; deduped by user_id
]


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


# ── Helpers ───────────────────────────────────────────────────────────────────
def _val(row: dict, key: str):
    v = row.get(key, "")
    return v if v != "" else None


def build_events(row: dict) -> dict[str, dict]:
    user_id    = row["id"]
    event_date = row.get("date", "")

    base = {
        "user_id":         user_id,
        "event_date":      event_date,
        "event_hour":      "0",                        # daily → always hour 0
        "event_timestamp": f"{event_date}T00:00:00",
        "source":          "fitbit_daily_csv",
    }

    sleep = {
        **base,
        "event_type": "sleep",
        "payload": {
            "sleep_duration":         _val(row, "sleep_duration"),
            "minutes_to_fall_asleep": _val(row, "minutesToFallAsleep"),
            "minutes_asleep":         _val(row, "minutesAsleep"),
            "minutes_awake":          _val(row, "minutesAwake"),
            "minutes_after_wakeup":   _val(row, "minutesAfterWakeup"),
            "sleep_efficiency":       _val(row, "sleep_efficiency"),
            "sleep_deep_ratio":       _val(row, "sleep_deep_ratio"),
            "sleep_wake_ratio":       _val(row, "sleep_wake_ratio"),
            "sleep_light_ratio":      _val(row, "sleep_light_ratio"),
            "sleep_rem_ratio":        _val(row, "sleep_rem_ratio"),
        },
    }

    hrv_summary = {
        **base,
        "event_type": "hrv_summary",
        "payload": {
            "nremhr": _val(row, "nremhr"),
            "rmssd":  _val(row, "rmssd"),
        },
    }

    breathing_summary = {
        **base,
        "event_type": "breathing_summary",
        "payload": {
            "full_sleep_breathing_rate": _val(row, "full_sleep_breathing_rate"),
        },
    }

    vitals_daily = {
        **base,
        "event_type": "vitals_daily",
        "payload": {
            "spo2":                        _val(row, "spo2"),
            "stress_score":               _val(row, "stress_score"),
            "resting_hr":                 _val(row, "resting_hr"),
            "filtered_demographic_vo2max": _val(row, "filteredDemographicVO2Max"),
            "nightly_temperature":        _val(row, "nightly_temperature"),
            "daily_temperature_variation":_val(row, "daily_temperature_variation"),
            "sleep_points_pct":           _val(row, "sleep_points_percentage"),
            "exertion_points_pct":        _val(row, "exertion_points_percentage"),
            "responsiveness_points_pct":  _val(row, "responsiveness_points_percentage"),
        },
    }

    profile = {
        **base,
        "event_type": "profile",
        "payload": {
            "badgeType":      _val(row, "badgeType"),
            "age":            _val(row, "age"),
            "gender":         _val(row, "gender"),
            "bmi":            _val(row, "bmi"),
            "step_goal":      _val(row, "step_goal"),
            "min_goal":       _val(row, "min_goal"),
            "max_goal":       _val(row, "max_goal"),
            "step_goal_label":_val(row, "step_goal_label"),
        },
    }

    return {
        "wearable.sleep":             sleep,
        "wearable.hrv_summary":       hrv_summary,
        "wearable.breathing_summary": breathing_summary,
        "wearable.vitals_daily":      vitals_daily,
        "wearable.profile":           profile,
    }


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    print(f"[daily-producer] Kafka: {KAFKA_BOOTSTRAP}  CSV: {DAILY_CSV_PATH}  delay: {DELAY}s/row")

    ensure_topics(KAFKA_BOOTSTRAP)

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "retries": 3,
    })

    def serialize(v: dict) -> bytes:
        return json.dumps(v).encode("utf-8")

    seen_profiles: set[str] = set()
    row_count = 0

    try:
        with open(DAILY_CSV_PATH, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                user_id = row.get("id", "")
                if not user_id:
                    continue

                events = build_events(row)

                for topic in ("wearable.sleep", "wearable.hrv_summary",
                              "wearable.breathing_summary", "wearable.vitals_daily"):
                    producer.produce(topic, key=user_id, value=serialize(events[topic]))

                if user_id not in seen_profiles:
                    producer.produce("wearable.profile", key=user_id,
                                     value=serialize(events["wearable.profile"]))
                    seen_profiles.add(user_id)

                row_count += 1
                if row_count % 200 == 0:
                    producer.flush()
                    print(f"[daily-producer] Sent {row_count} rows  (users seen: {len(seen_profiles)})")

                producer.poll(0)
                time.sleep(DELAY)

    except KeyboardInterrupt:
        print("\n[daily-producer] Interrupted.")
    finally:
        producer.flush()
        print(f"[daily-producer] Done. Total rows sent: {row_count}")


if __name__ == "__main__":
    main()
