"""
Wearable data streaming simulator.

Reads hourly_fitbit_sema_df_unprocessed.csv and publishes events to four Kafka topics:
  wearable_vitals   – bpm, temperature, scl_avg
  wearable_activity – calories, distance, steps, activity type, zone minutes
  wearable_context  – mood labels, location labels, mindfulness
  wearable_profile  – demographic / goal fields (sent once per unique user)

Run:
    python producer.py                         # default settings
    KAFKA_BOOTSTRAP=localhost:9092 DELAY=0.05 CSV_PATH=../hourly_fitbit_sema_df_unprocessed.csv python producer.py
"""

import csv
import json
import os
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CSV_PATH = os.getenv("CSV_PATH", "../hourly_fitbit_sema_df_unprocessed.csv")
# Seconds to sleep between rows (all topic messages for one row are sent together)
DELAY = float(os.getenv("DELAY", "0.1"))

TOPICS = [
    "wearable_vitals",
    "wearable_activity",
    "wearable_context",
    "wearable_profile",
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
                pass  # expected on reruns
            else:
                raise


# ── Event builder ─────────────────────────────────────────────────────────────
def _val(row: dict, key: str):
    """Return the value or None for empty strings."""
    v = row.get(key, "")
    return v if v != "" else None


def _num(row: dict, key: str):
    """Return a float for numeric fields, or None if missing/unparseable."""
    v = row.get(key, "")
    if v == "" or v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def build_events(row: dict) -> dict[str, dict]:
    user_id = row["id"]
    event_date = row.get("date", "")
    raw_hour = row.get("hour", "0")
    try:
        hour_int = int(float(raw_hour))
    except ValueError:
        hour_int = 0

    base = {
        "user_id": user_id,
        "event_date": event_date,
        "event_hour": raw_hour,
        "event_timestamp": f"{event_date}T{hour_int:02d}:00:00",
        "source": "fitbit_csv",
    }

    vitals = {
        **base,
        "event_type": "vitals",
        "payload": {
            "bpm": _num(row, "bpm"),
            "temperature": _num(row, "temperature"),
            "scl_avg": _num(row, "scl_avg"),
        },
    }

    activity = {
        **base,
        "event_type": "activity",
        "payload": {
            "calories": _num(row, "calories"),
            "distance": _num(row, "distance"),
            "steps": _num(row, "steps"),
            "activityType": _val(row, "activityType"),
            "minutes_in_default_zone_1": _num(row, "minutes_in_default_zone_1"),
            "minutes_in_default_zone_2": _num(row, "minutes_in_default_zone_2"),
            "minutes_in_default_zone_3": _num(row, "minutes_in_default_zone_3"),
            "minutes_below_default_zone_1": _num(row, "minutes_below_default_zone_1"),
        },
    }

    context = {
        **base,
        "event_type": "context",
        "payload": {
            "mindfulness_session": _val(row, "mindfulness_session"),  # "True"/"False" string → BooleanType in Silver
            "ALERT": _num(row, "ALERT"),
            "HAPPY": _num(row, "HAPPY"),
            "NEUTRAL": _num(row, "NEUTRAL"),
            "RESTED_RELAXED": _num(row, "RESTED/RELAXED"),
            "SAD": _num(row, "SAD"),
            "TENSE_ANXIOUS": _num(row, "TENSE/ANXIOUS"),
            "TIRED": _num(row, "TIRED"),
            "ENTERTAINMENT": _num(row, "ENTERTAINMENT"),
            "GYM": _num(row, "GYM"),
            "HOME": _num(row, "HOME"),
            "HOME_OFFICE": _num(row, "HOME_OFFICE"),
            "OTHER": _num(row, "OTHER"),
            "OUTDOORS": _num(row, "OUTDOORS"),
            "TRANSIT": _num(row, "TRANSIT"),
            "WORK_SCHOOL": _num(row, "WORK/SCHOOL"),
        },
    }

    profile = {
        **base,
        "event_type": "profile",
        "payload": {
            "badgeType": _val(row, "badgeType"),
            "age": _val(row, "age"),
            "gender": _val(row, "gender"),
            "bmi": _val(row, "bmi"),
            "step_goal": _val(row, "step_goal"),
            "min_goal": _val(row, "min_goal"),
            "max_goal": _val(row, "max_goal"),
            "step_goal_label": _val(row, "step_goal_label"),
        },
    }

    return {
        "wearable_vitals": vitals,
        "wearable_activity": activity,
        "wearable_context": context,
        "wearable_profile": profile,
    }


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    print(f"[producer] Kafka: {KAFKA_BOOTSTRAP}  CSV: {CSV_PATH}  delay: {DELAY}s/row")

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
        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                user_id = row.get("id", "")
                if not user_id:
                    continue

                events = build_events(row)

                # Always publish vitals, activity, context
                for topic in ("wearable_vitals", "wearable_activity", "wearable_context"):
                    producer.produce(topic, key=user_id, value=serialize(events[topic]))

                # Publish profile only once per user to avoid redundant demographic spam
                if user_id not in seen_profiles:
                    producer.produce("wearable_profile", key=user_id, value=serialize(events["wearable_profile"]))
                    seen_profiles.add(user_id)

                row_count += 1
                if row_count % 500 == 0:
                    producer.flush()
                    print(f"[producer] Sent {row_count} rows  (users seen: {len(seen_profiles)})")

                # Poll to serve delivery callbacks and avoid buffer buildup
                producer.poll(0)
                time.sleep(DELAY)

    except KeyboardInterrupt:
        print("\n[producer] Interrupted by user.")
    finally:
        producer.flush()
        print(f"[producer] Done. Total rows sent: {row_count}")


if __name__ == "__main__":
    main()
