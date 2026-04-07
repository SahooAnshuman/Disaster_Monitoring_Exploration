from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
from datetime import datetime, timedelta
import pytz

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ─── Reference Data ───────────────────────────────────────────────
stations      = ["DS100", "DS200", "DS300", "DS400", "DS500"]
regions       = ["COASTAL", "HILLSIDE", "FOREST", "URBAN", "RIVERBANK", "DESERT"]
disaster_types = ["EARTHQUAKE", "FLOOD", "WILDFIRE", "TSUNAMI", "LANDSLIDE", "CYCLONE", "VOLCANO"]

# Threat Index Scale:
# 0–20   → Level 1 → LOW
# 21–40  → Level 2 → MODERATE
# 41–60  → Level 3 → HIGH
# 61–80  → Level 4 → SEVERE
# 81–100 → Level 5 → CATASTROPHIC

def threat_to_alert_level(threat_index: int) -> int:
    """Map threat index to alert level 1–5."""
    if threat_index <= 20: return 1
    if threat_index <= 40: return 2
    if threat_index <= 60: return 3
    if threat_index <= 80: return 4
    return 5

sensor_cache = []  # stores seen sensor IDs for duplicate-event generation


# ─── Clean Event ──────────────────────────────────────────────────
def generate_clean_event():
    sid = fake.uuid4()
    sensor_cache.append(sid)
    threat_index = random.randint(0, 100)
    return {
        "sensor_id":     sid,
        "station_id":    random.choice(stations),
        "region":        random.choice(regions),
        "threat_index":  threat_index,
        "alert_level":   threat_to_alert_level(threat_index),
        "disaster_type": random.choice(disaster_types),
        "event_time":    datetime.now(pytz.utc).isoformat()
    }


# ─── Dirty Event ──────────────────────────────────────────────────
def generate_dirty_event():
    dirty_type = random.choice([
        "null_threat",
        "negative_threat",
        "extreme_threat",
        "duplicate_sensor",
        "late_reading",
        "future_reading",
        "threat_as_string",
        "schema_drift",
        "corrupt_json"
    ])

    base = generate_clean_event()

    if dirty_type == "null_threat":
        # Sensor lost signal — no reading available
        base["threat_index"] = None

    elif dirty_type == "negative_threat":
        # Physically impossible negative threat index — sensor malfunction
        base["threat_index"] = random.randint(-50, -1)

    elif dirty_type == "extreme_threat":
        # Way beyond scale (max 100) — calibration failure
        base["threat_index"] = random.randint(200, 999)

    elif dirty_type == "duplicate_sensor" and sensor_cache:
        # Same sensor reporting twice — deduplication needed
        base["sensor_id"] = random.choice(sensor_cache)

    elif dirty_type == "late_reading":
        # Reading arrived too late — critical for disaster systems!
        base["event_time"] = (
            datetime.now(pytz.utc) - timedelta(minutes=random.randint(10, 120))
        ).isoformat()

    elif dirty_type == "future_reading":
        # Timestamp ahead of now — clock skew on sensor
        base["event_time"] = (
            datetime.now(pytz.utc) + timedelta(minutes=random.randint(5, 60))
        ).isoformat()

    elif dirty_type == "threat_as_string":
        # Threat index sent as label instead of number — wrong data type
        base["threat_index"] = random.choice(["LOW", "HIGH", "CRITICAL", "UNKNOWN"])

    elif dirty_type == "schema_drift":
        # Extra unexpected field — new sensor firmware added aftershock data
        base["aftershock_probability"] = round(random.uniform(0.0, 1.0), 2)

    elif dirty_type == "corrupt_json":
        # Completely unreadable payload — transmission failure
        return "###CORRUPTED_ALERT###"

    return base


# ─── Main Loop ────────────────────────────────────────────────────
print(" Disaster Alert Producer started — streaming to [disaster-alert-topic] ...")
print("-" * 60)

while True:
    if random.random() < 0.7:
        event = generate_clean_event()
        tag   = " CLEAN"
    else:
        event = generate_dirty_event()
        tag   = " DIRTY"

    if isinstance(event, str):
        producer.send("disaster-alert-topic", value={"raw": event})
        print(" CORRUPT ALERT SENT")
    else:
        producer.send("disaster-alert-topic", value=event)
        print(f"{tag} | {event}")

    time.sleep(random.uniform(0.5, 1.5))