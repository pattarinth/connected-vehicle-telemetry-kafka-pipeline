import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Dict, Any

from dotenv import load_dotenv
from kafka import KafkaProducer


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def make_initial_state(car_id: str) -> Dict[str, Any]:
    # Start each car around Berlin-ish (just for demo)
    lat = 52.52 + random.uniform(-0.05, 0.05)
    lon = 13.405 + random.uniform(-0.08, 0.08)

    return {
        "car_id": car_id,
        "speed": random.uniform(0, 60),          # km/h
        "rpm": random.uniform(800, 2500),
        "engine_temp": random.uniform(70, 95),   # °C
        "fuel_level": random.uniform(30, 90),    # %
        "latitude": lat,
        "longitude": lon,
    }


def step_state(s: Dict[str, Any]) -> Dict[str, Any]:
    # Simulate driving behavior with small random walk
    accel = random.uniform(-6, 8)  # km/h change per tick
    s["speed"] = clamp(s["speed"] + accel, 0, 180)

    # RPM follows speed loosely
    target_rpm = 800 + (s["speed"] * 35)
    s["rpm"] = clamp(s["rpm"] + (target_rpm - s["rpm"]) * random.uniform(0.15, 0.35), 700, 6500)

    # Engine temp rises with rpm, cools when slow
    temp_delta = (s["rpm"] / 6500) * random.uniform(0.2, 0.8) - (1 if s["speed"] < 5 else 0) * random.uniform(0.1, 0.5)
    s["engine_temp"] = clamp(s["engine_temp"] + temp_delta, 60, 120)

    # Fuel burns slowly (super small so it doesn't hit 0 quickly)
    s["fuel_level"] = clamp(s["fuel_level"] - random.uniform(0.005, 0.03), 0, 100)

    # Move slightly based on speed (very rough)
    drift = s["speed"] / 180 * 0.0008
    s["latitude"] += random.uniform(-drift, drift)
    s["longitude"] += random.uniform(-drift, drift)

    # Occasionally inject anomalies (for demo / monitoring)
    if random.random() < 0.05:  # 5% chance
        anomaly_type = random.choice(["overspeed", "overheat", "low_fuel"])

        if anomaly_type == "overspeed":
            s["speed"] = random.uniform(190, 220)

        elif anomaly_type == "overheat":
            s["engine_temp"] = random.uniform(120, 140)

        elif anomaly_type == "low_fuel":
            s["fuel_level"] = random.uniform(0, 3)

    return s


def main() -> None:
    load_dotenv("config/.env")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic = os.getenv("KAFKA_TOPIC_TELEMETRY", "car.telemetry.v1")

    # Keep defaults lightweight for your laptop
    num_cars = int(os.getenv("SIM_NUM_CARS", "2"))
    interval_sec = float(os.getenv("SIM_INTERVAL_SEC", "1.0"))  # 1 event/sec per car
    max_events = int(os.getenv("SIM_MAX_EVENTS", "0"))  # 0 = run forever

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=10,
    )

    cars = [make_initial_state(f"CAR_{i:03d}") for i in range(1, num_cars + 1)]

    print(f"🚗 Starting simulator: cars={num_cars}, interval={interval_sec}s, max_events={max_events or '∞'}")
    sent = 0

    try:
        while True:
            for s in cars:
                step_state(s)
                event = {
                    "car_id": s["car_id"],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "speed": round(float(s["speed"]), 2),
                    "rpm": int(s["rpm"]),
                    "engine_temp": round(float(s["engine_temp"]), 2),
                    "fuel_level": round(float(s["fuel_level"]), 2),
                    "latitude": round(float(s["latitude"]), 6),
                    "longitude": round(float(s["longitude"]), 6),
                }

                producer.send(topic, event)
                sent += 1

                # ultra-light console output (every 50 events)
                if sent % 50 == 0:
                    print(f"✅ sent={sent} (latest: {event['car_id']} speed={event['speed']} km/h temp={event['engine_temp']}°C)")

                if max_events and sent >= max_events:
                    producer.flush()
                    print("🛑 Reached max events, stopping.")
                    return

            producer.flush()
            time.sleep(interval_sec)

    except KeyboardInterrupt:
        print("\n🧯 Stopped by user (Ctrl+C).")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()