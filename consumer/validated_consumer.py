import json
import os
from typing import Any, Dict, Tuple, Optional

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer


REQUIRED_FIELDS = [
    "car_id",
    "timestamp",
    "speed",
    "rpm",
    "engine_temp",
    "fuel_level",
    "latitude",
    "longitude",
]


def validate_event(e: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    # Required fields present
    for f in REQUIRED_FIELDS:
        if f not in e:
            return False, f"missing_field:{f}"

    # Basic type/format checks
    if not isinstance(e["car_id"], str) or not e["car_id"]:
        return False, "invalid:car_id"

    # Numeric sanity ranges (simple, recruiter-friendly)
    try:
        speed = float(e["speed"])
        rpm = int(e["rpm"])
        temp = float(e["engine_temp"])
        fuel = float(e["fuel_level"])
        lat = float(e["latitude"])
        lon = float(e["longitude"])
    except Exception:
        return False, "invalid:number_cast"

    if not (0 <= speed <= 260):
        return False, "range:speed"
    if not (0 <= rpm <= 9000):
        return False, "range:rpm"
    if not (40 <= temp <= 150):
        return False, "range:engine_temp"
    if not (0 <= fuel <= 100):
        return False, "range:fuel_level"
    if not (-90 <= lat <= 90):
        return False, "range:latitude"
    if not (-180 <= lon <= 180):
        return False, "range:longitude"

    return True, None


def main() -> None:
    load_dotenv("config/.env")

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic_in = os.getenv("KAFKA_TOPIC_TELEMETRY", "car.telemetry.v1")
    topic_dlq = os.getenv("KAFKA_TOPIC_DLQ", "car.telemetry.dlq.v1")

    consumer = KafkaConsumer(
        topic_in,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="telemetry-consumer-validated",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=10,
    )

    ok_count = 0
    bad_count = 0

    print(f"🧪 Validating events from {topic_in} → DLQ: {topic_dlq} (Ctrl+C to stop)")

    try:
        for msg in consumer:
            event = msg.value
            is_ok, err = validate_event(event)

            if is_ok:
                ok_count += 1
                if ok_count % 50 == 0:
                    print(f"✅ ok={ok_count} bad={bad_count} (latest car={event.get('car_id')} speed={event.get('speed')})")
            else:
                bad_count += 1
                dlq_payload = {
                    "error": err,
                    "source_topic": topic_in,
                    "kafka_partition": msg.partition,
                    "kafka_offset": msg.offset,
                    "event": event,
                }
                producer.send(topic_dlq, dlq_payload)

                # keep console quiet; show every 10 bad events
                if bad_count % 10 == 0:
                    print(f"⚠️ bad={bad_count} last_error={err}")

    except KeyboardInterrupt:
        print("\n🧯 Stopped by user.")
    finally:
        producer.flush()
        producer.close()
        consumer.close()
        print(f"Summary: ok={ok_count}, bad={bad_count}")


if __name__ == "__main__":
    main()