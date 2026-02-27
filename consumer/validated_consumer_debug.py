import json
import os
from typing import Any, Dict, Tuple, Optional

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

REQUIRED_FIELDS = ["car_id", "timestamp", "speed", "rpm", "engine_temp", "fuel_level", "latitude", "longitude"]


def validate_event(e: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    for f in REQUIRED_FIELDS:
        if f not in e:
            return False, f"missing_field:{f}"

    try:
        speed = float(e["speed"])
        rpm = int(e["rpm"])
        temp = float(e["engine_temp"])
        fuel = float(e["fuel_level"])
        lat = float(e["latitude"])
        lon = float(e["longitude"])
    except Exception:
        return False, "invalid:number_cast"

    if not (0 <= speed <= 260): return False, "range:speed"
    if not (0 <= rpm <= 9000): return False, "range:rpm"
    if not (40 <= temp <= 150): return False, "range:engine_temp"
    if not (0 <= fuel <= 100): return False, "range:fuel_level"
    if not (-90 <= lat <= 90): return False, "range:latitude"
    if not (-180 <= lon <= 180): return False, "range:longitude"

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
        group_id="telemetry-consumer-validated-debug",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=7000,  # exit if no messages for 7s
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
    printed = 0

    print(f"🧪 Debug validate from {topic_in} (will auto-exit if idle)")

    for msg in consumer:
        event = msg.value
        is_ok, err = validate_event(event)

        if is_ok:
            ok_count += 1
            if printed < 5:
                printed += 1
                print(f"OK#{printed}: car={event.get('car_id')} speed={event.get('speed')} rpm={event.get('rpm')}")
        else:
            bad_count += 1
            producer.send(topic_dlq, {
                "error": err,
                "source_topic": topic_in,
                "kafka_partition": msg.partition,
                "kafka_offset": msg.offset,
                "event": event,
            })

    producer.flush()
    producer.close()
    consumer.close()
    print(f"Summary: ok={ok_count}, bad={bad_count}")


if __name__ == "__main__":
    main()