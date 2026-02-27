import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer


def main() -> None:
    load_dotenv("config/.env")

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic = os.getenv("KAFKA_TOPIC_TELEMETRY", "car.telemetry.v1")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    event = {
        "car_id": "CAR_001",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "speed": 72.5,
        "rpm": 3100,
        "engine_temp": 91.2,
        "fuel_level": 62.0,
        "latitude": 52.5200,
        "longitude": 13.4050,
    }

    future = producer.send(topic, event)
    record_metadata = future.get(timeout=10)

    producer.flush()
    producer.close()

    print("✅ Sent 1 event")
    print(f"Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")


if __name__ == "__main__":
    main()