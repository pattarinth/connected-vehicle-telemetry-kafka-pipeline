import json
import os
from typing import Any, Dict, Optional, Tuple
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient


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
    for f in REQUIRED_FIELDS:
        if f not in e:
            return False, f"missing_field:{f}"

    if not isinstance(e["car_id"], str) or not e["car_id"]:
        return False, "invalid:car_id"

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


def detect_anomaly(e: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    speed = float(e["speed"])
    temp = float(e["engine_temp"])
    fuel = float(e["fuel_level"])
    rpm = int(e["rpm"])

    reasons = []
    if speed > 180:
        reasons.append("overspeed")
    if temp >= 120:
        reasons.append("engine_overheat")
    if fuel <= 5:
        reasons.append("low_fuel")
    if rpm >= 7000:
        reasons.append("high_rpm")

    if not reasons:
        return None

    return {
        "car_id": e["car_id"],
        "timestamp": e["timestamp"],
        "reasons": reasons,
        "telemetry": e,
    }


def emit_metric(
    producer: KafkaProducer,
    topic: str,
    metric: str,
    value: int = 1,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    payload: Dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "metric": metric,
        "value": value,
    }

    if extra:
        payload.update(extra)

    producer.send(topic, payload)


def main() -> None:
    load_dotenv("config/.env")

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic_in = os.getenv("KAFKA_TOPIC_TELEMETRY", "car.telemetry.v1")
    topic_dlq = os.getenv("KAFKA_TOPIC_DLQ", "car.telemetry.dlq.v1")
    topic_anomaly = os.getenv("KAFKA_TOPIC_ANOMALY", "car.telemetry.anomaly.v1")
    topic_metrics = os.getenv("KAFKA_TOPIC_METRICS", "car.telemetry.metrics.v1")

    mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    mongo_db = os.getenv("MONGODB_DB", "telemetry")
    mongo_collection = os.getenv("MONGODB_COLLECTION", "car_telemetry")

    consumer = KafkaConsumer(
        topic_in,
        bootstrap_servers=bootstrap,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="telemetry-processor-mongo-dlq",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=10,
    )

    mongo = MongoClient(mongo_uri)
    coll = mongo[mongo_db][mongo_collection]

    ok = 0
    bad = 0
    inserted = 0
    anomaly_count = 0

    print(
        f"🧠 Processor running: {topic_in} → MongoDB({mongo_db}.{mongo_collection}) "
        f"| DLQ={topic_dlq} | ANOMALY={topic_anomaly} | METRICS={topic_metrics}"
    )
    print("   Start the simulator in another terminal. Ctrl+C to stop.\n")

    try:
        for msg in consumer:
            event = msg.value

            # emit processed metric
            emit_metric(producer, topic_metrics, "telemetry.processed", 1)

            is_ok, err = validate_event(event)

            if is_ok:
                ok += 1

                emit_metric(
                    producer,
                    topic_metrics,
                    "telemetry.ok",
                    1,
                    {"car_id": event["car_id"]},
                )

                # store telemetry
                coll.insert_one(dict(event))
                inserted += 1

                anomaly = detect_anomaly(event)

                if anomaly is not None:
                    emit_metric(
                        producer,
                        topic_metrics,
                        "telemetry.anomaly",
                        1,
                        {"car_id": event["car_id"], "reasons": anomaly["reasons"]},
                    )

                    producer.send(topic_anomaly, anomaly)
                    anomaly_count += 1

                if inserted % 50 == 0:
                    print(
                        f"✅ inserted={inserted} ok={ok} bad={bad} anomalies_sent={anomaly_count}"
                    )

            else:
                bad += 1

                emit_metric(
                    producer,
                    topic_metrics,
                    "telemetry.bad",
                    1,
                    {"error": err},
                )

                producer.send(
                    topic_dlq,
                    {
                        "error": err,
                        "source_topic": topic_in,
                        "kafka_partition": msg.partition,
                        "kafka_offset": msg.offset,
                        "event": event,
                    },
                )

                if bad % 10 == 0:
                    print(f"⚠️ bad={bad} last_error={err}")

    except KeyboardInterrupt:
        print("\n🧯 Stopped by user.")

    finally:
        producer.flush()
        producer.close()
        consumer.close()
        mongo.close()

        print(
            f"Summary: inserted={inserted}, ok={ok}, bad={bad}, anomalies_sent={anomaly_count}"
        )


if __name__ == "__main__":
    main()

