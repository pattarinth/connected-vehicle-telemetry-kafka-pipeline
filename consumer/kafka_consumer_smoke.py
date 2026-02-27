import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer


def main() -> None:
    load_dotenv("config/.env")

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic = os.getenv("KAFKA_TOPIC_TELEMETRY", "car.telemetry.v1")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",     # start from beginning for first test
        enable_auto_commit=True,
        group_id="telemetry-consumer-smoke",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    print(f"👂 Listening on {topic} @ {bootstrap} ... (Ctrl+C to stop)")
    count = 0

    try:
        for msg in consumer:
            event = msg.value
            count += 1

            # print only every 25 messages to keep it quiet
            if count % 25 == 0:
                print(f"#{count} car={event.get('car_id')} speed={event.get('speed')} rpm={event.get('rpm')} temp={event.get('engine_temp')}")

    except KeyboardInterrupt:
        print("\n🧯 Stopped by user.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()