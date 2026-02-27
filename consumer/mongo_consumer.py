import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import MongoClient


def main():
    load_dotenv("config/.env")

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    topic = os.getenv("KAFKA_TOPIC_TELEMETRY")

    mongo_uri = os.getenv("MONGODB_URI")
    mongo_db = os.getenv("MONGODB_DB")
    mongo_collection = os.getenv("MONGODB_COLLECTION")

    # Kafka consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="mongo-consumer",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    # MongoDB connection
    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    collection = db[mongo_collection]

    print("📦 Writing telemetry events into MongoDB...")

    count = 0

    for msg in consumer:
        event = msg.value

        collection.insert_one(event)

        count += 1

        if count % 50 == 0:
            print(f"Inserted {count} events into MongoDB")


if __name__ == "__main__":
    main()