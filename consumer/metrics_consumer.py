import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import MongoClient

load_dotenv("config/.env")

bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
topic_metrics = os.getenv("KAFKA_TOPIC_METRICS", "car.telemetry.metrics.v1")

mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")

mongo = MongoClient(mongo_uri)
db = mongo["telemetry"]
metrics_coll = db["pipeline_metrics"]

consumer = KafkaConsumer(
    topic_metrics,
    bootstrap_servers=bootstrap,
    auto_offset_reset="earliest",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

print("📊 Metrics consumer running...")

for msg in consumer:
    event = msg.value
    print("METRIC:", event)   
    metrics_coll.insert_one(event)