import os
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, Query
from pymongo import MongoClient

app = FastAPI(title="Telemetry API", version="1.0")

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "telemetry")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "car_telemetry")

client: Optional[MongoClient] = None


@app.on_event("startup")
def startup():
    global client
    client = MongoClient(MONGODB_URI)


@app.on_event("shutdown")
def shutdown():
    global client
    if client:
        client.close()


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


def _coll():
    assert client is not None
    return client[MONGODB_DB][MONGODB_COLLECTION]

def _metrics_coll():
    assert client is not None
    return client[MONGODB_DB]["pipeline_metrics"]


@app.get("/series/speed")
def series_speed(
    car_id: str = Query(..., description="e.g. CAR_001"),
    limit: int = Query(300, ge=10, le=5000),
) -> List[Dict[str, Any]]:
    """
    Returns time-series points for Grafana:
    [{ "ts": "...ISO8601...", "value": <speed> }, ...]
    """
    docs = list(
        _coll()
        .find({"car_id": car_id}, {"_id": 0, "timestamp": 1, "speed": 1})
        .sort("timestamp", -1)
        .limit(limit)
    )
    docs.reverse()  # oldest -> newest
    return [{"ts": d["timestamp"], "value": float(d.get("speed", 0))} for d in docs]


@app.get("/latest")
def latest(
    car_id: str = Query(..., description="e.g. CAR_001"),
) -> Dict[str, Any]:
    doc = _coll().find_one({"car_id": car_id}, sort=[("timestamp", -1)], projection={"_id": 0})
    return doc or {}

@app.get("/series/anomalies")
def series_anomalies(
    car_id: str = Query(..., description="e.g. CAR_001"),
    limit: int = Query(500, ge=10, le=5000),
) -> List[Dict[str, Any]]:
    """
    Returns anomalies aggregated into 10-second buckets:
    [{ "ts": "YYYY-MM-DDTHH:MM:SS", "value": <count> }, ...]
    """
    pipeline = [
        {"$match": {"car_id": car_id, "engine_temp": {"$gte": 120}}},
        {
            "$group": {
                "_id": {
                    "$dateTrunc": {
                        "date": {"$toDate": "$timestamp"},
                        "unit": "second",
                        "binSize": 10,
                    }
                },
                "count": {"$sum": 1},
            }
        },
        {
            "$project": {
                "_id": 0,
                "ts": {
                    "$dateToString": {
                        "format": "%Y-%m-%dT%H:%M:%S",
                        "date": "$_id",
                    }
                },
                "value": "$count",
            }
        },
        {"$sort": {"ts": 1}},
        {"$limit": limit},
    ]

    return list(_coll().aggregate(pipeline))

@app.get("/fleet/latest")
def fleet_latest(limit: int = Query(50, ge=1, le=500)) -> List[Dict[str, Any]]:
    """
    Returns the latest telemetry point per car for fleet monitoring.
    Includes vehicle_status used for color-coded visualization.
    """

    pipeline = [
        {"$sort": {"timestamp": -1}},
        {
            "$group": {
                "_id": "$car_id",
                "car_id": {"$first": "$car_id"},
                "ts": {"$first": "$timestamp"},
                "latitude": {"$first": "$latitude"},
                "longitude": {"$first": "$longitude"},
                "speed": {"$first": "$speed"},
                "engine_temp": {"$first": "$engine_temp"},
                "fuel_level": {"$first": "$fuel_level"},
            }
        },
        {"$sort": {"car_id": 1}},
        {"$limit": limit},
        {"$project": {"_id": 0}},
    ]

    docs = list(_coll().aggregate(pipeline))

    # compute vehicle health status
    for d in docs:
        if d["engine_temp"] >= 120:
            d["vehicle_status"] = "overheating"
        elif d["fuel_level"] <= 10:
            d["vehicle_status"] = "low_fuel"
        else:
            d["vehicle_status"] = "normal"

    return docs

@app.get("/pipeline/health")
def pipeline_health() -> Dict[str, int]:
    c = _metrics_coll()

    processed = c.count_documents({"metric": "telemetry.processed"})
    ok = c.count_documents({"metric": "telemetry.ok"})
    anomalies = c.count_documents({"metric": "telemetry.anomaly"})
    bad = c.count_documents({"metric": "telemetry.bad"})

    return {
        "processed": processed,
        "ok": ok,
        "anomalies": anomalies,
        "dlq": bad,
    }