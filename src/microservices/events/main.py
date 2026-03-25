from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer, KafkaConsumer
import json
import threading
from datetime import datetime
import os
import time

app = FastAPI()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

producer = None
consumer_started = False


def get_producer():
    global producer
    if producer is not None:
        return producer

    last_error = None
    for _ in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            return producer
        except Exception as e:
            last_error = e
            time.sleep(1)
    raise last_error


def warmup_producer():
    try:
        p = get_producer()
        for topic in ("movie-events", "user-events", "payment-events"):
            for _ in range(30):
                partitions = p.partitions_for(topic)
                if partitions:
                    break
                time.sleep(1)
    except Exception:
        pass

# --- Consumer ---
def start_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                "movie-events",
                "user-events",
                "payment-events",
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                group_id="events-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True
            )

            print("Kafka consumer started")

            for message in consumer:
                print(f"Consumed from {message.topic}: {message.value}")
        except Exception:
            time.sleep(1)

@app.on_event("startup")
def startup():
    global consumer_started
    # Warm up producer/topic metadata in background to avoid blocking app startup.
    threading.Thread(target=warmup_producer, daemon=True).start()

    if not consumer_started:
        threading.Thread(target=start_consumer, daemon=True).start()
        consumer_started = True


# --- helper ---
def build_event(event_type: str, payload: dict):
    return {
        "id": f"{event_type}-{payload.get('user_id', payload.get('movie_id', payload.get('payment_id', 'unknown')))}-{payload.get('action', 'event')}",
        "type": event_type,
        "timestamp": datetime.utcnow().isoformat(),
        "payload": payload
    }


def send_to_kafka(topic: str, event: dict):
    p = get_producer()
    future = p.send(topic, event)
    metadata = future.get(timeout=5)

    return {
        "status": "success",
        "partition": metadata.partition,
        "offset": metadata.offset,
        "event": event
    }


# --- health ---
@app.get("/api/events/health")
def health():
    return {"status": True}


# --- movie event ---
@app.post("/api/events/movie", status_code=201)
def create_movie_event(event: dict):
    try:
        kafka_event = build_event("movie", event)
        return send_to_kafka("movie-events", kafka_event)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- user event ---
@app.post("/api/events/user", status_code=201)
def create_user_event(event: dict):
    try:
        kafka_event = build_event("user", event)
        return send_to_kafka("user-events", kafka_event)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- payment event ---
@app.post("/api/events/payment", status_code=201)
def create_payment_event(event: dict):
    try:
        kafka_event = build_event("payment", event)
        return send_to_kafka("payment-events", kafka_event)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
