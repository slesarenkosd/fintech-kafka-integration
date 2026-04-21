import json
import time
import uuid

from fastapi import FastAPI
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
REQUESTS_TOPIC = "transaction.status.requests"
REPLIES_TOPIC = "transaction.status.replies"

app = FastAPI(title="Transaction Status API")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


@app.get("/status/{transaction_id}")
def get_transaction_status(transaction_id: str):
    correlation_id = str(uuid.uuid4())

    request_message = {
        "transaction_id": transaction_id,
        "correlation_id": correlation_id
    }

    producer.send(REQUESTS_TOPIC, value=request_message)
    producer.flush()

    consumer = KafkaConsumer(
        REPLIES_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=None,
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    start_time = time.time()

    for message in consumer:
        reply = message.value

        if reply.get("correlation_id") == correlation_id:
            consumer.close()
            return reply

        if time.time() - start_time > 5:
            break

    consumer.close()

    return {
        "correlation_id": correlation_id,
        "transaction_id": transaction_id,
        "found": False,
        "data": {
            "status": "timeout"
        }
    }
