import json
import threading
from typing import Dict

from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"

ENRICHED_TOPIC = "transactions.enriched"
REQUESTS_TOPIC = "transaction.status.requests"
REPLIES_TOPIC = "transaction.status.replies"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Здесь храним известные статусы транзакций
transaction_store: Dict[str, dict] = {}


def consume_enriched_transactions():
    consumer = KafkaConsumer(
        ENRICHED_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="status-enriched-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    print("Status service: listening to transactions.enriched...")

    for message in consumer:
        transaction = message.value
        transaction_id = transaction["transaction_id"]

        transaction_store[transaction_id] = {
            "transaction_id": transaction_id,
            "user_id": transaction.get("user_id"),
            "status": transaction.get("fraud_status", "unknown"),
            "type": transaction.get("type"),
            "amount": transaction.get("amount")
        }

        print(f"[STATUS STORE UPDATED] {transaction_id} -> {transaction_store[transaction_id]}")


def consume_status_requests():
    consumer = KafkaConsumer(
        REQUESTS_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="status-requests-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    print("Status service: listening to transaction.status.requests...")

    for message in consumer:
        request_data = message.value
        transaction_id = request_data["transaction_id"]
        correlation_id = request_data["correlation_id"]

        if transaction_id in transaction_store:
            response = {
                "correlation_id": correlation_id,
                "transaction_id": transaction_id,
                "found": True,
                "data": transaction_store[transaction_id]
            }
        else:
            response = {
                "correlation_id": correlation_id,
                "transaction_id": transaction_id,
                "found": False,
                "data": {
                    "transaction_id": transaction_id,
                    "status": "not_found"
                }
            }

        producer.send(REPLIES_TOPIC, value=response)
        producer.flush()

        print(f"[STATUS REPLY SENT] {response}")


if __name__ == "__main__":
    t1 = threading.Thread(target=consume_enriched_transactions, daemon=True)
    t2 = threading.Thread(target=consume_status_requests, daemon=True)

    t1.start()
    t2.start()

    print("Status service is running...")

    t1.join()
    t2.join()
    