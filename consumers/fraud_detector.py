import json
from collections import defaultdict, deque
from datetime import datetime, timedelta

from kafka import KafkaConsumer, KafkaProducer

print("FILE STARTED")

BOOTSTRAP_SERVERS = "localhost:9092"

RAW_TOPIC = "transactions.raw"
ENRICHED_TOPIC = "transactions.enriched"
FRAUD_ALERTS_TOPIC = "fraud.alerts"
NOTIFICATIONS_TOPIC = "notifications"

consumer = KafkaConsumer(
    RAW_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="fraud-detector-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

user_transactions = defaultdict(deque)

WINDOW_SECONDS = 30
MAX_TX_PER_WINDOW = 5


def parse_timestamp(ts):
    return datetime.fromisoformat(ts)


def cleanup_old_transactions(user_id, current_time):
    window_start = current_time - timedelta(seconds=WINDOW_SECONDS)
    dq = user_transactions[user_id]

    while dq and dq[0] < window_start:
        dq.popleft()


def detect_fraud(transaction):
    amount = transaction["amount"]
    tx_type = transaction["type"]
    user_id = transaction["user_id"]
    tx_time = parse_timestamp(transaction["timestamp"])

    if amount > 150000:
        return True, "Amount exceeds 150000 RUB"

    if tx_type == "transfer" and amount > 50000:
        return True, "Transfer exceeds 50000 RUB"

    cleanup_old_transactions(user_id, tx_time)
    user_transactions[user_id].append(tx_time)

    if len(user_transactions[user_id]) > MAX_TX_PER_WINDOW:
        return True, "More than 5 transactions in 30 seconds"

    return False, None


print("Fraud detector is running...")

for message in consumer:
    transaction = message.value

    is_fraud, reason = detect_fraud(transaction)

    if is_fraud:
        fraud_message = {
            **transaction,
            "fraud_status": "suspicious",
            "fraud_reason": reason
        }

        producer.send(FRAUD_ALERTS_TOPIC, value=fraud_message)

        notification_message = {
            "user_id": transaction["user_id"],
            "transaction_id": transaction["transaction_id"],
            "fraud_reason": reason,
            "message": f"Suspicious transaction detected for user {transaction['user_id']}"
        }

        producer.send(NOTIFICATIONS_TOPIC, value=notification_message)
        producer.flush()

        print("[FRAUD ALERT]", fraud_message)

    else:
        enriched_message = {
            **transaction,
            "fraud_status": "clean"
        }

        producer.send(ENRICHED_TOPIC, value=enriched_message)
        producer.flush()

        print("[CLEAN]", enriched_message)