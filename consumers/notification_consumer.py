import json

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
NOTIFICATIONS_TOPIC = "notifications"

consumer = KafkaConsumer(
    NOTIFICATIONS_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="notification-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Notification consumer is running...")

for message in consumer:
    notification = message.value

    user_id = notification.get("user_id", "unknown_user")
    transaction_id = notification.get("transaction_id", "unknown_transaction")
    fraud_reason = notification.get("fraud_reason")

    if fraud_reason:
        print(
            f"[NOTIFICATION] User {user_id}: "
            f"Transaction {transaction_id} is blocked. Reason: {fraud_reason}"
        )
    else:
        print(
            f"[NOTIFICATION] User {user_id}: "
            f"Your transaction {transaction_id} is completed."
        )
        