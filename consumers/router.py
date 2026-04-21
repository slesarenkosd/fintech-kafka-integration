import json

from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"

ENRICHED_TOPIC = "transactions.enriched"
LEDGER_TOPIC = "ledger.events"
CRM_TOPIC = "crm.updates"
NOTIFICATIONS_TOPIC = "notifications"

consumer = KafkaConsumer(
    ENRICHED_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="router-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Router is running...")

for message in consumer:
    transaction = message.value
    tx_type = transaction["type"]

    if tx_type == "purchase":
        producer.send(CRM_TOPIC, value=transaction)
        producer.flush()
        print("[ROUTER] purchase -> crm.updates", transaction)

    elif tx_type == "transfer":
        producer.send(LEDGER_TOPIC, value=transaction)
        producer.flush()
        print("[ROUTER] transfer -> ledger.events", transaction)

    elif tx_type == "payment":
        producer.send(LEDGER_TOPIC, value=transaction)

        notification_message = {
            "user_id": transaction["user_id"],
            "transaction_id": transaction["transaction_id"],
            "message": f"Your transaction {transaction['transaction_id']} is completed."
        }

        producer.send(NOTIFICATIONS_TOPIC, value=notification_message)
        producer.flush()
        print("[ROUTER] payment -> ledger.events + notifications", transaction)
        