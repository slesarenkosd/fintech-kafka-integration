import json

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
LEDGER_TOPIC = "ledger.events"

consumer = KafkaConsumer(
    LEDGER_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="ledger-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Ledger consumer is running...")

for message in consumer:
    transaction = message.value
    print("[LEDGER]", transaction)
    