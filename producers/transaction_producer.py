import json
import time
import random
import uuid
from datetime import datetime

from kafka import KafkaProducer

# создаём producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

transaction_types = ["purchase", "transfer", "payment"]
locations = ["Moscow", "Saint Petersburg", "Kazan", "Novosibirsk"]

def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 10)}",
        "amount": round(random.uniform(10, 500000), 2),
        "currency": "RUB",
        "type": random.choice(transaction_types),
        "timestamp": datetime.utcnow().isoformat(),
        "merchant_id": f"merch_{random.randint(1, 5)}",
        "location": random.choice(locations)
    }

print("Starting producer...")

while True:
    transaction = generate_transaction()
    print("Sending:", transaction)

    producer.send("transactions.raw", value=transaction)
    producer.flush()

    time.sleep(random.uniform(0.2, 1))
    