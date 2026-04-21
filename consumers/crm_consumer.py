import json
from collections import defaultdict, deque
from datetime import datetime, timedelta

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
CRM_TOPIC = "crm.updates"

consumer = KafkaConsumer(
    CRM_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="crm-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("CRM consumer is running...")

# хранение транзакций
user_purchases = defaultdict(deque)

WINDOW_SECONDS = 300  # 5 минут
THRESHOLD = 200000


def parse_timestamp(ts):
    return datetime.fromisoformat(ts)


def cleanup_old(user_id, current_time):
    window_start = current_time - timedelta(seconds=WINDOW_SECONDS)
    dq = user_purchases[user_id]

    while dq and dq[0][0] < window_start:
        dq.popleft()


for message in consumer:
    transaction = message.value

    user_id = transaction["user_id"]
    amount = transaction["amount"]
    tx_time = parse_timestamp(transaction["timestamp"])

    # чистим старые
    cleanup_old(user_id, tx_time)

    # добавляем новую
    user_purchases[user_id].append((tx_time, amount))

    # считаем сумму
    total = sum(a for _, a in user_purchases[user_id])

    print(f"[CRM] User {user_id} total in last 5 min: {total}")

    if total > THRESHOLD:
        print(f"[CRM] User {user_id} upgraded! Total: {total}")
        
