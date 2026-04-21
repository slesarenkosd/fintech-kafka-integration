# FinTech Kafka Integration

Учебный проект по интеграции систем с использованием Apache Kafka.

## 📌 Описание

В рамках задания реализован прототип обработки финансовых транзакций с использованием потоковой архитектуры и паттернов интеграции:

- Publish–Subscribe  
- Content-Based Router  
- Message Filter  

Система имитирует работу финансовой платформы с антифрод-проверкой, маршрутизацией и обработкой событий.

---

## 🛠️ Технологии

- Python 3.9+
- Apache Kafka
- Zookeeper
- Docker / Docker Compose
- kafka-python

---

## 📁 Структура проекта

fintech-kafka-integration/
│
├── consumers/
│   ├── fraud_detector.py
│   ├── router.py
│   ├── notification_consumer.py
│   ├── ledger_consumer.py
│   └── crm_consumer.py
│
├── producers/
│   └── transaction_producer.py
│
├── scripts/
│   └── create_topics.sh
│
├── docker-compose.yml
├── README.md
└── .gitignore

---

## 🚀 Запуск проекта

### 1. Запуск Kafka и Zookeeper

```bash
docker compose up -d
