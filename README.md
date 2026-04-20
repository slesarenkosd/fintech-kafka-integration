# FinTech Kafka Integration

Учебный проект по интеграции систем на базе Apache Kafka.

## Описание
В рамках задания реализуется прототип интеграционного решения для финансовой компании с использованием Apache Kafka.

## Используемые технологии
- Docker
- Docker Compose
- Apache Kafka
- Zookeeper
- Python 3.9+

## Структура проекта

```text
fintech-kafka-integration/
│
├── consumers/
├── producers/
├── models/
├── scripts/
│   └── create_topics.sh
├── docker-compose.yml
├── README.md
└── .gitignore

docker compose up -d
chmod +x scripts/create_topics.sh
./scripts/create_topics.sh

docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
