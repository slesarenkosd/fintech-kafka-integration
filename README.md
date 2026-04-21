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

```text
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
```

---

## 🚀 Запуск проекта

### 1. Запуск Kafka и Zookeeper

```bash
docker compose up -d
```
### 2. Создание топиков
```bash
chmod +x scripts/create_topics.sh
./scripts/create_topics.sh
```
### 3. Проверка топиков
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```
### 4. Генератор транзакций (Producer)
```bash
python3 producers/transaction_producer.py
```
Отправляет случайные транзакции в топик transactions.raw.
### 5. Антифрод-сервис
```bash
python3 consumers/fraud_detector.py
```
Читает транзакции из `transactions.raw` и применяет правила:

- сумма > 150000 руб.
- тип `transfer` и сумма > 50000 руб.
- более 5 транзакций от одного пользователя за 30 секунд

Результат:

- безопасные → `transactions.enriched`
- подозрительные → `fraud.alerts`
- уведомления → `notifications`

### 6. Роутер сообщений
```bash
python3 consumers/router.py
```
Маршрутизация:

- purchase → `crm.updates`
- transfer → `ledger.events`
- payment → `ledger.events` и `notifications`
### 7. Сервис уведомлений
```bash
python3 consumers/notification_consumer.py
```
Выводит:
- успешные операции
- предупреждения о подозрительных транзакциях
### 8. Учётная система (Ledger)
```bash
python3 consumers/ledger_consumer.py
```
Вывод:
```bash
[LEDGER] ...
```
### 9. CRM-сервис
```bash
python3 consumers/crm_consumer.py
```
Агрегирует покупки за 5 минут.
Если сумма > 200000:
```bash
[CRM] User upgraded!
```
### 10. Поток данных

```text
transactions.raw
      ↓
Fraud Detector
      ↓
transactions.enriched / fraud.alerts
      ↓
Router
      ↓
crm.updates / ledger.events / notifications
      ↓
CRM / Ledger / Notifications
```
### 11. Примечание
Все сервисы работают в режиме потоковой обработки.

Остановка:
```bash
Ctrl + C
``` 