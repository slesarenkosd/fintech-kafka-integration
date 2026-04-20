#!/bin/bash

echo "Creating Kafka topics..."

docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic transactions.raw --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic transactions.enriched --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic fraud.alerts --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic notifications --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic ledger.events --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic crm.updates --partitions 1 --replication-factor 1

echo "Done. Existing topics:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
