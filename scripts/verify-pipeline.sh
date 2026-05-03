#!/usr/bin/env bash
set -e

echo "Verifying Pipeline Execution..."
echo "1. Checking Kafka topics..."
docker exec cs-kafka kafka-topics --bootstrap-server localhost:9092 --list | grep "crypto.prices.raw" || echo "Topic not found!"

echo "2. Checking Neon Database (via Airflow run)..."
echo "You can check DB manually using psql and the NEON_DATABASE_URL."

echo "3. Pipeline is active if the DAG consume_and_load is running and dbt is triggered."
