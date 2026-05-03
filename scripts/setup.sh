#!/usr/bin/env bash
set -e

echo "Setting up CryptoStream environment..."
if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env from .env.example. Please update the variables with your actual keys."
fi

if [ ! -f ingestor/.env ]; then
  cp ingestor/.env.example ingestor/.env
  echo "Created ingestor/.env. Please update it."
fi

mkdir -p airflow/dags airflow/plugins airflow/logs dbt_transform/logs ingestor/logs grafana/provisioning prometheus
echo "Setup complete. Run 'docker compose up -d' to start the pipeline."
