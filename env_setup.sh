#!/bin/bash
set -e

echo "Build Docker images"
docker compose build

echo "Start containers"
docker compose up -d airflow python-dev

echo "Setup completed"
echo "Open Airflow UI at: localhost:8080 (admin / admin)"
