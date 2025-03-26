#!/bin/bash
set -e

echo "Install Python dependencies"
pip install --no-cache-dir -r /requirements.txt

echo "Initialize Airflow"
airflow db init

echo "Set up Airflow admin user"
# ref: https://airflow.apache.org/docs/apache-airflow/2.5.2/administration-and-deployment/security/webserver.html
airflow users create \
  --username admin \
  --password admin \
  --firstname first \
  --lastname last \
  --role Admin \
  --email admin@admin.com

echo "Start Airflow scheduler and webserver..."
airflow scheduler & airflow webserver

echo "Airflow setup completed"
