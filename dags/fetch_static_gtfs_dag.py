import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from ingestion.fetch_static_gtfs import fetch_static_gtfs
from utils.config import get_config

# get configs
cfg = get_config()
url = cfg["api"]["static_url"]
output_path = cfg["paths"]["raw"]["static_gtfs"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 20),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=1),
}

with DAG(
    dag_id="fetch_static_gtfs",
    default_args=default_args,
    schedule_interval="@once",  # run only once
    catchup=False,
    max_active_runs=1,
    tags=["static-gtfs", "mbta"],
    is_paused_upon_creation=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_static_gtfs_data",
        python_callable=fetch_static_gtfs,
        op_kwargs={
            "agency": "mbta",
            "url": url,
            "output_path": output_path,
        },
    )
