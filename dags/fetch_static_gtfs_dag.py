import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from ingestion.fetch_static_gtfs import fetch_static_gtfs
from utils.config import get_config


def fetch_static_wrapper(**kwargs):
    """
    Load environment and config at runtime, then call fetch_static_gtfs.
    """

    # get configs
    cfg = get_config()
    url = cfg["api"]["static_url"]
    output_path = cfg["paths"]["raw"]["static_gtfs"]

    print(f"static url: {url}")
    print(f"output_path: {output_path}")

    return fetch_static_gtfs(
        agency="mbta",
        url=url,
        output_path=output_path,
    )


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
        python_callable=fetch_static_wrapper,
    )

    trigger_ingest = TriggerDagRunOperator(
        task_id="trigger_ingest_static_gtfs",
        trigger_dag_id="ingest_static_gtfs",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    fetch_task >> trigger_ingest
