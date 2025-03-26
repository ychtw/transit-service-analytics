import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import DagRun
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

from ingestion.fetch_realtime_gtfs import fetch_realtime_gtfs

load_dotenv()


# TODO: limit num of runs for testing purpose, might remove later
def stop_after_n_runs(max_runs=3, **context):
    dag_id = context["dag"].dag_id
    dag_runs = DagRun.find(dag_id=dag_id)
    if len(dag_runs) >= max_runs:
        subprocess.run(["airflow", "dags", "pause", dag_id], check=True)
        raise Exception(f"Reached maximum runs ({max_runs}), pause DAG '{dag_id}'")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 20),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=1),
}

with DAG(
    dag_id="fetch_realtime_gtfs",
    default_args=default_args,
    schedule_interval="* * * * *",  # run every minute
    catchup=False,
    max_active_runs=1,
    tags=["realtime-gtfs", "mbta"],
    is_paused_upon_creation=False,
) as dag:

    # TODO: limit num of runs for testing purpose, might remove later
    check_limit = PythonOperator(
        task_id="check_run_count",
        python_callable=stop_after_n_runs,
        op_kwargs={"max_runs": 60},
    )

    fetch_task = PythonOperator(
        task_id="fetch_realtime_gtfs_data",
        python_callable=fetch_realtime_gtfs,
        op_kwargs={
            "agency": "mbta",
            "url": "https://cdn.mbta.com/realtime/TripUpdates.pb",
            "api_key": os.getenv("MBTA_API_KEY"),
            "output_path": "data/raw/realtime_gtfs",
        },
    )

    check_limit >> fetch_task
