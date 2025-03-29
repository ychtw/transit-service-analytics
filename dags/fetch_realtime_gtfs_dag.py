import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import DagRun
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dotenv import load_dotenv

from ingestion.fetch_realtime_gtfs import fetch_realtime_gtfs
from utils.config import get_config


# TODO: limit num of runs for testing purpose, might remove later
def stop_after_n_runs(max_runs=3, **context):
    dag_id = context["dag"].dag_id
    dag_runs = DagRun.find(dag_id=dag_id)
    if len(dag_runs) >= max_runs:
        subprocess.run(["airflow", "dags", "pause", dag_id], check=True)
        raise Exception(f"Reached maximum runs ({max_runs}), pause DAG '{dag_id}'")


def fetch_realtime_wrapper(**kwargs):
    """
    Load environment and config at runtime, then call fetch_realtime_gtfs.
    """
    load_dotenv()
    api_key = os.getenv("MBTA_API_KEY")

    cfg = get_config()
    url = cfg["api"]["realtime_url"]
    output_path = cfg["paths"]["raw"]["realtime_gtfs"]

    file_path = fetch_realtime_gtfs(
        agency="mbta",
        url=url,
        api_key=api_key,
        output_path=output_path,
    )

    # pass file path as task instance to next dag via xcom (if there's new file saved)
    if file_path:
        kwargs["ti"].xcom_push(key="file_path", value=file_path)
        return file_path
    else:
        print("No new file saved — skip ingest trigger")
        kwargs["ti"].xcom_push(key="file_path", value=None)
        return None


def should_trigger_ingest(**kwargs):
    """
    Check if a file_path was returned. If not, skip downstream ingestion.
    """
    file_path = kwargs["ti"].xcom_pull(
        task_ids="fetch_realtime_gtfs_data", key="file_path"
    )
    return file_path is not None


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
        python_callable=fetch_realtime_wrapper,
    )

    check_file_exists = ShortCircuitOperator(
        task_id="check_file_exists",
        python_callable=should_trigger_ingest,
    )

    # TODO: consider trigger ingest in larger batches (probably no need to ingest everytime)
    trigger_ingest = TriggerDagRunOperator(
        task_id="trigger_ingest_realtime_gtfs",
        trigger_dag_id="ingest_realtime_gtfs",
        wait_for_completion=False,
        reset_dag_run=True,
        conf={
            "file_path": "{{ ti.xcom_pull(task_ids='fetch_realtime_gtfs_data', key='file_path') }}"
        },
    )

    check_limit >> fetch_task >> check_file_exists >> trigger_ingest
