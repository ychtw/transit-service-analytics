from datetime import datetime, timedelta

import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator

from ingestion.ingest_realtime_gtfs import (
    has_been_ingested,
    ingest_realtime_gtfs,
    mark_as_ingested,
)
from utils.config import get_config
from utils.sql_utils import execute_sql_query

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=2),
}


def run_ingest_realtime_gtfs(**kwargs):
    file_path = kwargs["dag_run"].conf.get("file_path")
    if not file_path:
        raise ValueError("Missing `file_path`")
    # print(f"file_path: {file_path}")

    cfg = get_config()
    db_path = cfg["paths"]["staging"]["gtfs_db"]
    sql_path = cfg["paths"]["sql"]["realtime_gtfs_schema"]

    print("Connect to DuckDB")
    conn = duckdb.connect(db_path)

    print("Create realtime GTFS schema (if not exists)")
    execute_sql_query(sql_path=sql_path, db_path=db_path)

    filename = file_path.split("/")[-1]

    if has_been_ingested(conn, filename):
        print(f"Already ingested, skip {filename}")
        return

    print(f"Ingest {filename}")
    rows = ingest_realtime_gtfs(db_path=db_path, realtime_file=file_path)
    mark_as_ingested(conn, filename)

    print(f"Load {rows} rows from {filename}")


with DAG(
    dag_id="ingest_realtime_gtfs",
    default_args=default_args,
    schedule_interval=None,  # triggered externally by fetch_realtime_gtfs_dag
    catchup=False,
    max_active_runs=5,
    tags=["realtime-gtfs", "mbta"],
    is_paused_upon_creation=False,
) as dag:

    task = PythonOperator(
        task_id="ingest_realtime_gtfs_data",
        python_callable=run_ingest_realtime_gtfs,
    )
