from datetime import datetime, timedelta
from pathlib import Path

import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator

from ingestion.ingest_static_gtfs import ingest_static_gtfs
from utils.config import get_config
from utils.sql_utils import drop_static_gtfs_tables, execute_sql_query

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 20),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
}


def run_ingest_static_gtfs(**kwargs):
    """
    Ingest static GTFS data into a DuckDB database.

    Steps:
      1. Connects to the DuckDB database.
      2. Drops existing static GTFS tables (if any).
      3. Executes SQL schema scripts to recreate the static GTFS schema.
      4. Iterates over all GTFS zip files in the provided directory and ingests them.

    Args:
        sql_path (str or Path): Path to the SQL script that defines the static GTFS schema.
        db_path (str): Path to the DuckDB database file.
        static_gtfs_dir (Path): Directory containing GTFS zip files to ingest.

    Raises:
        duckdb.IOException: If the database connection fails or schema execution fails.
    """
    # get configs
    cfg = get_config()
    sql_path = cfg["paths"]["sql"]["static_gtfs_schema"]
    db_path = cfg["paths"]["staging"]["gtfs_db"]
    static_gtfs_dir = Path(cfg["paths"]["raw"]["static_gtfs"])

    print("Connect to DuckDB")
    conn = duckdb.connect(db_path)

    print("Drop existing static GTFS tables")
    drop_static_gtfs_tables(conn)

    print("Create static GTFS schema")
    execute_sql_query(sql_path=sql_path, db_path=db_path)

    for zip_file in static_gtfs_dir.glob("*.zip"):
        print(f"Load static GTFS: {zip_file.name}")
        ingest_static_gtfs(
            db_path=db_path,
            static_gtfs_zip=str(zip_file),
        )


with DAG(
    dag_id="ingest_static_gtfs",
    default_args=default_args,
    schedule_interval=None,  # only run when triggered by fetch_static_gtfs_dag
    catchup=False,
    max_active_runs=1,
    tags=["static-gtfs", "mbta"],
    is_paused_upon_creation=False,
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_static_gtfs_data",
        python_callable=run_ingest_static_gtfs,
    )
