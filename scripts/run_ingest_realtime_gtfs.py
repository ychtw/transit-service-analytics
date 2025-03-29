from pathlib import Path

import duckdb

from src.ingestion.ingest_realtime_gtfs import (
    has_been_ingested,
    ingest_realtime_gtfs,
    mark_as_ingested,
)
from src.utils.sql_utils import execute_sql_query

if __name__ == "__main__":
    sql_path = "sql/schema/realtime_gtfs_schema.sql"
    db_path = "data/staging/gtfs_staging.duckdb"
    realtime_dir = Path("data/raw/realtime_gtfs")

    print("Connect to DuckDB")
    conn = duckdb.connect(db_path)

    print("Create realtime GTFS schema (if not exists)")
    execute_sql_query(sql_path=sql_path, db_path=db_path)

    for pb_file in realtime_dir.glob("*.pb"):
        filename = pb_file.name

        if has_been_ingested(conn, filename):
            print(f"Already ingested, skip {filename}")
            continue

        print(f"Ingest: {filename}")
        num_rows = ingest_realtime_gtfs(db_path=db_path, realtime_file=str(pb_file))

        if num_rows > 0:
            mark_as_ingested(conn, filename)
            print(f"Load {num_rows} rows from {filename}")
        else:
            print(f"No data found in {filename}")
