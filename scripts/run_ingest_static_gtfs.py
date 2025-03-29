from pathlib import Path

import duckdb

from src.ingestion.ingest_static_gtfs import ingest_static_gtfs
from src.utils.sql_utils import drop_static_gtfs_tables, execute_sql_query

if __name__ == "__main__":
    sql_path = "sql/schema/static_gtfs_schema.sql"
    db_path = "data/staging/gtfs_staging.duckdb"
    static_gtfs_dir = Path("data/raw/static_gtfs/")

    print("Connect to DuckDB")
    conn = duckdb.connect(db_path)

    # TODO: consider other way to keep idempotency
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
