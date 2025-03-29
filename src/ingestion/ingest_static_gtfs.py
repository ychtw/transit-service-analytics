import zipfile

import duckdb
import pandas as pd


def ingest_static_gtfs(db_path: str = None, static_gtfs_zip: str = None):
    """
    Load static GTFS data from a zip file into a DuckDB database.

    Each .txt file in the GTFS zip archive is loaded into a corresponding table
    in the database, assuming the table already exists with the correct schema.
    Rows violating foreign key constraints are filtered out before insertion.

    Args:
        db_path (str): Path to the DuckDB database file.
        static_gtfs_zip (str): Path to the GTFS zip file containing .txt files.

    Raises:
        ValueError: If `db_path` or `static_gtfs_zip` is not provided.
    """
    # validate function inputs
    if not db_path:
        raise ValueError("db_path cannot be None")
    if not static_gtfs_zip:
        raise ValueError("schema_sql_path cannot be None")

    conn = duckdb.connect(db_path)

    # get all existing table names starting with "static_"
    static_tables = set(
        row[0]
        for row in conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name LIKE 'static_%'
            """
        ).fetchall()
    )

    # create a map of base_name (like 'agency') to full table name (like 'static_agency')
    table_map = {tbl.replace("static_", ""): tbl for tbl in static_tables}

    # load static gtfs data to db: each .txt file in the zip file will be a table
    # TODO:
    #   - refactor to accomodate the case of multiple agencies
    #   - generalize later to enable different databases
    with zipfile.ZipFile(static_gtfs_zip, "r") as zf:
        for name in zf.namelist():
            if name.endswith(".txt"):
                base_name = name.replace(".txt", "").lower()

                if base_name not in table_map:
                    print(f"Skip unrecognized file: {name}")
                    continue

                table_name = table_map[base_name]

                with zf.open(name) as f:
                    df = pd.read_csv(f)
                    # print(f"columns: {df.columns}")
                    # print(f"table_name: {table_name}")

                    # get expected columns from schema
                    expected_cols = [
                        row[1]
                        for row in conn.execute(
                            f"PRAGMA table_info('{table_name}')"
                        ).fetchall()
                    ]
                    # print(f"expected_cols: {expected_cols}")

                    # add missing columns (if there's any)
                    for col in expected_cols:
                        if col not in df.columns:
                            df[col] = None

                    # reorder columns to match schema
                    df = df[expected_cols]

                    # drop records where FK constraint is violated
                    df = apply_foreign_key_filters(df, conn, table_name)

                    print(f"Load data into table {table_name} ({len(df)} rows)")
                    conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")


def apply_foreign_key_filters(df, conn, table_name):
    """
    Apply hardcoded foreign key constraints for static GTFS tables.

    Rows that violate known FK relationships (e.g. invalid `trip_id` or `route_id`)
    are dropped from the DataFrame before insertion into the database.

    Args:
        df (pd.DataFrame): DataFrame to filter.
        conn (duckdb.DuckDBPyConnection): Active connection to the DuckDB database.
        table_name (str): Name of the table the data will be inserted into.

    Returns:
        pd.DataFrame: Filtered DataFrame with FK violations removed.
    """
    # FK relationships of the static gtfs
    # "conn.execute(f"PRAGMA foreign_keys('{table_name}')").fetchall()" not work on duckdb
    # TODO: find other ways to handle this
    fk_constraints = {
        "static_routes": [("agency_id", "static_agency", "agency_id")],
        "static_trips": [("route_id", "static_routes", "route_id")],
        "static_stop_times": [
            ("trip_id", "static_trips", "trip_id"),
            ("stop_id", "static_stops", "stop_id"),
        ],
        "static_calendar_dates": [("service_id", "static_calendar", "service_id")],
        "static_frequencies": [("trip_id", "static_trips", "trip_id")],
    }

    if table_name not in fk_constraints:
        return df  # no FK checks needed

    for from_col, ref_table, to_col in fk_constraints[table_name]:
        valid = conn.execute(f"SELECT {to_col} FROM {ref_table}").fetchdf()[to_col]
        valid_set = set(valid)
        before = len(df)
        df = df[df[from_col].isin(valid_set)]
        dropped = before - len(df)

        if dropped > 0:
            print(
                f"Dropped {dropped} row(s) from {table_name} due to FK mismatch on {from_col} -> {ref_table}.{to_col}"
            )

    return df
