import duckdb


def execute_sql_query(sql_path: str = None, db_path: str = None) -> None:
    """
    Execute the SQL script against a database

    Args:
    - sql_path (str): path to the sql script
    - db_path (str): path to the database
    """
    # validate function inputs
    if not sql_path:
        raise ValueError("sql_path cannot be None")
    if not db_path:
        raise ValueError("db_path cannot be None")

    # read & execute sql script
    with open(sql_path, "r") as f:
        query = f.read()

    conn = duckdb.connect(db_path)  # TODO: generalize later
    conn.execute(query)


def table_exists(conn, table_name: str) -> bool:
    """
    Check if a table exists in the connected DuckDB database.

    Args:
    - conn: duckdb.DuckDBPyConnection object
    - table_name: name of the table to check

    Returns:
    - True if table exists, False otherwise
    """
    result = conn.execute(
        f"""
        SELECT 1 FROM information_schema.tables
        WHERE table_name = '{table_name}'
        """
    ).fetchone()

    return result is not None


def drop_static_gtfs_tables(conn):
    """
    Drops all static GTFS tables in dependency-safe order.
    Assumes tables are prefixed with 'static_'.
    """
    drop_order = [
        "static_frequencies",  # depends on trips
        "static_shapes",
        "static_calendar_dates",  # depends on calendar
        "static_calendar",
        "static_stop_times",  # depends on trips, stops
        "static_trips",  # depends on routes
        "static_routes",  # depends on agency
        "static_stops",
        "static_agency",
    ]

    for table in drop_order:
        print(f"drop table: {table}")
        conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
