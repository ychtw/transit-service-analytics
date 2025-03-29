import duckdb
import pandas as pd
from google.transit import gtfs_realtime_pb2


def has_been_ingested(conn, filename: str) -> bool:
    """
    Check if a GTFS-RT file has already been ingested.

    Args:
        conn (duckdb.DuckDBPyConnection): Open DuckDB connection.
        filename (str): Name of the file to check.

    Returns:
        bool: True if the file was already ingested, False otherwise.
    """
    result = conn.execute(
        "SELECT 1 FROM realtime_ingestion_log WHERE filename = ?", [filename]
    ).fetchone()
    return result is not None


def mark_as_ingested(conn, filename: str) -> None:
    """
    Record a GTFS-RT file as ingested in the tracking log table.

    Args:
        conn (duckdb.DuckDBPyConnection): Open DuckDB connection.
        filename (str): Name of the file that has been ingested.
    """
    conn.execute("INSERT INTO realtime_ingestion_log (filename) VALUES (?)", [filename])


def ingest_realtime_gtfs(db_path: str, realtime_file: str) -> int:
    """
    Parse a GTFS-RT (Trip Updates) protobuf file and insert its contents into DuckDB.

    This function extracts relevant trip update data (e.g. trip_id, stop_id, delays)
    from a GTFS real-time `.pb` file and loads it into the `realtime_trip_updates` table.

    Args:
        db_path (str): Path to the DuckDB database file.
        realtime_file (str): Path to the GTFS real-time `.pb` file.

    Returns:
        int: Number of records successfully ingested. Returns 0 if no data is found.

    Raises:
        Exception: If the protobuf file is corrupted or cannot be parsed.
    """
    with open(realtime_file, "rb") as f:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(f.read())

    timestamp = pd.to_datetime(feed.header.timestamp, unit="s")
    rows = []

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        tu = entity.trip_update
        trip = tu.trip
        vehicle = tu.vehicle

        for stu in tu.stop_time_update:
            rows.append(
                {
                    "timestamp": timestamp,
                    "trip_id": trip.trip_id,
                    "route_id": trip.route_id,
                    "start_date": trip.start_date,
                    "start_time": trip.start_time,
                    "vehicle_id": vehicle.id if vehicle else None,
                    "vehicle_label": vehicle.label if vehicle else None,
                    "stop_id": stu.stop_id,
                    "stop_sequence": stu.stop_sequence,
                    "arrival_time": (
                        pd.to_datetime(stu.arrival.time, unit="s")
                        if stu.HasField("arrival") and stu.arrival.HasField("time")
                        else None
                    ),
                    "arrival_delay": (
                        stu.arrival.delay
                        if stu.HasField("arrival") and stu.arrival.HasField("delay")
                        else None
                    ),
                    "departure_time": (
                        pd.to_datetime(stu.departure.time, unit="s")
                        if stu.HasField("departure") and stu.departure.HasField("time")
                        else None
                    ),
                    "departure_delay": (
                        stu.departure.delay
                        if stu.HasField("departure") and stu.departure.HasField("delay")
                        else None
                    ),
                }
            )

    if rows:
        df = pd.DataFrame(rows)
        conn = duckdb.connect(db_path)
        conn.execute("INSERT INTO realtime_trip_updates SELECT * FROM df")
        return len(df)
    return 0
