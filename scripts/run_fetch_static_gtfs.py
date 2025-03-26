from src.ingestion.fetch_static_gtfs import fetch_static_gtfs

if __name__ == "__main__":
    fetch_static_gtfs(
        agency="mbta",
        url="https://cdn.mbta.com/MBTA_GTFS.zip",
        output_path="data/raw/static_gtfs",
    )
