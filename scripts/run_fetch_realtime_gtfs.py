from dotenv import load_dotenv

from src.ingestion.fetch_realtime_gtfs import fetch_realtime_gtfs

load_dotenv()

if __name__ == "__main__":
    fetch_realtime_gtfs(
        url="https://cdn.mbta.com/realtime/TripUpdates.pb",
        api_key=os.getenv("MBTA_API_KEY"),
        output_path="data/raw/realtime_gtfs",
    )
