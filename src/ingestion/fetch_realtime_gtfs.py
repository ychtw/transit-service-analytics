import os
from datetime import datetime

import requests
from google.transit import gtfs_realtime_pb2


def fetch_realtime_gtfs(
    agency: str = "transit",
    url: str = None,
    api_key: str = None,
    output_path: str = None,
) -> None:
    """
    Downloads the real-time GTFS data

    Parameters:
    - agency: GTFS feed agency name (e.g. mta, mbta, ...)
    - url: URL of the dataset
    - api_key: user api key of the real-time gtfs service
    - output_path: local output directory
    """
    # validate function inputs
    if not url:
        raise ValueError("url cannot be None")
    if not output_path:
        raise ValueError("output_path cannot be None")

    # make request to fetch data
    print(f"Download realtime GTFS from: {url}")
    headers = {"x-api-key": api_key} if api_key else {}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Download failed with status: {response.status_code}")

    # prep for file saving
    current_time = datetime.now()
    file_extension = url.split(".")[-1]
    filename = f"{agency}_realtime_gtfs_{current_time:%Y%m%d_%H%M%S}.{file_extension}"
    os.makedirs(output_path, exist_ok=True)
    file_path = os.path.join(output_path, filename)

    # save data to output location
    print(f"Save file to: {file_path}")
    with open(file_path, "wb") as f:
        f.write(response.content)

    # validate result
    if os.path.exists(file_path):
        print(f"File saved: {file_path}")
    else:
        print("File write failed")
