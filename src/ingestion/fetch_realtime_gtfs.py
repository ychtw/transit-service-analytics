import os
from datetime import datetime

import requests


def fetch_realtime_gtfs(
    agency: str = "transit",
    url: str = None,
    api_key: str = None,
    output_path: str = None,
) -> str:
    """
    Fetch and store real-time GTFS data from a given URL.

    Args:
        agency (str): Name of the transit agency (used for filename prefix).
        url (str): Full URL to the GTFS real-time .pb feed.
        api_key (str, optional): API key required for accessing the feed (if applicable).
        output_path (str): Directory where the downloaded file will be stored.

    Returns:
        str: Full path to the saved `.pb` file if successful.
             Returns `None` if saving the file fails.

    Raises:
        ValueError: If required parameters `url` or `output_path` are not provided.
        Exception: If the request fails with a non-200 HTTP status.
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
        return file_path
    else:
        print("File write failed")
        return None
