import sys
import time

from database import insert_observation
from fetcher import fetch_live_data


def start_daemon(live_data_url: str, database_path: str) -> None:
    print(f"Starting to watch {live_data_url}")
    print("Press Ctrl+C to stop")
    try:
        while True:
            live_data = fetch_live_data(live_data_url)
            print(live_data)
            insert_observation(database_path, live_data)
            time.sleep(60)
    except KeyboardInterrupt:
        print(f"\nStopping... results saved to {database_path}")
        sys.exit(0)
