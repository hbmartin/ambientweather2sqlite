import json
import sys
import time
from datetime import datetime
from pathlib import Path

from database import insert_observation
from fetcher import fetch_labels, fetch_live_data


def create_metadata_if_not_exists(
    database_path: str, live_data_url: str,
) -> dict[str, str]:
    path = Path(".".join(database_path.split(".")[:-1]) + "_metadata.json")
    if not path.exists():
        labels = fetch_labels(live_data_url)
        path.write_text(json.dumps(labels, indent=4))
    return json.loads(path.read_text())


def clear_lines(n: int) -> None:
    for _ in range(n):
        print("\033[A\033[K", end="")


def start_daemon(
    live_data_url: str, database_path: str, period_seconds: int = 60,
) -> None:
    print(f"Observing {live_data_url}")
    print("Press Ctrl+C to stop")
    metadata = create_metadata_if_not_exists(database_path, live_data_url)
    remove_newlines = 0
    try:
        while True:
            clear_lines(remove_newlines)
            print(f"Updated at: {datetime.now()}")
            live_data = fetch_live_data(live_data_url)
            labeled_data = {
                metadata.get(key, key): value for key, value in live_data.items()
            }
            pretty_data = json.dumps(labeled_data, indent=4)
            remove_newlines = pretty_data.count("\n") + 2
            print(pretty_data)
            insert_observation(database_path, live_data)
            for i in range(period_seconds, 0, -1):
                print(f"Next update in {i} seconds", end="\r")
                time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nStopping... results saved to {database_path}")
        sys.exit(0)
