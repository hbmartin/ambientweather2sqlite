import json
from http.client import HTTPException
from pathlib import Path

from ambientweather2sqlite import mureq
from ambientweather2sqlite.awparser import extract_labels


def create_metadata(
    database_path: str,
    live_data_url: str,
) -> dict:
    _database_path = Path(database_path)
    path = _database_path.parent / f"{_database_path.stem}_metadata.json"
    try:
        body = mureq.get(live_data_url, auto_retry=True)
        labels = extract_labels(body)
    except HTTPException as e:
        print(f"Error fetching metadata labels: {e}")
        return {}
    metadata = {
        "databases": {
            _database_path.stem: {
                "source_url": live_data_url,
                "about_url": "https://github.com/hbmartin/ambientweather2sqlite",
                "tables": {
                    "observations": {
                        "columns": labels,
                    },
                },
            },
        },
    }
    path.write_text(json.dumps(metadata, indent=4))
    return labels
