import json
from http.client import HTTPException
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urljoin

from ambientweather2sqlite import mureq
from ambientweather2sqlite.awparser import extract_labels, extract_units
from ambientweather2sqlite.units_mapping import units_for_columns

if TYPE_CHECKING:
    from ambientweather2sqlite.models import ColumnUnitMap, LabelMap


def create_metadata(
    database_path: str,
    live_data_url: str,
) -> tuple[LabelMap, ColumnUnitMap]:
    _database_path = Path(database_path)
    path = _database_path.parent / f"{_database_path.stem}_metadata.json"
    try:
        labels = extract_labels(mureq.get(live_data_url, auto_retry=True))
        units = extract_units(
            mureq.get(
                urljoin(live_data_url, "station.htm"),
                auto_retry=True,
            ),
        )
        labels_with_units, column_to_unit = units_for_columns(labels, units)
    except HTTPException as e:
        print(f"Error fetching metadata labels: {e}")
        return {}, {}
    metadata = {
        "title": "AmbientWeather Observations",
        "description": (
            "Weather observations collected from a local AmbientWeather station."
        ),
        "license": "GPL-3.0-or-later",
        "license_url": "https://www.gnu.org/licenses/gpl-3.0.html",
        "databases": {
            _database_path.stem: {
                "source": "AmbientWeather Station",
                "source_url": live_data_url,
                "about": "ambientweather2sqlite",
                "about_url": "https://github.com/hbmartin/ambientweather2sqlite",
                "tables": {
                    "observations": {
                        "description": "Minute-by-minute weather sensor readings.",
                        "sort_desc": "ts",
                        "columns": labels,
                        "units": column_to_unit,
                    },
                },
            },
        },
    }
    path.write_text(json.dumps(metadata, indent=4), encoding="utf-8")
    return labels_with_units, column_to_unit
