from __future__ import annotations

import json
import os
import stat
import tempfile
from http.client import HTTPException
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urljoin

from ambientweather2sqlite import mureq
from ambientweather2sqlite.awparser import extract_labels, extract_units
from ambientweather2sqlite.database import (
    persist_sensor_metadata,
    query_sensor_metadata,
)
from ambientweather2sqlite.units_mapping import units_for_columns

if TYPE_CHECKING:
    from ambientweather2sqlite.models import ColumnUnitMap, LabelMap


def metadata_path(database_path: str) -> Path:
    path = Path(database_path)
    return path.parent / f"{path.stem}_metadata.json"


def _metadata_document(
    database_path: str,
    live_data_url: str,
    labels: LabelMap,
    column_to_unit: ColumnUnitMap,
) -> dict[str, object]:
    database_name = Path(database_path).stem
    return {
        "title": "AmbientWeather Observations",
        "description": (
            "Weather observations collected from a local AmbientWeather station."
        ),
        "license": "GPL-3.0-or-later",
        "license_url": "https://www.gnu.org/licenses/gpl-3.0.html",
        "databases": {
            database_name: {
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


def _write_metadata_file(
    database_path: str,
    live_data_url: str,
    labels: LabelMap,
    column_to_unit: ColumnUnitMap,
) -> Path:
    path = metadata_path(database_path)
    metadata = _metadata_document(
        database_path,
        live_data_url,
        labels,
        column_to_unit,
    )
    file_descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8") as temporary_file:
            json.dump(metadata, temporary_file, indent=4)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        try:
            mode = stat.S_IMODE(path.stat().st_mode)
        except (FileNotFoundError,):
            mode = 0o644
        temporary_path.chmod(mode)
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return path


def write_metadata_from_database(
    database_path: str,
    live_data_url: str,
) -> Path | None:
    """Regenerate the Datasette sidecar from metadata stored in the database."""
    rows = query_sensor_metadata(database_path)
    if not rows:
        return None

    labels = {
        str(column_name): str(label) if label is not None else str(raw_name)
        for raw_name, column_name, label, _ in rows
    }
    column_to_unit = {
        str(column_name): str(unit)
        for _, column_name, _, unit in rows
        if unit is not None
    }
    return _write_metadata_file(
        database_path,
        live_data_url,
        labels,
        column_to_unit,
    )


def create_metadata(
    database_path: str,
    live_data_url: str,
) -> tuple[LabelMap, ColumnUnitMap]:
    try:
        labels = extract_labels(mureq.get(live_data_url, auto_retry=True))
        units = extract_units(
            mureq.get(
                urljoin(live_data_url, "station.htm"),
                auto_retry=True,
            ),
        )
        labels_with_units, column_to_unit = units_for_columns(labels, units)
    except (HTTPException, TimeoutError) as e:
        print(f"Error fetching metadata labels: {e}")
        return {}, {}
    if not labels:
        print("Error fetching metadata labels: parsed 0 sensor labels")
        return {}, {}

    persist_sensor_metadata(database_path, labels, column_to_unit)
    write_metadata_from_database(database_path, live_data_url)
    return labels_with_units, column_to_unit
