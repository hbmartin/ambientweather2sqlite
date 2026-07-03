"""Observation validation and insertion."""

from __future__ import annotations

import logging
from contextlib import closing
from typing import TYPE_CHECKING

from ambientweather2sqlite.exceptions import UnexpectedEmptyDictionaryError

from .connection import (
    _DEFAULT_TABLE_NAME,
    _TS_COL,
    _connect_database,
    _current_observation_timestamp,
)
from .schema import _column_name, _ensure_columns

if TYPE_CHECKING:
    import sqlite3

    from ambientweather2sqlite.models import Observation

_logger = logging.getLogger(__name__)

# Generous bounds for sensor value validation (column_substring -> (min, max))
_SENSOR_BOUNDS: dict[str, tuple[float, float]] = {
    "temp": (-150.0, 200.0),
    "humi": (-1.0, 101.0),
    "wind": (-1.0, 500.0),
    "gust": (-1.0, 500.0),
    "rain": (-1.0, 2000.0),
    "press": (0.0, 2000.0),
    "uv": (-1.0, 2000.0),
    "solar": (-1.0, 5000.0),
    "pm": (-1.0, 3000.0),
}


def _prepare_observation(
    observation: Observation,
) -> dict[str, str | int | float | None]:
    prepared = dict(observation)
    ts_value = prepared.get(_TS_COL)
    if not isinstance(ts_value, str) or not ts_value.strip():
        prepared[_TS_COL] = _current_observation_timestamp()
    return prepared


def _validate_observation(observation: Observation) -> None:
    """Emit warning logs for sensor values outside plausible ranges."""
    for column, value in observation.items():
        if not isinstance(value, int | float):
            continue
        col_lower = column.lower()
        for substr, (lo, hi) in _SENSOR_BOUNDS.items():
            if substr in col_lower and not lo <= value <= hi:
                _logger.warning(
                    "Implausible value for %s: %s (expected %s..%s)",
                    column,
                    value,
                    lo,
                    hi,
                )
                break


def _insert_dict_row(
    conn: sqlite3.Connection,
    table_name: str,
    data_dict: Observation,
) -> int | None:
    """Alternative version that takes an existing connection.

    Args:
        conn (sqlite3.Connection): Existing database connection
        table_name (str): Name of the table to insert into
        data_dict (dict): Dictionary where keys are column names and values are the data

    Returns:
        int: The rowid of the inserted row

    """
    if not data_dict:
        raise UnexpectedEmptyDictionaryError

    cursor = conn.cursor()

    columns = [_column_name(c) for c in list(data_dict.keys())]
    values = list(data_dict.values())

    placeholders = ", ".join(["?" for _ in values])
    columns_str = ", ".join(columns)

    query = (
        f"INSERT OR IGNORE INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    )
    cursor.execute(query, values)
    conn.commit()
    if cursor.rowcount <= 0:
        _logger.debug(
            "Ignored insert into %s because the row already exists",
            table_name,
        )
        return None
    return cursor.lastrowid


def insert_observation(
    db_path: str,
    observation: Observation,
) -> None:
    prepared_observation = _prepare_observation(observation)
    _validate_observation(prepared_observation)
    with closing(_connect_database(db_path, read_only=False)) as conn:
        _ensure_columns(conn, set(prepared_observation.keys()))
        _insert_dict_row(conn, _DEFAULT_TABLE_NAME, prepared_observation)
