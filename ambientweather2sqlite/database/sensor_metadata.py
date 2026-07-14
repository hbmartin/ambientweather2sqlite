"""Persistence helpers for sensor labels and units."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

from .connection import _connect_database
from .identifiers import (
    _SENSOR_MAPPING_TABLE,
    _quote_identifier,
    _resolve_sensor_names,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


def persist_sensor_metadata(
    database_path: str,
    labels: Mapping[str, str],
    column_to_unit: Mapping[str, str],
) -> dict[str, str]:
    """Persist labels and units and return their physical column names."""
    with closing(_connect_database(database_path, read_only=False)) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            resolved = _resolve_sensor_names(conn, labels)
            conn.executemany(
                f"UPDATE {_quote_identifier(_SENSOR_MAPPING_TABLE)} "
                "SET label = ?, unit = ? WHERE raw_name = ?",
                (
                    (label, column_to_unit.get(raw_name), raw_name)
                    for raw_name, label in labels.items()
                ),
            )
            conn.commit()
        except (sqlite3.Error,):
            conn.rollback()
            raise
    return resolved


def query_sensor_metadata(
    database_path: str,
) -> list[tuple[str, str, str | None, str | None]]:
    """Return persisted raw names, physical columns, labels, and units."""
    with closing(_connect_database(database_path, read_only=True)) as conn:
        rows = conn.execute(
            "SELECT raw_name, column_name, label, unit "
            f"FROM {_quote_identifier(_SENSOR_MAPPING_TABLE)} "
            "ORDER BY column_name",
        ).fetchall()
    return [
        (
            str(raw_name),
            str(column_name),
            str(label) if label is not None else None,
            str(unit) if unit is not None else None,
        )
        for raw_name, column_name, label, unit in rows
    ]
