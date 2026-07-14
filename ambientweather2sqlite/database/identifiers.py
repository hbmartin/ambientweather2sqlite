"""Persistent sensor-name to SQLite-column mapping."""

from __future__ import annotations

import re
import sqlite3
from typing import TYPE_CHECKING

from .connection import _DEFAULT_TABLE_NAME, _TS_COL

if TYPE_CHECKING:
    from collections.abc import Iterable

_SENSOR_MAPPING_TABLE = "aw2sqlite_sensor_columns"
_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_identifier(identifier: str) -> str:
    """Quote a SQLite identifier without changing its value."""
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _column_name(text: str) -> str:
    """Return the legacy, human-readable normalization of a sensor name."""
    return "".join(char if char.isalnum() or char == "_" else "_" for char in text)


def _encoded_column_name(raw_name: str) -> str:
    """Return an injective ASCII identifier for a raw sensor name."""
    return f"aw2_{raw_name.encode('utf-8').hex()}"


def _create_sensor_mapping_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_quote_identifier(_SENSOR_MAPPING_TABLE)} (
            raw_name TEXT PRIMARY KEY,
            column_name TEXT NOT NULL COLLATE NOCASE UNIQUE,
            label TEXT,
            unit TEXT
        )
        """,
    )


def _table_columns(
    conn: sqlite3.Connection,
    table_name: str = _DEFAULT_TABLE_NAME,
) -> dict[str, str]:
    cursor = conn.execute(f"PRAGMA table_info({_quote_identifier(table_name)})")
    return {str(row[1]).casefold(): str(row[1]) for row in cursor.fetchall()}


def _sensor_mappings(conn: sqlite3.Connection) -> dict[str, tuple[str, str]]:
    cursor = conn.execute(
        f"SELECT raw_name, column_name FROM {_quote_identifier(_SENSOR_MAPPING_TABLE)}",
    )
    return {
        str(column_name).casefold(): (str(raw_name), str(column_name))
        for raw_name, column_name in cursor.fetchall()
    }


def _mapped_column(conn: sqlite3.Connection, raw_name: str) -> str | None:
    row = conn.execute(
        f"SELECT column_name FROM {_quote_identifier(_SENSOR_MAPPING_TABLE)} "
        "WHERE raw_name = ?",
        (raw_name,),
    ).fetchone()
    return str(row[0]) if row is not None else None


def _column_is_available(
    column_name: str,
    raw_name: str,
    mappings: dict[str, tuple[str, str]],
) -> bool:
    if column_name.casefold() == _TS_COL.casefold():
        return False
    owner = mappings.get(column_name.casefold())
    return owner is None or owner[0] == raw_name


def _preferred_column_name(raw_name: str) -> str:
    legacy_name = _column_name(raw_name)
    if _SQL_IDENTIFIER_RE.fullmatch(legacy_name):
        return legacy_name
    return _encoded_column_name(raw_name)


def _allocate_column_name(
    conn: sqlite3.Connection,
    raw_name: str,
) -> str:
    mappings = _sensor_mappings(conn)
    preferred = _preferred_column_name(raw_name)
    if _column_is_available(preferred, raw_name, mappings):
        return preferred

    encoded = _encoded_column_name(raw_name)
    if _column_is_available(encoded, raw_name, mappings):
        return encoded

    suffix = 2
    while not _column_is_available(f"{encoded}_{suffix}", raw_name, mappings):
        suffix += 1
    return f"{encoded}_{suffix}"


def _resolve_sensor_names(
    conn: sqlite3.Connection,
    raw_names: Iterable[str],
) -> dict[str, str]:
    """Resolve raw sensor names to stable, collision-free physical columns."""
    _create_sensor_mapping_table(conn)
    resolved: dict[str, str] = {}
    for raw_name in dict.fromkeys(raw_names):
        if (mapped := _mapped_column(conn, raw_name)) is not None:
            resolved[raw_name] = mapped
            continue

        while True:
            column_name = _allocate_column_name(conn, raw_name)
            try:
                conn.execute(
                    f"INSERT INTO {_quote_identifier(_SENSOR_MAPPING_TABLE)} "
                    "(raw_name, column_name) VALUES (?, ?)",
                    (raw_name, column_name),
                )
            except (sqlite3.IntegrityError,):
                if (mapped := _mapped_column(conn, raw_name)) is not None:
                    column_name = mapped
                    break
                if column_name.casefold() in _sensor_mappings(conn):
                    continue
                raise
            break
        resolved[raw_name] = column_name
    return resolved


def _ensure_sensor_columns(
    conn: sqlite3.Connection,
    raw_names: Iterable[str],
    table_name: str = _DEFAULT_TABLE_NAME,
) -> tuple[dict[str, str], list[str]]:
    """Resolve sensor names and add any missing physical columns."""
    resolved = _resolve_sensor_names(conn, raw_names)
    existing_columns = _table_columns(conn, table_name)
    added_raw_names: list[str] = []

    for raw_name, column_name in resolved.items():
        if column_name.casefold() in existing_columns:
            continue
        conn.execute(
            f"ALTER TABLE {_quote_identifier(table_name)} "
            f"ADD COLUMN {_quote_identifier(column_name)} REAL",
        )
        existing_columns[column_name.casefold()] = column_name
        added_raw_names.append(raw_name)

    return resolved, added_raw_names
