"""Table creation, dynamic columns, and index maintenance."""

from __future__ import annotations

import logging
import sqlite3
from contextlib import closing
from pathlib import Path

from .connection import (
    _DEFAULT_TABLE_NAME,
    _SQLITE_TS_DEFAULT,
    _TS_COL,
    _connect_database,
)

_logger = logging.getLogger(__name__)


def _column_name(text: str) -> str:
    result = []
    for char in text:
        if char.isalnum() or char == "_":
            result.append(char)
        else:
            result.append("_")
    return "".join(result)


def _unique_ts_index_name(table_name: str) -> str:
    return f"idx_{table_name}_{_TS_COL}"


def _unique_ts_index_sql(table_name: str) -> str:
    return (
        f"CREATE UNIQUE INDEX IF NOT EXISTS {_unique_ts_index_name(table_name)} "
        f"ON {table_name}({_TS_COL})"
    )


def _ensure_columns(
    conn: sqlite3.Connection,
    required_columns: set[str],
    table_name: str = _DEFAULT_TABLE_NAME,
) -> list[str]:
    """Checks if a table has columns for every string in required_columns.
    If not, adds the missing columns with REAL type.

    Args:
        conn (sqlite3.Connection): Connection to the SQLite database
        required_columns (set): Set of column names that should exist
        table_name (str): Name of the table to check/modify

    Returns:
        list: List of column names that were added

    Raises:
        sqlite3.Error: If there's a database error

    """
    added_columns = []

    cursor = conn.cursor()

    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}  # row[1] is column name

    required_column_names = {
        _column_name(column_name) for column_name in required_columns
    }
    missing_columns = required_column_names - existing_columns

    for column_name in missing_columns:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} REAL")
        added_columns.append(column_name)

    cursor.close()
    conn.commit()

    return added_columns


def _table_exists(db_path: str, table_name: str) -> bool:
    """Check for the table in an existing database file.

    Real errors (locks, corruption, I/O) propagate rather than being treated
    as "no table", which would misroute an existing database into the create
    path and skip the unique-ts-index migration.
    """
    if not Path(db_path).exists():
        return False

    with closing(_connect_database(db_path, read_only=True)) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        )
        return cursor.fetchone() is not None


def create_database_if_not_exists(
    db_path: str,
    table_name: str = _DEFAULT_TABLE_NAME,
) -> bool:
    """Check if a SQLite database exists at the specified path.
    If not, create the database and a table with the given name.

    Args:
        db_path (str): Path to the SQLite database file
        table_name (str): Name of the table to create

    Returns:
        bool: True if database was created, False if it already existed

    """
    if _table_exists(db_path, table_name):
        _ensure_unique_ts_index(db_path, table_name)
        return False

    with closing(_connect_database(db_path, read_only=False)) as conn:
        cursor = conn.cursor()

        table_schema = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {_TS_COL} TIMESTAMP DEFAULT ({_SQLITE_TS_DEFAULT})
            )
        """

        cursor.execute(table_schema)
        cursor.execute(_unique_ts_index_sql(table_name))
        conn.commit()

        print(f"Database created with table '{table_name}' at: {db_path}")
        return True


def _has_unique_ts_index(
    conn: sqlite3.Connection,
    table_name: str,
) -> bool:
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA index_list({table_name})")
    for _, index_name, is_unique, *_ in cursor.fetchall():
        if not is_unique:
            continue
        cursor.execute(f"PRAGMA index_info({index_name})")
        indexed_columns = [row[2] for row in cursor.fetchall()]
        if indexed_columns == [_TS_COL]:
            return True
    return False


def _deduplicate_timestamps(
    conn: sqlite3.Connection,
    table_name: str,
) -> int:
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    value_columns = [column for column in columns if column != _TS_COL]

    cursor.execute(
        f"SELECT {_TS_COL} FROM {table_name} "
        f"WHERE {_TS_COL} IS NOT NULL "
        f"GROUP BY {_TS_COL} HAVING COUNT(*) > 1",
    )
    duplicate_timestamps = [row[0] for row in cursor.fetchall()]

    if not duplicate_timestamps:
        return 0

    removed_rows = 0
    selected_columns = ", ".join([_TS_COL, *value_columns])
    assignments = ", ".join(f"{column} = ?" for column in [_TS_COL, *value_columns])

    for timestamp in duplicate_timestamps:
        cursor.execute(
            f"SELECT rowid, {selected_columns} FROM {table_name} "
            f"WHERE {_TS_COL} = ? ORDER BY rowid",
            (timestamp,),
        )
        rows = cursor.fetchall()
        keep_rowid = rows[0][0]
        merged_row: list[str | float | int | None] = []

        for column_index in range(1, len(columns) + 1):
            merged_value = next(
                (
                    row[column_index]
                    for row in reversed(rows)
                    if row[column_index] is not None
                ),
                None,
            )
            merged_row.append(merged_value)

        cursor.execute(
            f"UPDATE {table_name} SET {assignments} WHERE rowid = ?",
            [*merged_row, keep_rowid],
        )

        duplicate_rowids = [(row[0],) for row in rows[1:]]
        cursor.executemany(
            f"DELETE FROM {table_name} WHERE rowid = ?",
            duplicate_rowids,
        )
        removed_rows += len(duplicate_rowids)

    return removed_rows


def _ensure_unique_ts_index(
    db_path: str,
    table_name: str = _DEFAULT_TABLE_NAME,
) -> None:
    """Add a UNIQUE index on ts for existing databases that lack one."""
    with closing(_connect_database(db_path, read_only=False)) as conn:
        if _has_unique_ts_index(conn, table_name):
            return

        try:
            conn.execute(_unique_ts_index_sql(table_name))
        except sqlite3.IntegrityError:
            removed_rows = _deduplicate_timestamps(conn, table_name)
            _logger.warning(
                "Removed %s duplicate observations while adding a UNIQUE index on %s",
                removed_rows,
                _TS_COL,
            )
            conn.execute(_unique_ts_index_sql(table_name))
        conn.commit()
