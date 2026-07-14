"""Application schema creation and compatibility checks."""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

from ambientweather2sqlite.exceptions import (
    MigrationRequiredError,
    UnsupportedSchemaVersionError,
)

from .connection import (
    _DEFAULT_TABLE_NAME,
    _SQLITE_TS_DEFAULT,
    _TS_COL,
    _connect_database,
)
from .identifiers import (
    _SENSOR_MAPPING_TABLE,
    _create_sensor_mapping_table,
    _quote_identifier,
)

if TYPE_CHECKING:
    import sqlite3

CURRENT_SCHEMA_VERSION = 1


def _unique_ts_index_name(table_name: str) -> str:
    return f"idx_{table_name}_{_TS_COL}"


def _unique_ts_index_sql(table_name: str) -> str:
    return (
        "CREATE UNIQUE INDEX IF NOT EXISTS "
        f"{_quote_identifier(_unique_ts_index_name(table_name))} ON "
        f"{_quote_identifier(table_name)}({_quote_identifier(_TS_COL)})"
    )


def _table_exists_on_connection(
    conn: sqlite3.Connection,
    table_name: str,
) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _schema_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("PRAGMA user_version").fetchone()
    return int(row[0])


def _has_unique_ts_index(
    conn: sqlite3.Connection,
    table_name: str,
) -> bool:
    cursor = conn.execute(f"PRAGMA index_list({_quote_identifier(table_name)})")
    for _, index_name, is_unique, _origin, is_partial in cursor.fetchall():
        if not is_unique or is_partial:
            continue
        index_cursor = conn.execute(
            f"PRAGMA index_info({_quote_identifier(str(index_name))})",
        )
        indexed_columns = [row[2] for row in index_cursor.fetchall()]
        if (
            len(indexed_columns) == 1
            and isinstance(indexed_columns[0], str)
            and indexed_columns[0].casefold() == _TS_COL.casefold()
        ):
            return True
    return False


def _sensor_mapping_columns_are_current(
    columns: list[tuple[object, ...]],
) -> bool:
    """Validate table_info rows using name/type/notnull/pk at positions 1/2/3/5."""
    contract = [
        (str(row[1]), str(row[2]).upper(), bool(row[3]), bool(row[5]))
        for row in columns
    ]
    return contract == [
        ("raw_name", "TEXT", False, True),
        ("column_name", "TEXT", True, False),
        ("label", "TEXT", False, False),
        ("unit", "TEXT", False, False),
    ]


def _has_sensor_mapping_column_index(conn: sqlite3.Connection) -> bool:
    """Validate index_list and index_xinfo unique-column contracts.

    index_list exposes name/unique/partial at positions 1/2/4. index_xinfo
    exposes column name/collation/key-column at positions 2/4/5.
    """
    indexes = conn.execute(
        f"PRAGMA index_list({_quote_identifier(_SENSOR_MAPPING_TABLE)})",
    ).fetchall()
    for _, index_name, is_unique, _origin, is_partial in indexes:
        if (bool(is_unique), bool(is_partial)) != (True, False):
            continue
        index_columns = conn.execute(
            f"PRAGMA index_xinfo({_quote_identifier(str(index_name))})",
        ).fetchall()
        key_columns = [row for row in index_columns if row[5]]
        key_contract = [
            (str(row[2]).casefold(), str(row[4]).casefold()) for row in key_columns
        ]
        if key_contract == [("column_name", "nocase")]:
            return True
    return False


def _sensor_mapping_table_is_current(conn: sqlite3.Connection) -> bool:
    if not _table_exists_on_connection(conn, _SENSOR_MAPPING_TABLE):
        return False
    columns = conn.execute(
        f"PRAGMA table_info({_quote_identifier(_SENSOR_MAPPING_TABLE)})",
    ).fetchall()
    return _sensor_mapping_columns_are_current(
        columns,
    ) and _has_sensor_mapping_column_index(conn)


def _timestamp_is_not_null(
    conn: sqlite3.Connection,
    table_name: str,
) -> bool:
    cursor = conn.execute(f"PRAGMA table_info({_quote_identifier(table_name)})")
    return any(
        str(row[1]).casefold() == _TS_COL.casefold() and bool(row[3])
        for row in cursor.fetchall()
    )


def _schema_is_current(
    conn: sqlite3.Connection,
    table_name: str = _DEFAULT_TABLE_NAME,
) -> bool:
    return (
        _schema_version(conn) == CURRENT_SCHEMA_VERSION
        and _sensor_mapping_table_is_current(conn)
        and _has_unique_ts_index(conn, table_name)
        and _timestamp_is_not_null(conn, table_name)
    )


def _schema_has_objects(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' LIMIT 1",
    ).fetchone()
    return row is not None


def _create_current_schema(
    conn: sqlite3.Connection,
    table_name: str,
) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_quote_identifier(table_name)} (
            {_quote_identifier(_TS_COL)} TIMESTAMP NOT NULL
                DEFAULT ({_SQLITE_TS_DEFAULT})
        )
        """,
    )
    _create_sensor_mapping_table(conn)
    conn.execute(_unique_ts_index_sql(table_name))
    conn.execute(f"PRAGMA user_version = {CURRENT_SCHEMA_VERSION}")


def create_database_if_not_exists(
    db_path: str,
    table_name: str = _DEFAULT_TABLE_NAME,
) -> bool:
    """Create a current database or require explicit migration of a legacy one."""
    with closing(_connect_database(db_path, read_only=False)) as conn:
        conn.execute("BEGIN IMMEDIATE")
        version = _schema_version(conn)
        if version > CURRENT_SCHEMA_VERSION:
            raise UnsupportedSchemaVersionError(
                version,
                CURRENT_SCHEMA_VERSION,
            )
        if _table_exists_on_connection(conn, table_name):
            if not _schema_is_current(conn, table_name):
                raise MigrationRequiredError(version, CURRENT_SCHEMA_VERSION)
            conn.rollback()
            return False
        if _schema_has_objects(conn):
            raise MigrationRequiredError(version, CURRENT_SCHEMA_VERSION)

        _create_current_schema(conn, table_name)
        conn.commit()

    print(f"Database created with table '{table_name}' at: {db_path}")
    return True
