"""Inspectable, backed-up database migrations."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from ambientweather2sqlite.exceptions import (
    NullTimestampMigrationError,
    UnsupportedSchemaVersionError,
)

from .backup import backup_database
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
    _table_columns,
)
from .schema import (
    CURRENT_SCHEMA_VERSION,
    _has_unique_ts_index,
    _schema_version,
    _sensor_mapping_table_is_current,
    _table_exists_on_connection,
    _timestamp_is_not_null,
    _unique_ts_index_name,
    _unique_ts_index_sql,
)

_MIGRATION_AUDIT_TABLE = "aw2sqlite_migration_conflicts"


@dataclass(frozen=True, slots=True)
class MigrationPlan:
    current_version: int
    target_version: int
    unique_timestamp_index: bool
    timestamp_not_null: bool
    sensor_mapping_table: bool
    duplicate_rows_to_remove: int
    null_timestamp_rows: int

    @property
    def required(self) -> bool:
        """Whether applying the current application schema is necessary."""
        return (
            self.current_version != self.target_version
            or not self.unique_timestamp_index
            or not self.timestamp_not_null
            or not self.sensor_mapping_table
        )

    def as_dict(self) -> dict[str, int | bool]:
        """Return a JSON-serializable migration plan."""
        return {**asdict(self), "required": self.required}


@dataclass(frozen=True, slots=True)
class MigrationResult:
    backup_path: str | None
    removed_duplicate_rows: int
    schema_version: int

    def as_dict(self) -> dict[str, str | int | None]:
        """Return a JSON-serializable migration result."""
        return asdict(self)


def _duplicate_row_count(
    conn: sqlite3.Connection,
    table_name: str,
) -> int:
    row = conn.execute(
        "SELECT COALESCE(SUM(count - 1), 0) FROM ("
        f"SELECT COUNT(*) AS count FROM {_quote_identifier(table_name)} "
        f"WHERE {_quote_identifier(_TS_COL)} IS NOT NULL "
        f"GROUP BY {_quote_identifier(_TS_COL)} HAVING COUNT(*) > 1"
        ")",
    ).fetchone()
    return int(row[0])


def _null_timestamp_count(
    conn: sqlite3.Connection,
    table_name: str,
) -> int:
    row = conn.execute(
        f"SELECT COUNT(*) FROM {_quote_identifier(table_name)} "
        f"WHERE {_quote_identifier(_TS_COL)} IS NULL",
    ).fetchone()
    return int(row[0])


def _inspect_database_migration_connection(
    conn: sqlite3.Connection,
    table_name: str,
) -> MigrationPlan:
    if not _table_exists_on_connection(conn, table_name):
        msg = f"Missing table: {table_name}"
        raise sqlite3.OperationalError(msg)
    current_version = _schema_version(conn)
    if current_version > CURRENT_SCHEMA_VERSION:
        raise UnsupportedSchemaVersionError(
            current_version,
            CURRENT_SCHEMA_VERSION,
        )
    return MigrationPlan(
        current_version=current_version,
        target_version=CURRENT_SCHEMA_VERSION,
        unique_timestamp_index=_has_unique_ts_index(conn, table_name),
        timestamp_not_null=_timestamp_is_not_null(conn, table_name),
        sensor_mapping_table=_sensor_mapping_table_is_current(conn),
        duplicate_rows_to_remove=_duplicate_row_count(conn, table_name),
        null_timestamp_rows=_null_timestamp_count(conn, table_name),
    )


def inspect_database_migration(
    db_path: str,
    table_name: str = _DEFAULT_TABLE_NAME,
) -> MigrationPlan:
    with closing(_connect_database(db_path, read_only=True)) as conn:
        return _inspect_database_migration_connection(conn, table_name)


def _default_backup_path(db_path: str) -> str:
    source = Path(db_path)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return str(
        source.with_name(
            f"{source.stem}.pre-migration-{timestamp}{source.suffix or '.db'}",
        ),
    )


def _create_migration_audit_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_quote_identifier(_MIGRATION_AUDIT_TABLE)} (
            id INTEGER PRIMARY KEY,
            migrated_at TEXT NOT NULL,
            source_rowid INTEGER NOT NULL,
            timestamp TEXT,
            row_json TEXT NOT NULL
        )
        """,
    )


def _backfill_sensor_mappings(
    conn: sqlite3.Connection,
    table_name: str,
) -> None:
    """Register legacy physical columns so metadata remains discoverable."""
    mapped_columns = {
        str(row[0]).casefold()
        for row in conn.execute(
            f"SELECT column_name FROM {_quote_identifier(_SENSOR_MAPPING_TABLE)}",
        ).fetchall()
    }
    for column_name in _table_columns(conn, table_name).values():
        if (
            column_name.casefold() == _TS_COL.casefold()
            or column_name.casefold() in mapped_columns
        ):
            continue
        conn.execute(
            f"INSERT INTO {_quote_identifier(_SENSOR_MAPPING_TABLE)} "
            "(raw_name, column_name) VALUES (?, ?)",
            (column_name, column_name),
        )
        mapped_columns.add(column_name.casefold())


def _require_sensor_mapping_table(conn: sqlite3.Connection) -> None:
    """Create or transactionally repair the sensor mapping contract."""
    if _sensor_mapping_table_is_current(conn):
        return
    if not _table_exists_on_connection(conn, _SENSOR_MAPPING_TABLE):
        _create_sensor_mapping_table(conn)
        return

    temporary_table = f"{_SENSOR_MAPPING_TABLE}__migration"
    if _table_exists_on_connection(conn, temporary_table):
        msg = f"Temporary migration table already exists: {temporary_table}"
        raise sqlite3.OperationalError(msg)
    existing_columns = _table_columns(conn, _SENSOR_MAPPING_TABLE)
    required_columns = {"raw_name", "column_name"}
    if not required_columns.issubset(existing_columns):
        missing_columns = ", ".join(sorted(required_columns - existing_columns.keys()))
        msg = (
            f"Cannot repair {_SENSOR_MAPPING_TABLE}: missing required column(s) "
            f"{missing_columns}"
        )
        raise sqlite3.OperationalError(msg)
    conn.execute(
        f"ALTER TABLE {_quote_identifier(_SENSOR_MAPPING_TABLE)} "
        f"RENAME TO {_quote_identifier(temporary_table)}",
    )
    _create_sensor_mapping_table(conn)
    label_expression = (
        _quote_identifier(existing_columns["label"])
        if "label" in existing_columns
        else "NULL"
    )
    unit_expression = (
        _quote_identifier(existing_columns["unit"])
        if "unit" in existing_columns
        else "NULL"
    )
    # A malformed legacy table may lack both required constraints. In rowid
    # order, keep the first non-null row accepted for each raw name and each
    # case-insensitive physical column.
    conn.execute(
        f"INSERT OR IGNORE INTO {_quote_identifier(_SENSOR_MAPPING_TABLE)} "
        "(raw_name, column_name, label, unit) "
        f"SELECT {_quote_identifier(existing_columns['raw_name'])}, "
        f"{_quote_identifier(existing_columns['column_name'])}, "
        f"{label_expression}, {unit_expression} "
        f"FROM {_quote_identifier(temporary_table)} "
        f"WHERE {_quote_identifier(existing_columns['raw_name'])} IS NOT NULL "
        f"AND {_quote_identifier(existing_columns['column_name'])} IS NOT NULL "
        "ORDER BY rowid",
    )
    conn.execute(f"DROP TABLE {_quote_identifier(temporary_table)}")


def _replace_unique_ts_index(conn: sqlite3.Connection, table_name: str) -> None:
    index_name = _unique_ts_index_name(table_name)
    conn.execute(f"DROP INDEX IF EXISTS {_quote_identifier(index_name)}")
    conn.execute(_unique_ts_index_sql(table_name))


def _require_timestamp_column(
    conn: sqlite3.Connection,
    table_name: str,
) -> None:
    """Rebuild the application table with a NOT NULL timestamp column."""
    temporary_table = f"{table_name}__aw2sqlite_migration"
    if _table_exists_on_connection(conn, temporary_table):
        msg = f"Temporary migration table already exists: {temporary_table}"
        raise sqlite3.OperationalError(msg)

    schema_statements = [
        str(row[0])
        for row in conn.execute(
            "SELECT sql FROM sqlite_schema "
            "WHERE type IN ('index', 'trigger') "
            "AND tbl_name = ? AND sql IS NOT NULL "
            "ORDER BY type, name",
            (table_name,),
        ).fetchall()
    ]

    sensor_columns = [
        column_name
        for column_name in _table_columns(conn, table_name).values()
        if column_name.casefold() != _TS_COL.casefold()
    ]
    conn.execute(
        f"CREATE TABLE {_quote_identifier(temporary_table)} ("
        f"{_quote_identifier(_TS_COL)} TIMESTAMP NOT NULL "
        f"DEFAULT ({_SQLITE_TS_DEFAULT}))",
    )
    for column_name in sensor_columns:
        conn.execute(
            f"ALTER TABLE {_quote_identifier(temporary_table)} "
            f"ADD COLUMN {_quote_identifier(column_name)} REAL",
        )

    columns = [_TS_COL, *sensor_columns]
    column_list = ", ".join(_quote_identifier(column) for column in columns)
    conn.execute(
        f"INSERT INTO {_quote_identifier(temporary_table)} ({column_list}) "
        f"SELECT {column_list} FROM {_quote_identifier(table_name)}",
    )
    conn.execute(f"DROP TABLE {_quote_identifier(table_name)}")
    conn.execute(
        f"ALTER TABLE {_quote_identifier(temporary_table)} "
        f"RENAME TO {_quote_identifier(table_name)}",
    )
    for schema_statement in schema_statements:
        conn.execute(schema_statement)


def _require_plan_without_null_timestamps(
    plan: MigrationPlan,
    backup_path: str,
) -> None:
    if plan.null_timestamp_rows:
        raise NullTimestampMigrationError(plan.null_timestamp_rows, backup_path)


def _require_rowid(value: object) -> int:
    if isinstance(value, int):
        return value
    msg = f"Expected integer SQLite rowid, got {type(value).__name__}"
    raise TypeError(msg)


def _record_conflicting_rows(
    conn: sqlite3.Connection,
    rows: list[tuple[object, ...]],
    columns: list[str],
) -> None:
    migrated_at = datetime.now(UTC).isoformat()
    for row in rows:
        source_rowid = _require_rowid(row[0])
        values = dict(zip(columns, row[1:], strict=True))
        timestamp = next(
            (
                value
                for column_name, value in values.items()
                if column_name.casefold() == _TS_COL.casefold()
            ),
            None,
        )
        conn.execute(
            f"INSERT INTO {_quote_identifier(_MIGRATION_AUDIT_TABLE)} "
            "(migrated_at, source_rowid, timestamp, row_json) VALUES (?, ?, ?, ?)",
            (
                migrated_at,
                source_rowid,
                timestamp,
                json.dumps(values, default=str, sort_keys=True),
            ),
        )


def _deduplicate_timestamps(
    conn: sqlite3.Connection,
    table_name: str,
) -> int:
    _create_migration_audit_table(conn)
    columns = list(_table_columns(conn, table_name).values())
    duplicate_timestamps = [
        row[0]
        for row in conn.execute(
            f"SELECT {_quote_identifier(_TS_COL)} "
            f"FROM {_quote_identifier(table_name)} "
            f"WHERE {_quote_identifier(_TS_COL)} IS NOT NULL "
            f"GROUP BY {_quote_identifier(_TS_COL)} HAVING COUNT(*) > 1",
        ).fetchall()
    ]

    removed_rows = 0
    selected_columns = ", ".join(_quote_identifier(column) for column in columns)
    assignments = ", ".join(f"{_quote_identifier(column)} = ?" for column in columns)

    for timestamp in duplicate_timestamps:
        rows = conn.execute(
            f"SELECT rowid, {selected_columns} "
            f"FROM {_quote_identifier(table_name)} "
            f"WHERE {_quote_identifier(_TS_COL)} = ? ORDER BY rowid",
            (timestamp,),
        ).fetchall()
        _record_conflicting_rows(conn, rows, columns)
        keep_rowid = _require_rowid(rows[0][0])
        merged_row = [
            next(
                (
                    row[column_index]
                    for row in reversed(rows)
                    if row[column_index] is not None
                ),
                None,
            )
            for column_index in range(1, len(columns) + 1)
        ]
        conn.execute(
            f"UPDATE {_quote_identifier(table_name)} SET {assignments} WHERE rowid = ?",
            [*merged_row, keep_rowid],
        )
        duplicate_rowids = [(_require_rowid(row[0]),) for row in rows[1:]]
        conn.executemany(
            f"DELETE FROM {_quote_identifier(table_name)} WHERE rowid = ?",
            duplicate_rowids,
        )
        removed_rows += len(duplicate_rowids)

    return removed_rows


def migrate_database(
    db_path: str,
    *,
    backup_path: str | None = None,
    table_name: str = _DEFAULT_TABLE_NAME,
) -> MigrationResult:
    """Back up and explicitly migrate a legacy database."""
    with closing(_connect_database(db_path, read_only=False)) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            plan = _inspect_database_migration_connection(conn, table_name)
            if not plan.required:
                conn.rollback()
                return MigrationResult(None, 0, CURRENT_SCHEMA_VERSION)

            resolved_backup_path = backup_path or _default_backup_path(db_path)
            # backup_database returns only after the file and parent directory
            # have been flushed to durable storage.
            backup_database(db_path, resolved_backup_path)
            _require_plan_without_null_timestamps(plan, resolved_backup_path)

            _require_sensor_mapping_table(conn)
            _backfill_sensor_mappings(conn, table_name)
            removed_rows = 0
            if not _has_unique_ts_index(conn, table_name):
                try:
                    _replace_unique_ts_index(conn, table_name)
                except (sqlite3.IntegrityError,):
                    removed_rows = _deduplicate_timestamps(conn, table_name)
                    _replace_unique_ts_index(conn, table_name)
            if not plan.timestamp_not_null:
                _require_timestamp_column(conn, table_name)
                conn.execute(_unique_ts_index_sql(table_name))
            conn.execute(f"PRAGMA user_version = {CURRENT_SCHEMA_VERSION}")
            conn.commit()
        except (BaseException,):
            conn.rollback()
            raise

    return MigrationResult(
        resolved_backup_path,
        removed_rows,
        CURRENT_SCHEMA_VERSION,
    )
