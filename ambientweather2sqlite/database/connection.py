"""Low-level SQLite connection and timestamp helpers."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

_DEFAULT_TABLE_NAME = "observations"
_TS_COL = "ts"
_SQLITE_BUSY_TIMEOUT_MS = 5_000
_SQLITE_MMAP_SIZE_BYTES = 268_435_456
_SQLITE_TS_DEFAULT = "STRFTIME('%Y-%m-%d %H:%M:%f', 'now')"


def _current_observation_timestamp() -> str:
    """Create a UTC timestamp with sub-second precision for inserts."""
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")


def _configure_connection(
    conn: sqlite3.Connection,
    *,
    read_only: bool,
    use_row_factory: bool = False,
) -> sqlite3.Connection:
    conn.execute(f"PRAGMA busy_timeout = {_SQLITE_BUSY_TIMEOUT_MS}")
    if use_row_factory:
        conn.row_factory = sqlite3.Row
    if not read_only:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute(f"PRAGMA mmap_size={_SQLITE_MMAP_SIZE_BYTES}")
    return conn


def _connect_database(
    db_path: str,
    *,
    read_only: bool,
    use_row_factory: bool = False,
) -> sqlite3.Connection:
    connect_target = f"file:{db_path}?mode=ro" if read_only else db_path
    conn = sqlite3.connect(
        connect_target,
        uri=read_only,
        timeout=_SQLITE_BUSY_TIMEOUT_MS / 1000,
    )
    return _configure_connection(
        conn,
        read_only=read_only,
        use_row_factory=use_row_factory,
    )


def _format_sqlite_timestamp(value: datetime) -> str:
    """Store timestamps as UTC naive strings in SQLite's text format.

    Callers should pass timezone-aware datetimes because the value is converted to
    UTC before its tzinfo is stripped for storage.
    """
    return value.astimezone(UTC).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _parse_stored_timestamp(value: str) -> datetime:
    """Parse stored timestamps and normalize them to UTC-aware datetimes.

    Naive timestamps are assumed to have been stored in UTC. Aware timestamps are
    converted to UTC before being returned.
    """
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
