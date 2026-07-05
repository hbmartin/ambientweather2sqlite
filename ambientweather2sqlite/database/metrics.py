"""Database summary metrics and gap detection."""

from __future__ import annotations

from contextlib import closing
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import TYPE_CHECKING

from .connection import (
    _DEFAULT_TABLE_NAME,
    _TS_COL,
    _connect_database,
    _format_sqlite_timestamp,
    _is_timezone_aware,
    _parse_stored_timestamp,
)

if TYPE_CHECKING:
    from ambientweather2sqlite.models import DbMetrics, Observation, ObservationGap


def _normalize_current_time(now: datetime | None) -> datetime:
    if now is None:
        return datetime.now(UTC)
    if not _is_timezone_aware(now):
        return now.replace(tzinfo=UTC)
    return now.astimezone(UTC)


def query_db_metrics(db_path: str) -> DbMetrics:
    """Query database for summary metrics."""
    db_file = Path(db_path)
    file_size = db_file.stat().st_size if db_file.exists() else 0

    with closing(
        _connect_database(db_path, read_only=True, use_row_factory=True),
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) as cnt FROM {_DEFAULT_TABLE_NAME}")
        row_count = cursor.fetchone()["cnt"]

        cursor.execute(
            f"SELECT MIN({_TS_COL}) as earliest, MAX({_TS_COL}) as latest "
            f"FROM {_DEFAULT_TABLE_NAME}",
        )
        ts_row = cursor.fetchone()

        cursor.execute(f"PRAGMA table_info({_DEFAULT_TABLE_NAME})")
        column_count = len(cursor.fetchall())

    return {
        "row_count": row_count,
        "db_file_size_bytes": file_size,
        "earliest_ts": ts_row["earliest"],
        "latest_ts": ts_row["latest"],
        "column_count": column_count,
    }


def query_latest_timestamp(db_path: str) -> str | None:
    """Return the most recent observation timestamp, or None if empty."""
    with closing(
        _connect_database(db_path, read_only=True, use_row_factory=True),
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT MAX({_TS_COL}) as latest FROM {_DEFAULT_TABLE_NAME}",
        )
        row = cursor.fetchone()
        return row["latest"] if row else None


def query_latest_observation(db_path: str) -> Observation | None:
    """Return the most recent observation row as a dict, or None if empty."""
    with closing(
        _connect_database(db_path, read_only=True, use_row_factory=True),
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT * FROM {_DEFAULT_TABLE_NAME} ORDER BY {_TS_COL} DESC LIMIT 1",
        )
        row = cursor.fetchone()
        return dict(row) if row is not None else None


def query_observation_gaps(
    db_path: str,
    *,
    hours: int = 24,
    threshold_seconds: float = 180.0,
    now: datetime | None = None,
) -> list[ObservationGap]:
    """Find periods without observations within the last `hours` hours.

    A gap is any span between consecutive observations longer than
    `threshold_seconds`. A trailing gap (latest observation to now) is
    reported with `end` set to None, meaning the gap is ongoing.
    """
    current_time = _normalize_current_time(now)
    window_start = _format_sqlite_timestamp(current_time - timedelta(hours=hours))

    with closing(
        _connect_database(db_path, read_only=True),
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT {_TS_COL} FROM {_DEFAULT_TABLE_NAME} "
            f"WHERE {_TS_COL} >= ? ORDER BY {_TS_COL}",
            (window_start,),
        )
        timestamps: list[str] = [row[0] for row in cursor.fetchall()]
        cursor.execute(
            f"SELECT {_TS_COL} FROM {_DEFAULT_TABLE_NAME} "
            f"WHERE {_TS_COL} < ? ORDER BY {_TS_COL} DESC LIMIT 1",
            (window_start,),
        )
        if prev_row := cursor.fetchone():
            timestamps.insert(0, prev_row[0])

    gaps: list[ObservationGap] = []
    parsed = [(raw, _parse_stored_timestamp(raw)) for raw in timestamps]

    for (prev_raw, prev_dt), (cur_raw, cur_dt) in pairwise(parsed):
        duration = (cur_dt - prev_dt).total_seconds()
        if duration > threshold_seconds:
            gaps.append(
                {
                    "start": prev_raw,
                    "end": cur_raw,
                    "duration_seconds": duration,
                },
            )

    if parsed:
        last_raw, last_dt = parsed[-1]
        trailing = (current_time - last_dt).total_seconds()
        if trailing > threshold_seconds:
            gaps.append(
                {
                    "start": last_raw,
                    "end": None,
                    "duration_seconds": trailing,
                },
            )

    return gaps
