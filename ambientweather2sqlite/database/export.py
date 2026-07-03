"""Export observations to CSV or JSON."""

from __future__ import annotations

import csv
import json
from contextlib import closing
from typing import TYPE_CHECKING

from .connection import _DEFAULT_TABLE_NAME, _TS_COL, _connect_database

if TYPE_CHECKING:
    import sqlite3
    from typing import TextIO

EXPORT_FORMATS = ("csv", "json")


def _observation_cursor(
    conn: sqlite3.Connection,
    start: str | None,
    end: str | None,
) -> sqlite3.Cursor:
    conditions = []
    params: list[str] = []
    if start is not None:
        conditions.append(f"{_TS_COL} >= ?")
        params.append(start)
    if end is not None:
        conditions.append(f"{_TS_COL} < ?")
        params.append(end)
    where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"SELECT * FROM {_DEFAULT_TABLE_NAME}{where_clause} ORDER BY {_TS_COL}"
    return conn.cursor().execute(query, params)


def _export_csv(cursor: sqlite3.Cursor, output: TextIO) -> int:
    writer = csv.writer(output)
    writer.writerow([column[0] for column in cursor.description])
    row_count = 0
    for row in cursor:
        writer.writerow(row)
        row_count += 1
    return row_count


def _export_json(cursor: sqlite3.Cursor, output: TextIO) -> int:
    columns = [column[0] for column in cursor.description]
    output.write("[")
    row_count = 0
    for row in cursor:
        separator = "\n " if row_count == 0 else ",\n "
        output.write(separator + json.dumps(dict(zip(columns, row, strict=True))))
        row_count += 1
    output.write("\n]\n" if row_count else "]\n")
    return row_count


def export_observations(
    db_path: str,
    output: TextIO,
    *,
    fmt: str = "csv",
    start: str | None = None,
    end: str | None = None,
) -> int:
    """Write observations to `output` in the requested format, oldest first.

    `start` (inclusive) and `end` (exclusive) are compared as ISO timestamp
    strings against the stored UTC `ts` column, so date-only values like
    "2026-01-02" behave as midnight boundaries.

    Returns:
        Number of rows exported.

    """
    with closing(
        _connect_database(db_path, read_only=True),
    ) as conn:
        cursor = _observation_cursor(conn, start, end)
        if fmt == "json":
            return _export_json(cursor, output)
        return _export_csv(cursor, output)
