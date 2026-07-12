"""Database backup via SQLite's VACUUM INTO."""

from __future__ import annotations

from contextlib import closing
from pathlib import Path

from .connection import _connect_database


def backup_database(db_path: str, dest_path: str) -> int:
    """Write a compacted, consistent copy of the database to dest_path.

    VACUUM INTO produces a single self-contained file even while the
    daemon holds the WAL open, unlike a raw file copy.

    Returns:
        The size of the backup file in bytes.

    Raises:
        FileExistsError: If dest_path already exists (VACUUM INTO
            refuses to overwrite, with a less helpful error).
        sqlite3.Error: On database errors (e.g. missing or corrupt source).

    """
    destination = Path(dest_path)
    if destination.exists():
        msg = f"Backup destination already exists: {dest_path}"
        raise FileExistsError(msg)
    with closing(_connect_database(db_path, read_only=True)) as conn:
        conn.execute("VACUUM INTO ?", (str(destination),))
    return destination.stat().st_size
