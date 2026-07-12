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
        FileExistsError: If dest_path already exists. The destination is
            reserved atomically, and a failed vacuum cleans it up so a
            retry is never blocked by a partial file.
        sqlite3.Error: On database errors (e.g. missing or corrupt source).

    """
    destination = Path(dest_path)
    try:
        # Atomically reserve the destination so a concurrent process
        # cannot claim the same path between a check and the vacuum.
        destination.touch(exist_ok=False)
    except FileExistsError:
        msg = f"Backup destination already exists: {dest_path}"
        raise FileExistsError(msg) from None
    try:
        with closing(_connect_database(db_path, read_only=True)) as conn:
            conn.execute("VACUUM INTO ?", (str(destination),))
    except BaseException:
        # Remove the reserved/partial file so a retry is not blocked.
        destination.unlink(missing_ok=True)
        raise
    return destination.stat().st_size
