"""Database backup via SQLite's VACUUM INTO."""

from __future__ import annotations

import os
from contextlib import closing
from pathlib import Path

from .connection import _connect_database


def _sync_file_and_parent(path: Path) -> None:
    """Flush a file and its directory entry to durable storage."""
    with path.open("rb") as backup_file:
        os.fsync(backup_file.fileno())

    if os.name == "nt":
        # Windows cannot open a directory with os.open; fsyncing the file still
        # flushes its data and metadata through FlushFileBuffers.
        return
    try:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(path.parent, flags)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except (OSError,):
        # Some platforms and filesystems do not support syncing directories.
        pass


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
        _sync_file_and_parent(destination)
    except (BaseException,):
        # Remove the reserved/partial file so a retry is not blocked.
        destination.unlink(missing_ok=True)
        raise
    return destination.stat().st_size
