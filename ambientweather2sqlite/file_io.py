"""Atomic JSON replacement and durable filesystem updates."""

import json
import os
import stat
import tempfile
from pathlib import Path


def sync_parent(path: Path) -> None:
    """Sync a directory entry where the platform/filesystem supports it."""
    if os.name == "nt":
        return
    try:
        descriptor = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except (OSError,):  # noqa: B013
        # Match backup behavior on filesystems without directory syncing.
        pass


def sync_file_and_parent(path: Path) -> None:
    """Flush a file and its directory entry to durable storage."""
    with path.open("rb") as stream:
        os.fsync(stream.fileno())
    sync_parent(path)


def atomic_write_json(
    path: Path,
    document: object,
    *,
    mode: int | None = None,
    indent: int | None = None,
) -> None:
    """Replace JSON durably, preserving permissions unless mode is explicit."""
    if mode is None:
        try:
            mode = stat.S_IMODE(path.stat().st_mode)
        except (FileNotFoundError,):  # noqa: B013
            mode = 0o644
    descriptor, name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    temporary_path = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(document, stream, indent=indent)
            stream.flush()
            temporary_path.chmod(mode)
            os.fsync(stream.fileno())
        temporary_path.replace(path)
        sync_parent(path)
    finally:
        temporary_path.unlink(missing_ok=True)
