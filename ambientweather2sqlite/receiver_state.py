"""Persist and reset identity scoped to a configured receiver and database."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from .arp import normalize_mac
from .file_io import atomic_write_json, sync_parent

if TYPE_CHECKING:
    import logging


def receiver_state_path(database_path: str) -> Path:
    """Keep the database extension to avoid same-stem identity collisions."""
    path = Path(database_path)
    return path.with_name(f"{path.name}.receiver.json")


def legacy_receiver_state_path(database_path: str) -> Path:
    """Return the identity location used before extension-preserving names."""
    return Path(database_path).with_suffix(".receiver.json")


def _read_identity(path: Path, configured_url: str) -> str | None:
    state = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(state, dict):
        message = "expected an object"
        raise TypeError(message)
    if state.get("configured_url") != configured_url:
        return None
    raw_mac = state.get("mac_address")
    if not isinstance(raw_mac, str) or (mac := normalize_mac(raw_mac)) is None:
        message = "invalid MAC address"
        raise ValueError(message)
    return mac


def save_receiver_identity(path: Path, configured_url: str, mac: str) -> None:
    """Write only identities learned at an operator-configured address."""
    atomic_write_json(
        path, {"configured_url": configured_url, "mac_address": mac}, mode=0o600
    )


def load_receiver_identity(
    path: Path,
    configured_url: str,
    logger: logging.Logger,
    *,
    legacy_path: Path | None = None,
) -> str | None:
    """Prefer current state; migrate matching legacy state without losing it."""
    try:
        return _read_identity(path, configured_url)
    except (FileNotFoundError,):  # noqa: B013
        pass
    except (OSError, TypeError, ValueError) as error:
        logger.warning("Cannot load receiver identity: %s", error)
        return None
    if legacy_path is None:
        return None
    try:
        mac = _read_identity(legacy_path, configured_url)
    except (FileNotFoundError,):  # noqa: B013
        return None
    except (OSError, TypeError, ValueError) as error:
        logger.warning("Cannot load legacy receiver identity: %s", error)
        return None
    if mac is not None:
        try:
            save_receiver_identity(path, configured_url, mac)
            legacy_path.unlink()
            sync_parent(legacy_path)
        except (OSError,) as error:  # noqa: B013
            logger.warning("Cannot migrate receiver identity: %s", error)
    return mac


def reset_receiver_identity(database_path: str, configured_url: str) -> None:
    """Remove legacy state first so a partial reset cannot resurrect it."""
    legacy_path = legacy_receiver_state_path(database_path)
    try:
        state = json.loads(legacy_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        state = None
    if isinstance(state, dict) and state.get("configured_url") == configured_url:
        legacy_path.unlink(missing_ok=True)
        sync_parent(legacy_path)
    path = receiver_state_path(database_path)
    path.unlink(missing_ok=True)
    sync_parent(path)
