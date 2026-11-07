"""Automatically re-locate a weather station whose DHCP address changed."""

from __future__ import annotations

import ipaddress
import time
from dataclasses import dataclass, field
from threading import Lock
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from .arp import get_mac_address
from .exceptions import ReceiverIdentityMismatch
from .receiver_state import (
    legacy_receiver_state_path,
    load_receiver_identity,
    receiver_state_path,
    save_receiver_identity,
)
from .scanner import detect_local_subnet, probe_weather_station, scan_port80

if TYPE_CHECKING:
    import logging
    from pathlib import Path

    from .models import RediscoveryConfig

# Rescans sweep the subnet, so space them out even when the collection loop
# is failing (and retrying) every few seconds.
_RESCAN_COOLDOWN_SECONDS = 300.0
# Refuse to sweep networks larger than a /22 - a wider scan would block the
# collection loop for minutes on every cooldown expiry.
_MAX_SCAN_ADDRESSES = 1024


def _host_ip(url: str) -> str | None:
    """Return the URL's host when it is an IP literal, else None."""
    host = urlparse(url).hostname
    if host is None:
        return None
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return None
    return host


def _candidate_urls(subnet: str | None) -> list[str] | None:
    """Scan the subnet and return probe-confirmed station URLs, or None."""
    try:
        subnet = subnet or detect_local_subnet()
        if ipaddress.ip_network(subnet, strict=False).num_addresses > (
            _MAX_SCAN_ADDRESSES
        ):
            return None
        open_hosts = scan_port80(subnet)
    except (OSError, TypeError, ValueError):
        return None
    return [url for ip in open_hosts if (url := probe_weather_station(ip))]


def find_station_by_mac(
    mac_address: str | None,
    subnet: str | None = None,
) -> tuple[str, str | None] | None:
    """Scan the LAN and return (live_data_url, mac) for the matching station.

    Candidates are matched against ``mac_address`` using the OS neighbor
    cache, which each probe's TCP connection freshly populates. Before identity has been
    established the single-candidate heuristic applies. Once a MAC is known,
    only a positive identity match permits relocation.
    """
    candidates = _candidate_urls(subnet)
    if not candidates:
        return None

    macs = {
        url: mac
        for url in candidates
        if (ip := _host_ip(url)) and (mac := get_mac_address(ip))
    }
    if mac_address is not None:
        for url, mac in macs.items():
            if mac == mac_address:
                return url, mac
        # Once identity is known, an unreadable ARP cache is not evidence
        # that another device is ours. Retain the identity and retry later.
        return None

    if len(candidates) == 1:
        url = candidates[0]
        return url, macs.get(url)
    return None


@dataclass(slots=True)
class StationRediscovery:
    """Track consecutive fetch failures and relocate the station by MAC.

    ``record_failure`` triggers a subnet rescan once the configured threshold
    is reached, rate-limited by a cooldown. A relocation only updates
    ``current_url`` in memory - the config file is never modified.
    """

    config: RediscoveryConfig
    current_url: str
    logger: logging.Logger
    mac_address: str | None = None
    consecutive_failures: int = 0
    state_path: Path | None = None
    legacy_state_path: Path | None = None
    _last_scan: float = field(default=float("-inf"), init=False)
    _configured_url: str = field(init=False)
    _temporary_identity: bool = field(default=False, init=False)
    _identity_lock: Lock = field(default_factory=Lock, init=False, repr=False)
    _last_mismatch: tuple[str, str, str] | None = field(default=None, init=False)
    _last_mismatch_warning: float = field(default=float("-inf"), init=False)

    def __post_init__(self) -> None:
        """Load identity without changing the operator's configured URL."""
        self._configured_url = self.current_url
        if self.state_path is None or self.mac_address is not None:
            return
        self.mac_address = load_receiver_identity(
            self.state_path,
            self._configured_url,
            self.logger,
            legacy_path=self.legacy_state_path,
        )
        if self.mac_address is not None:
            self.logger.info("Restored receiver identity %s", self.mac_address)

    def _save_identity(self) -> None:
        if (
            self.state_path is None
            or self.mac_address is None
            or self._temporary_identity
        ):
            return
        try:
            save_receiver_identity(
                self.state_path, self._configured_url, self.mac_address
            )
        except (OSError,) as error:  # noqa: B013
            self.logger.warning("Cannot save receiver identity: %s", error)

    def verify_identity(self, url: str) -> None:
        """Check the actual fetched URL against a snapshot of session identity."""
        with self._identity_lock:
            expected = self.mac_address
        if expected is None or (ip := _host_ip(url)) is None:
            return
        mac = get_mac_address(ip)
        with self._identity_lock:
            if mac == expected:
                self._last_mismatch = None
            if mac is None or mac == expected:
                return
            mismatch = (url, expected, mac)
            now = time.monotonic()
            if (
                mismatch != self._last_mismatch
                or now - self._last_mismatch_warning >= _RESCAN_COOLDOWN_SECONDS
            ):
                self.logger.warning("%s", ReceiverIdentityMismatch(*mismatch))
                self._last_mismatch_warning = now
            self._last_mismatch = mismatch
        raise ReceiverIdentityMismatch(*mismatch)

    def record_success(self) -> None:
        """Reset the failure count and capture the station's MAC once."""
        self.consecutive_failures = 0
        if self.mac_address is not None:
            return
        if (ip := _host_ip(self.current_url)) and (mac := get_mac_address(ip)):
            with self._identity_lock:
                self.mac_address = mac
            self._save_identity()
            self.logger.info("Captured station MAC %s for %s", mac, ip)

    def record_failure(self) -> str | None:
        """Count a failed fetch, returning the new URL if the station moved."""
        self.consecutive_failures += 1
        now = time.monotonic()
        if (
            not self.config.enabled
            or self.consecutive_failures < self.config.failure_threshold
            or now - self._last_scan < _RESCAN_COOLDOWN_SECONDS
        ):
            return None
        self._last_scan = now
        return self._rescan()

    def _rescan(self) -> str | None:
        found = find_station_by_mac(self.mac_address)
        if found is None:
            self.logger.info(
                "Rescan found no matching weather station; keeping %s",
                self.current_url,
            )
            return None
        new_url, new_mac = found
        if new_url == self.current_url:
            self.logger.info(
                "Rescan found the station still at %s",
                self.current_url,
            )
            return None
        matched_by = (
            f"MAC {new_mac}"
            if new_mac is not None and new_mac == self.mac_address
            else "single-station fallback"
        )
        self.logger.warning(
            "Weather station moved: switching from %s to %s (matched by %s); "
            "config file left unchanged",
            self.current_url,
            new_url,
            matched_by,
        )
        with self._identity_lock:
            if self.mac_address is None:
                self._temporary_identity = True
            self.current_url = new_url
            # A fallback without evidence can learn a session-only MAC later.
            self.mac_address = new_mac
        self._save_identity()
        return new_url


def create_rediscovery(
    config: RediscoveryConfig,
    url: str,
    database_path: str,
    logger: logging.Logger,
) -> StationRediscovery | None:
    """Load a database's identity only when rediscovery is enabled."""
    if not config.enabled:
        return None
    return StationRediscovery(
        config,
        url,
        logger,
        state_path=receiver_state_path(database_path),
        legacy_state_path=legacy_receiver_state_path(database_path),
    )
