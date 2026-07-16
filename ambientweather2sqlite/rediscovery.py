"""Automatically re-locate a weather station whose DHCP address changed."""

from __future__ import annotations

import ipaddress
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from .arp import get_mac_address
from .scanner import detect_local_subnet, probe_weather_station, scan_port80

if TYPE_CHECKING:
    import logging

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
    cache, which each probe's TCP connection freshly populates. Without any
    MAC evidence the single-candidate heuristic applies instead; with
    readable MACs that all differ, no station is returned - never silently
    switch to a different physical device.
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
        if macs:
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
    _last_scan: float = field(default=float("-inf"), init=False)

    def record_success(self) -> None:
        """Reset the failure count and capture the station's MAC once."""
        self.consecutive_failures = 0
        if self.mac_address is not None:
            return
        if (ip := _host_ip(self.current_url)) and (mac := get_mac_address(ip)):
            self.mac_address = mac
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
        self.current_url = new_url
        # A fallback match may not have MAC evidence; clearing lets the next
        # successful fetch re-capture the identity at the new address.
        self.mac_address = new_mac
        return new_url
