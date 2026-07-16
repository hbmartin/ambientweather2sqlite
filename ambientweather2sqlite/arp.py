"""Read a device's MAC address from the OS ARP/neighbor cache."""

import re
import subprocess
from pathlib import Path

_MAC_PATTERN = re.compile(r"\b([0-9A-Fa-f]{1,2}(?:[:-][0-9A-Fa-f]{1,2}){5})\b")
_MAC_OCTET_COUNT = 6
# All-zeros means an unresolved entry; all-ones is the broadcast address.
_INVALID_MACS = frozenset({"00:00:00:00:00:00", "ff:ff:ff:ff:ff:ff"})
_NEIGHBOR_COMMANDS: tuple[tuple[str, ...], ...] = (
    ("ip", "neigh", "show", "{ip}"),  # Linux (iproute2)
    ("arp", "-n", "{ip}"),  # macOS / BSD / Linux net-tools
    ("arp", "-a", "{ip}"),  # Windows
)
_PROC_NET_ARP = Path("/proc/net/arp")
_COMMAND_TIMEOUT_SECONDS = 1


def _normalize_mac(raw: str) -> str | None:
    """Return the MAC as lowercase colon-separated zero-padded octets."""
    octets = re.split(r"[:-]", raw)
    if len(octets) != _MAC_OCTET_COUNT:
        return None
    try:
        return ":".join(f"{int(octet, 16):02x}" for octet in octets)
    except ValueError:
        return None


def _mac_from_output(output: str) -> str | None:
    """Extract the first usable MAC address from command output."""
    for match in _MAC_PATTERN.finditer(output):
        if (mac := _normalize_mac(match.group(1))) and mac not in _INVALID_MACS:
            return mac
    return None


def _mac_from_command(command: list[str]) -> str | None:
    try:
        result = subprocess.run(  # noqa: S603
            command,
            capture_output=True,
            check=True,
            text=True,
            timeout=_COMMAND_TIMEOUT_SECONDS,
        )
    except (
        FileNotFoundError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        OSError,
    ):
        return None
    return _mac_from_output(result.stdout)


def _mac_from_proc_net_arp(ip: str) -> str | None:
    """Look up the IP in /proc/net/arp (Linux hosts without ip/arp binaries)."""
    try:
        lines = _PROC_NET_ARP.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in lines[1:]:  # first line is the column header
        columns = line.split()
        if len(columns) >= 4 and columns[0] == ip:  # noqa: PLR2004
            return _mac_from_output(columns[3])
    return None


def get_mac_address(ip: str) -> str | None:
    """Return the neighbor-cache MAC for ``ip``, or None if unavailable.

    The cache only holds entries for hosts the OS recently exchanged packets
    with, so callers should query right after connecting to the device.
    """
    for template in _NEIGHBOR_COMMANDS:
        command = [part.format(ip=ip) for part in template]
        if mac := _mac_from_command(command):
            return mac
    return _mac_from_proc_net_arp(ip)
