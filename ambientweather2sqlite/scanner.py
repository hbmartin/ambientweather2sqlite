"""Network scanner to auto-detect AmbientWeather stations on the local subnet."""

import ipaddress
import re
import socket
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.client import HTTPException

from ambientweather2sqlite import mureq
from ambientweather2sqlite.awparser import extract_values


def _is_non_loopback_ipv4(address: object) -> bool:
    return isinstance(address, str) and not address.startswith("127.")


def _ipv4_candidate(sockaddr: object) -> str | None:
    if not isinstance(sockaddr, tuple) or not sockaddr:
        return None

    candidate = sockaddr[0]
    return candidate if _is_non_loopback_ipv4(candidate) else None


def _non_loopback_ipv4_candidates() -> list[str]:
    try:
        _, _, addresses = socket.gethostbyname_ex(socket.gethostname())
    except OSError:
        addresses = []
    candidates = [address for address in addresses if _is_non_loopback_ipv4(address)]
    if candidates:
        return candidates

    try:
        addrinfos = socket.getaddrinfo(
            socket.gethostname(),
            None,
            family=socket.AF_INET,
        )
    except OSError:
        addrinfos = []

    candidates.extend(
        candidate
        for address_info in addrinfos
        if (candidate := _ipv4_candidate(address_info[4])) is not None
    )

    return list(dict.fromkeys(candidates))


def _detect_local_ip() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except OSError:
        for candidate in _non_loopback_ipv4_candidates():
            return candidate

    msg = "Unable to detect local IPv4 address"
    raise OSError(msg)


def _prefix_length_from_ifconfig(output: str, local_ip: str) -> int | None:
    match = re.search(
        rf"inet {re.escape(local_ip)} .*?netmask (0x[0-9a-fA-F]+|\d+\.\d+\.\d+\.\d+)",
        output,
    )
    if match is None:
        return None

    netmask = match.group(1)
    netmask_ip = (
        str(ipaddress.IPv4Address(int(netmask, 16)))
        if netmask.startswith("0x")
        else netmask
    )
    return ipaddress.IPv4Network(f"0.0.0.0/{netmask_ip}").prefixlen


def _detect_prefix_length(local_ip: str) -> int | None:
    command_parsers = (
        (
            ["ip", "-o", "-f", "inet", "addr", "show"],
            lambda output: next(
                (
                    int(match.group(1))
                    for match in re.finditer(
                        rf"\binet {re.escape(local_ip)}/(\d+)\b",
                        output,
                    )
                ),
                None,
            ),
        ),
        (["ifconfig"], lambda output: _prefix_length_from_ifconfig(output, local_ip)),
    )

    for command, parser in command_parsers:
        try:
            result = subprocess.run(  # noqa: S603
                command,
                capture_output=True,
                check=True,
                text=True,
                timeout=1,
            )
        except (
            FileNotFoundError,
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
        ):
            continue

        if (prefix_length := parser(result.stdout)) is not None:
            return prefix_length

    return None


def detect_local_subnet() -> str:
    """Detect the local subnet by finding the machine's default route IP."""
    local_ip = _detect_local_ip()
    prefix_length = _detect_prefix_length(local_ip)
    if prefix_length is None:
        prefix_length = 24
    network = ipaddress.ip_network(f"{local_ip}/{prefix_length}", strict=False)
    return str(network)


def scan_port80(
    subnet: str,
    *,
    timeout: float = 0.5,
    workers: int = 100,
) -> list[str]:
    """Scan a subnet for hosts with TCP port 80 open.

    Args:
        subnet: CIDR subnet, e.g. "192.168.0.0/24"
        timeout: Socket connect timeout in seconds
        workers: Number of concurrent scan threads

    Returns:
        Sorted list of IPs with port 80 open

    """
    network = ipaddress.ip_network(subnet, strict=False)
    if not isinstance(network, ipaddress.IPv4Network):
        msg = "Only IPv4 subnets are supported"
        raise TypeError(msg)
    open_hosts: list[str] = []

    def check_host(
        ip: ipaddress.IPv4Address,
    ) -> str | None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            if s.connect_ex((str(ip), 80)) == 0:
                return str(ip)
        return None

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(check_host, ip) for ip in network.hosts()]
        open_hosts.extend(
            result for future in as_completed(futures) if (result := future.result())
        )

    return sorted(open_hosts, key=lambda ip: ipaddress.ip_address(ip))  # noqa: PLW0108


def probe_weather_station(ip: str) -> str | None:
    """Attempt to fetch live data from a potential weather station at the given IP.

    Returns:
        The live data URL if a station is found, None otherwise.

    """
    url = f"http://{ip}/livedata.htm"
    try:
        body = mureq.get(url, timeout=3)
        values = extract_values(body)
        if values:
            return url
    except (TimeoutError, HTTPException, OSError):
        pass
    return None


def scan_for_stations(subnet: str | None = None) -> list[str]:
    """Scan the local network for AmbientWeather stations.

    Args:
        subnet: CIDR subnet to scan. Auto-detected if None.

    Returns:
        List of live data URLs for discovered stations.

    """
    if subnet is None:
        try:
            subnet = detect_local_subnet()
        except (OSError, ValueError) as exc:
            print(f"Unable to auto-detect local subnet: {exc}")
            raise

    print(f"Scanning {subnet} for devices with port 80 open...")
    open_hosts = scan_port80(subnet)

    if not open_hosts:
        print("No devices with port 80 found.")
        return []

    print(f"Found {len(open_hosts)} device(s). Probing for weather stations...")
    stations: list[str] = []

    for ip in open_hosts:
        if url := probe_weather_station(ip):
            stations.append(url)
            print(f"  Found weather station at {url}")

    if not stations:
        print("No weather stations found.")

    return stations
