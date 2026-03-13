"""Network scanner to auto-detect AmbientWeather stations on the local subnet."""

import ipaddress
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.client import HTTPException

from ambientweather2sqlite import mureq
from ambientweather2sqlite.awparser import extract_values


def detect_local_subnet() -> str:
    """Detect the local /24 subnet by finding the machine's default route IP."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
    network = ipaddress.ip_network(f"{local_ip}/24", strict=False)
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
    open_hosts: list[str] = []

    def check_host(
        ip: ipaddress.IPv4Address | ipaddress.IPv6Address,
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
    except TimeoutError, HTTPException, OSError:
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
        subnet = detect_local_subnet()

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
