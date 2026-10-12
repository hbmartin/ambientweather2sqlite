"""Serialize HTTP requests to each receiver within one process."""

import ipaddress
import socket
from threading import Lock
from urllib.parse import urlsplit
from weakref import WeakValueDictionary

from . import mureq

_REGISTRY_LOCK = Lock()
_RECEIVER_LOCKS: WeakValueDictionary[str, Lock] = WeakValueDictionary()


def _receiver_address(url: str) -> str:
    host = urlsplit(url).hostname or url
    try:
        return str(ipaddress.ip_address(host))
    except (ValueError,):  # noqa: B013
        try:
            addresses = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
        except (OSError,):  # noqa: B013
            return host.casefold()
        return str(ipaddress.ip_address(addresses[0][4][0]))


def _receiver_lock(url: str) -> Lock:
    address = _receiver_address(url)
    with _REGISTRY_LOCK:
        request_lock = _RECEIVER_LOCKS.get(address)
        if request_lock is None:
            request_lock = Lock()
            _RECEIVER_LOCKS[address] = request_lock
        return request_lock


def station_get(
    url: str,
    *,
    auto_retry: bool = False,
    timeout: float | None = None,
) -> str:
    """Fetch one page, serializing requests and retries to the same receiver."""
    with _receiver_lock(url):
        if timeout is None:
            return mureq.get(url, auto_retry=auto_retry)
        return mureq.get(url, auto_retry=auto_retry, timeout=timeout)
