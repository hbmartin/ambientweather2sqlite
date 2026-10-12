"""Serialize HTTP requests by normalized receiver hostname within one process."""

import ipaddress
from http.client import HTTPException
from threading import Lock
from urllib.parse import urlsplit
from weakref import WeakValueDictionary

from . import mureq

_REGISTRY_LOCK = Lock()
_RECEIVER_LOCKS: WeakValueDictionary[str, Lock] = WeakValueDictionary()


def _receiver_address(url: str) -> str:
    parsed = urlsplit(url)
    host = parsed.hostname
    # Accessing port validates malformed and out-of-range port strings.
    _ = parsed.port
    if host is None:
        msg = "Receiver URL is missing a hostname"
        raise ValueError(msg)
    try:
        return str(ipaddress.ip_address(host))
    except (ValueError,):  # noqa: B013
        return host.encode("idna").decode("ascii").lower()


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
    """Fetch one page, serializing requests and retries to the same host key."""
    try:
        with _receiver_lock(url):
            if timeout is None:
                return mureq.get(url, auto_retry=auto_retry)
            return mureq.get(url, auto_retry=auto_retry, timeout=timeout)
    except (ValueError,) as error:  # noqa: B013
        msg = f"Invalid receiver URL: {error}"
        raise HTTPException(msg) from error
