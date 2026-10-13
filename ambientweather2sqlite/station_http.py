"""Serialize HTTP requests by normalized receiver hostname within one process."""

from http.client import HTTPException
from threading import Lock
from weakref import WeakValueDictionary

from . import mureq
from .http_validation import normalized_http_host

_REGISTRY_LOCK = Lock()
_RECEIVER_LOCKS: WeakValueDictionary[str, Lock] = WeakValueDictionary()


def _receiver_lock(url: str) -> Lock:
    address = normalized_http_host(url)
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
        request_lock = _receiver_lock(url)
    except (ValueError,) as error:  # noqa: B013
        msg = f"Invalid receiver URL: {error}"
        raise HTTPException(msg) from error
    with request_lock:
        try:
            if timeout is None:
                return mureq.get(url, auto_retry=auto_retry)
            return mureq.get(url, auto_retry=auto_retry, timeout=timeout)
        except (ValueError,) as error:  # noqa: B013
            msg = f"Receiver request failed: {error}"
            raise HTTPException(msg) from error
