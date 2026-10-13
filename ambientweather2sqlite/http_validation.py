"""Pure HTTP URL validation shared by configuration and request boundaries."""

import ipaddress
from urllib.parse import SplitResult, urlsplit

_MAX_PORT = 65535
_MAX_PORT_DIGITS = len(str(_MAX_PORT))
_ASCII_SPACE = 32
_ASCII_DELETE = 127


class UnsupportedHttpSchemeError(ValueError):
    """The URL has no HTTP or HTTPS scheme."""


def validate_port(value: int | None, key: str) -> int | None:
    """Validate an optional TCP port without changing its value."""
    if value is not None and not (1 <= value <= _MAX_PORT):
        msg = f"{key} must be between 1 and {_MAX_PORT}"
        raise ValueError(msg)
    return value


def _raw_url_port(authority: str) -> str:
    if authority.startswith("["):
        authority = authority.partition("]")[2]
    return authority.partition(":")[2]


def _validate_url_port(parsed: SplitResult, authority: str) -> None:
    try:
        port = parsed.port
    except (ValueError,):  # noqa: B013
        raw_port = _raw_url_port(authority)
        if raw_port.isascii() and raw_port.isdecimal():
            significant = raw_port.lstrip("0") or "0"
            numeric_port = (
                int(significant)
                if len(significant) <= _MAX_PORT_DIGITS
                else _MAX_PORT + 1
            )
            validate_port(numeric_port, "URL port")
        raise
    validate_port(port, "URL port")


def _normalized_host(host: str, *, bracketed: bool) -> str:
    if bracketed:
        return str(ipaddress.IPv6Address(host))
    try:
        return str(ipaddress.ip_address(host))
    except (ValueError,):  # noqa: B013
        return host.encode("idna").decode("ascii").lower()


def _parse_http_url(url: str) -> tuple[SplitResult, str]:
    if any(
        character.isspace()
        or ord(character) < _ASCII_SPACE
        or ord(character) == _ASCII_DELETE
        for character in url
    ):
        msg = "URL must not contain whitespace or ASCII control characters"
        raise ValueError(msg)
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"}:
        msg = "URL must use http:// or https://"
        raise UnsupportedHttpSchemeError(msg)
    host = parsed.hostname
    if host is None:
        msg = "URL is missing a hostname"
        raise ValueError(msg)
    authority = parsed.netloc.rsplit("@", 1)[-1]
    _validate_url_port(parsed, authority)
    normalized_host = _normalized_host(host, bracketed=authority.startswith("["))
    return parsed, normalized_host


def parse_http_url(url: str) -> SplitResult:
    """Validate HTTP URL structure without rewriting it or accessing the network."""
    return _parse_http_url(url)[0]


def normalized_http_host(url: str) -> str:
    """Return a validated URL's canonical IP or lowercase IDNA hostname."""
    return _parse_http_url(url)[1]
