"""Pure HTTP URL validation shared by configuration and request boundaries."""

import ipaddress
from urllib.parse import SplitResult, urlsplit

_MAX_PORT = 65535
_ASCII_SPACE = 32
_ASCII_DELETE = 127


def validate_port(value: int | None, key: str) -> int | None:
    """Validate an optional TCP port without changing its value."""
    if value is not None and not (1 <= value <= _MAX_PORT):
        msg = f"{key} must be between 1 and {_MAX_PORT}"
        raise ValueError(msg)
    return value


def parse_http_url(url: str) -> SplitResult:
    """Validate HTTP URL structure without normalization or network access."""
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
        raise ValueError(msg)
    host = parsed.hostname
    if host is None:
        msg = "URL is missing a hostname"
        raise ValueError(msg)
    validate_port(parsed.port, "URL port")
    if parsed.netloc.rsplit("@", 1)[-1].startswith("["):
        ipaddress.IPv6Address(host)
    else:
        host.encode("idna")
    return parsed
