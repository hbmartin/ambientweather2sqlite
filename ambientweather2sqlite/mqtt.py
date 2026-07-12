"""Minimal MQTT 3.1.1 publisher over raw sockets.

Implements just enough of the protocol for fire-and-forget QoS 0 publishing:
CONNECT/CONNACK, PUBLISH, and DISCONNECT. This keeps the project free of
third-party dependencies while supporting the common Home Assistant / broker
ingestion path.
"""

from __future__ import annotations

import socket
import ssl
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import MqttConfig

_CONNECT = 0x10
_CONNACK = 0x20
_PUBLISH = 0x30
_RETAIN_FLAG = 0x01
_DISCONNECT = 0xE0
_PROTOCOL_LEVEL = 4  # MQTT 3.1.1
_CONNECT_TIMEOUT_SECONDS = 5.0
_CONNACK_LENGTH = 4
_CONNACK_ACCEPTED = 0

_CONNACK_ERRORS = {
    1: "unacceptable protocol version",
    2: "identifier rejected",
    3: "server unavailable",
    4: "bad user name or password",
    5: "not authorized",
}


class MqttError(Exception):
    """Raised when the broker rejects or aborts an MQTT exchange."""


class MqttProtocolError(MqttError):
    def __init__(self) -> None:
        super().__init__("Broker did not send a valid CONNACK")


class MqttConnectionRefusedError(MqttError):
    def __init__(self, reason: str) -> None:
        super().__init__(f"Broker refused connection: {reason}")


def _encode_remaining_length(length: int) -> bytes:
    """Encode the MQTT variable-length remaining-length field."""
    encoded = bytearray()
    while True:
        digit = length % 128
        length //= 128
        if length > 0:
            digit |= 0x80
        encoded.append(digit)
        if length == 0:
            return bytes(encoded)


def _encode_string(value: str) -> bytes:
    """Encode a UTF-8 string with its 2-byte length prefix."""
    data = value.encode("utf-8")
    return len(data).to_bytes(2) + data


def _connect_packet(config: MqttConfig) -> bytes:
    if config.password is not None and config.username is None:
        msg = "MQTT password requires username"
        raise MqttError(msg)

    connect_flags = 0x02  # clean session
    payload = _encode_string(config.client_id)
    if config.username is not None:
        connect_flags |= 0x80
        payload += _encode_string(config.username)
        if config.password is not None:
            connect_flags |= 0x40
            payload += _encode_string(config.password)

    keepalive_seconds = 60
    variable_header = (
        _encode_string("MQTT")
        + bytes([_PROTOCOL_LEVEL, connect_flags])
        + keepalive_seconds.to_bytes(2)
    )
    body = variable_header + payload
    return bytes([_CONNECT]) + _encode_remaining_length(len(body)) + body


def _publish_packet(topic: str, payload: bytes, *, retain: bool = False) -> bytes:
    fixed_header = _PUBLISH | _RETAIN_FLAG if retain else _PUBLISH
    body = _encode_string(topic) + payload
    return bytes([fixed_header]) + _encode_remaining_length(len(body)) + body


def _read_exact(sock: socket.socket, length: int) -> bytes:
    response = bytearray()
    while len(response) < length:
        chunk = sock.recv(length - len(response))
        if not chunk:
            break
        response.extend(chunk)
    return bytes(response)


def _read_connack(sock: socket.socket) -> None:
    response = _read_exact(sock, _CONNACK_LENGTH)
    if len(response) < _CONNACK_LENGTH or response[0] != _CONNACK:
        raise MqttProtocolError
    return_code = response[3]
    if return_code != _CONNACK_ACCEPTED:
        reason = _CONNACK_ERRORS.get(return_code, f"return code {return_code}")
        raise MqttConnectionRefusedError(reason)


def _open_socket(config: MqttConfig) -> socket.socket:
    sock = socket.create_connection(
        (config.host, config.port),
        timeout=_CONNECT_TIMEOUT_SECONDS,
    )
    if config.tls:
        context = ssl.create_default_context()
        sock = context.wrap_socket(sock, server_hostname=config.host)
    return sock


def publish_message(
    config: MqttConfig,
    topic: str,
    payload: str,
    *,
    retain: bool = False,
) -> None:
    """Publish a single message to a topic at QoS 0 and disconnect.

    Connecting per message keeps failure handling trivial for a once-a-minute
    publish cadence; a persistent session would save little and require
    keepalive and reconnect logic.

    Raises:
        MqttError: If the broker rejects the connection.
        OSError: On network failures (including timeouts).

    """
    with _open_socket(config) as sock:
        sock.sendall(_connect_packet(config))
        _read_connack(sock)
        sock.sendall(
            _publish_packet(topic, payload.encode("utf-8"), retain=retain),
        )
        sock.sendall(bytes([_DISCONNECT, 0]))


def publish(config: MqttConfig, payload: str) -> None:
    """Publish a single message to the configured topic at QoS 0.

    Raises:
        MqttError: If the broker rejects the connection.
        OSError: On network failures (including timeouts).

    """
    publish_message(config, config.topic, payload)
