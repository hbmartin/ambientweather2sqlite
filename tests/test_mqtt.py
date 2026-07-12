import socket
import threading
from unittest import TestCase

from ambientweather2sqlite.models import MqttConfig
from ambientweather2sqlite.mqtt import (
    MqttError,
    _connect_packet,
    _encode_remaining_length,
    _encode_string,
    _publish_packet,
    _read_connack,
    publish,
    publish_message,
)


class TestPacketEncoding(TestCase):
    def test_encode_remaining_length_single_byte(self):
        self.assertEqual(_encode_remaining_length(0), b"\x00")
        self.assertEqual(_encode_remaining_length(127), b"\x7f")

    def test_encode_remaining_length_multi_byte(self):
        self.assertEqual(_encode_remaining_length(128), b"\x80\x01")
        self.assertEqual(_encode_remaining_length(16383), b"\xff\x7f")

    def test_encode_string_prefixes_length(self):
        self.assertEqual(_encode_string("MQTT"), b"\x00\x04MQTT")

    def test_connect_packet_without_credentials(self):
        config = MqttConfig(host="broker", topic="t", client_id="client-1")

        packet = _connect_packet(config)

        self.assertEqual(packet[0], 0x10)
        self.assertIn(b"\x00\x04MQTT", packet)
        self.assertIn(b"client-1", packet)
        # Connect flags: clean session only
        flags_index = packet.index(b"\x00\x04MQTT") + 6 + 1
        self.assertEqual(packet[flags_index], 0x02)

    def test_connect_packet_with_credentials(self):
        config = MqttConfig(
            host="broker",
            topic="t",
            username="user",
            password="pass",
        )

        packet = _connect_packet(config)

        flags_index = packet.index(b"\x00\x04MQTT") + 6 + 1
        self.assertEqual(packet[flags_index], 0x02 | 0x80 | 0x40)
        self.assertIn(b"\x00\x04user", packet)
        self.assertIn(b"\x00\x04pass", packet)

    def test_connect_packet_rejects_password_without_username(self):
        config = MqttConfig(host="broker", topic="t", password="pass")

        with self.assertRaises(MqttError):
            _connect_packet(config)

    def test_publish_packet_contains_topic_and_payload(self):
        packet = _publish_packet("weather/obs", b'{"outTemp": 72.5}')

        self.assertEqual(packet[0], 0x30)
        self.assertIn(b"weather/obs", packet)
        self.assertIn(b'{"outTemp": 72.5}', packet)

    def test_publish_packet_sets_retain_flag(self):
        packet = _publish_packet("t", b"{}", retain=True)

        self.assertEqual(packet[0], 0x31)


class _FragmentedSocket:
    def __init__(self, chunks):
        self.chunks = list(chunks)

    def recv(self, size: int) -> bytes:
        if not self.chunks:
            return b""
        chunk = self.chunks.pop(0)
        return chunk[:size]


class TestConnackReading(TestCase):
    def test_read_connack_accepts_fragmented_response(self):
        sock = _FragmentedSocket([b"\x20", b"\x02\x00", b"\x00"])

        _read_connack(sock)


class _FakeBroker:
    """Accept one MQTT connection and record everything sent after CONNACK."""

    def __init__(self, *, connack_code: int = 0):
        self.connack_code = connack_code
        self.received = b""
        self._server = socket.create_server(("127.0.0.1", 0))
        self.port = self._server.getsockname()[1]
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        conn, _ = self._server.accept()
        with conn:
            conn.settimeout(2.0)
            fixed_header = conn.recv(1)
            if not fixed_header:
                return
            remaining = self._read_remaining_length(conn)
            self._recv_exact(conn, remaining)
            conn.sendall(bytes([0x20, 0x02, 0x00, self.connack_code]))
            try:
                while chunk := conn.recv(4096):
                    self.received += chunk
            except TimeoutError:
                pass
        self._server.close()

    def _read_remaining_length(self, conn: socket.socket) -> int:
        multiplier = 1
        value = 0
        while True:
            encoded = self._recv_exact(conn, 1)
            digit = encoded[0]
            value += (digit & 0x7F) * multiplier
            if digit & 0x80 == 0:
                return value
            multiplier *= 128

    def _recv_exact(self, conn: socket.socket, length: int) -> bytes:
        data = bytearray()
        while len(data) < length:
            chunk = conn.recv(length - len(data))
            if not chunk:
                break
            data.extend(chunk)
        return bytes(data)

    def join(self) -> None:
        self._thread.join(timeout=5)


class TestPublish(TestCase):
    def test_publish_sends_publish_and_disconnect(self):
        broker = _FakeBroker()
        config = MqttConfig(host="127.0.0.1", port=broker.port, topic="weather/obs")

        publish(config, '{"outTemp": 72.5}')
        broker.join()

        self.assertEqual(broker.received[0], 0x30)
        self.assertIn(b"weather/obs", broker.received)
        self.assertIn(b'{"outTemp": 72.5}', broker.received)
        self.assertEqual(broker.received[-2:], bytes([0xE0, 0x00]))

    def test_publish_message_sends_retained_publish_to_topic(self):
        broker = _FakeBroker()
        config = MqttConfig(host="127.0.0.1", port=broker.port, topic="weather/obs")

        publish_message(
            config,
            "homeassistant/sensor/aw2sqlite/outTemp/config",
            '{"name": "Outdoor Temperature"}',
            retain=True,
        )
        broker.join()

        self.assertEqual(broker.received[0], 0x31)
        self.assertIn(b"homeassistant/sensor/aw2sqlite/outTemp/config", broker.received)
        self.assertNotIn(b"weather/obs", broker.received)
        self.assertEqual(broker.received[-2:], bytes([0xE0, 0x00]))

    def test_publish_raises_when_broker_refuses(self):
        broker = _FakeBroker(connack_code=5)
        config = MqttConfig(host="127.0.0.1", port=broker.port, topic="t")

        with self.assertRaises(MqttError) as ctx:
            publish(config, "{}")

        self.assertIn("not authorized", str(ctx.exception))

    def test_publish_raises_oserror_when_broker_unreachable(self):
        # Port 1 on localhost should refuse the connection.
        config = MqttConfig(host="127.0.0.1", port=1, topic="t")

        with self.assertRaises(OSError):
            publish(config, "{}")
