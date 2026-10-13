"""Pure URL parsing and port-boundary invariants."""

from unittest import TestCase
from unittest.mock import patch

from hypothesis import given
from hypothesis import strategies as st

from ambientweather2sqlite.http_validation import parse_http_url, validate_port

_MAX_TCP_PORT = 65535


class TestHttpValidation(TestCase):
    @given(port=st.integers())
    def test_port_validation_agrees_with_tcp_range(self, port: int) -> None:
        valid = 1 <= port <= _MAX_TCP_PORT
        if valid:
            self.assertEqual(validate_port(port, "port"), port)
            self.assertEqual(parse_http_url(f"http://station.local:{port}/").port, port)
        else:
            with self.assertRaisesRegex(ValueError, "port must be between 1 and 65535"):
                validate_port(port, "port")
            with self.assertRaises(ValueError):
                parse_http_url(f"http://station.local:{port}/")

    def test_omitted_ports_preserve_existing_defaults(self) -> None:
        self.assertIsNone(validate_port(None, "port"))
        for scheme in ("http", "https"):
            with self.subTest(scheme=scheme):
                self.assertIsNone(parse_http_url(f"{scheme}://station.local/").port)

    def test_parsing_preserves_parts_without_receiver_or_network_calls(self) -> None:
        url = "https://USER:password@BÜCHER.local:65535/live%20data?raw=%09#top"
        with (
            patch("socket.getaddrinfo") as dns,
            patch("ambientweather2sqlite.station_http._receiver_lock") as lock,
            patch("ambientweather2sqlite.mureq.get") as get,
        ):
            parts = parse_http_url(url)
        self.assertEqual(parts.geturl(), url)
        self.assertEqual(parts.hostname, "bücher.local")
        self.assertEqual(parts.port, 65535)
        for operation in (dns, lock, get):
            operation.assert_not_called()

    def test_bracketed_hosts_require_ipv6(self) -> None:
        with self.assertRaises(ValueError):
            parse_http_url("http://[v1.station]/")
