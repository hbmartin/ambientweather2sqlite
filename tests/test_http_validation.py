"""Pure URL parsing and port-boundary invariants."""

import sys
from http.client import HTTPException
from unittest import TestCase
from unittest.mock import patch
from urllib.parse import urlsplit

from hypothesis import given
from hypothesis import strategies as st

from ambientweather2sqlite import http_validation
from ambientweather2sqlite.http_validation import (
    UnsupportedHttpSchemeError,
    normalized_http_host,
    parse_http_url,
    validate_port,
)
from ambientweather2sqlite.station_http import station_get

_MAX_TCP_PORT = 65535


class TestHttpValidation(TestCase):
    def test_multicolon_ports_keep_complete_nonnumeric_diagnostics(self) -> None:
        for host in (
            "station.local",
            "user:password@station.local",
            "[::1]",
            "[fe80::1%ETH0]",
        ):
            for port in ("abc:70000", "80:70000", ":70000"):
                url = f"http://{host}:{port}/"
                diagnostic = f"Port could not be cast to integer value as {port!r}"
                with self.subTest(host=host, port=port):
                    with self.assertRaises(ValueError) as raised:
                        parse_http_url(url)
                    self.assertEqual(str(raised.exception), diagnostic)
                    with (
                        patch("ambientweather2sqlite.mureq.get") as get,
                        patch("socket.getaddrinfo") as dns,
                        self.assertRaises(HTTPException) as request_error,
                    ):
                        station_get(url, auto_retry=True, timeout=1.5)
                    self.assertEqual(
                        str(request_error.exception),
                        f"Invalid receiver URL: {diagnostic}",
                    )
                    self.assertIsInstance(request_error.exception.__cause__, ValueError)
                    self.assertEqual(str(request_error.exception.__cause__), diagnostic)
                    get.assert_not_called()
                    dns.assert_not_called()

    def test_excessive_valid_port_padding_retains_standard_parser_behavior(
        self,
    ) -> None:
        digits = sys.get_int_max_str_digits() or sys.int_info.default_max_str_digits
        url = "http://station.local:" + "0" * (digits + 1) + "80/"
        if sys.get_int_max_str_digits():
            with self.assertRaises(ValueError) as standard_error:
                _ = urlsplit(url).port
            with self.assertRaises(ValueError) as validation_error:
                parse_http_url(url)
            self.assertEqual(
                str(validation_error.exception), str(standard_error.exception)
            )
        else:
            self.assertEqual(parse_http_url(url).port, 80)

    def test_numeric_port_range_errors_use_the_supported_range(self) -> None:
        for host in ("station.local", "[::1]", "user:password@station.local"):
            for port in ("0", "65536", "00065536", "9" * 5000):
                with self.subTest(host=host, port=port[:20]):
                    with self.assertRaisesRegex(
                        ValueError, "^URL port must be between 1 and 65535$"
                    ):
                        parse_http_url(f"http://{host}:{port}/")

    def test_nonnumeric_ports_keep_distinct_diagnostics(self) -> None:
        for port in ("invalid", "-1", "+1", "\uff11\uff12"):
            with self.subTest(port=port):
                with self.assertRaisesRegex(ValueError, "Port could not be cast"):
                    parse_http_url(f"http://station.local:{port}/")

    def test_missing_and_unsupported_schemes_have_a_specific_error_type(self) -> None:
        for url in ("station.local/live", "ftp://station.local/", "//station.local/"):
            with self.subTest(url=url):
                with self.assertRaises(UnsupportedHttpSchemeError):
                    parse_http_url(url)
        with self.assertRaises(ValueError) as raised:
            parse_http_url("http://station local/")
        self.assertNotIsInstance(raised.exception, UnsupportedHttpSchemeError)

    def test_requests_parse_and_normalize_the_hostname_once(self) -> None:
        url = "HtTp://BÜCHER.local:8080/live?raw=1#top"
        with (
            patch("socket.getaddrinfo") as dns,
            patch("ambientweather2sqlite.mureq.get", return_value="page") as get,
            patch(
                "ambientweather2sqlite.http_validation.urlsplit",
                wraps=http_validation.urlsplit,
            ) as parse,
            patch(
                "ambientweather2sqlite.http_validation._normalized_host",
                wraps=http_validation._normalized_host,  # noqa: SLF001
            ) as normalize,
        ):
            self.assertEqual(station_get(url, auto_retry=True, timeout=1.5), "page")
        parse.assert_called_once_with(url)
        normalize.assert_called_once_with("bücher.local", bracketed=False)
        get.assert_called_once_with(url, auto_retry=True, timeout=1.5)
        dns.assert_not_called()

    def test_normalized_hosts_preserve_ipv6_scopes_and_idna_equivalence(self) -> None:
        for url, expected in (
            ("HTTP://BÜCHER.local/", "xn--bcher-kva.local"),
            ("https://XN--BCHER-KVA.local:8080/", "xn--bcher-kva.local"),
            ("http://127.0.0.1/", "127.0.0.1"),
            ("http://[2001:0db8::1]/", "2001:db8::1"),
            ("http://[fe80:0000::1%ETH0]/", "fe80::1%ETH0"),
        ):
            with self.subTest(url=url), patch("socket.getaddrinfo") as dns:
                self.assertEqual(normalized_http_host(url), expected)
                dns.assert_not_called()

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
        for scheme in ("http", "https", "HTTP", "hTtPs"):
            with self.subTest(scheme=scheme):
                self.assertIsNone(parse_http_url(f"{scheme}://station.local/").port)
                self.assertIsNone(parse_http_url(f"{scheme}://station.local:/").port)

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
