"""Regression coverage for configuration and receiver failure diagnostics."""

import json
import tempfile
from http.client import HTTPException
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from hypothesis import given
from hypothesis import strategies as st

from ambientweather2sqlite.alerts import AlertNotifier
from ambientweather2sqlite.cli.commands import main
from ambientweather2sqlite.configuration import load_config
from ambientweather2sqlite.daemon import _collect_forever, _fetch_receiver_page
from ambientweather2sqlite.models import AlertConfig
from ambientweather2sqlite.station_http import station_get

_INVALID_URLS = (
    "http://",
    "http:///livedata.htm",
    "http://[::1/livedata.htm",
    "http://[not-an-ip]/",
    "http://station.local:invalid/",
    "http://station.local:-1/",
    "http://station.local:0/",
    "http://station.local:65536/",
    "http://" + "a" * 64 + ".invalid/",
    "http://\u200d.invalid/",
    "http://station local/live",
    "http://sta\ttion.local/live",
    "http://station.local/\n",
    "http://station.local/\r",
    "http://station.local/\x00",
    "http://station.local/\x1f",
    "http://station.local/\x7f",
)
_SUPPORTED_URLS = (
    "HTTP://Observer.LOCAL/livedata.htm",
    "hTtPs://station.local:8080/live?raw=1#top",
    "http://Observer.LOCAL:8080/livedata.htm?raw=1#top",
    "https://127.0.0.1/livedata.htm",
    "http://[2001:0db8:0:0:0:0:0:1]:8080/live",
    "http://[fe80::1%eth0]/live",
    "http://BÜCHER.local/livedata.htm",
    "http://xn--bcher-kva.local/livedata.htm",
    "http://station.local:1/live",
    "https://station.local:65535/live",
    "https://user:password@station.local/live%20data?raw=%09#top",
)
_PAGE = '<input name="outTemp" disabled value="72.5">'


def _write_config(directory: str, url: str, extra: str = "") -> Path:
    path = Path(directory) / "config.toml"
    path.write_text(
        f'live_data_url = {json.dumps(url)}\ndatabase_path = "unused.db"\n' + extra,
        encoding="utf-8",
    )
    return path


class TestReceiverConfiguration(TestCase):
    def test_validation_diagnostics_keep_field_names_and_scheme_messages(self) -> None:
        cases = (
            ("FTP://station.local/", "must start with http:// or https://"),
            ("station.local/live", "must start with http:// or https://"),
            (
                "HTTP://station.local:0/",
                "is invalid: URL port must be between 1 and 65535",
            ),
            (
                "https://station.local:65536/",
                "is invalid: URL port must be between 1 and 65535",
            ),
            ("http://station.local:invalid/", "is invalid: Port could not be cast"),
        )
        for field in ("live_data_url", "alerts.webhook_url"):
            for url, diagnostic in cases:
                with (
                    self.subTest(field=field, url=url),
                    tempfile.TemporaryDirectory() as directory,
                ):
                    path = _write_config(
                        directory,
                        url if field == "live_data_url" else "http://station.local/",
                        f"[alerts]\nwebhook_url = {json.dumps(url)}\n"
                        if field == "alerts.webhook_url"
                        else "",
                    )
                    with self.assertRaises(ValueError) as raised:
                        load_config(path)
                    self.assertTrue(
                        str(raised.exception).startswith(f"{field} {diagnostic}")
                    )
                    self.assertIsInstance(raised.exception.__cause__, ValueError)

    def test_malformed_configuration_exits_before_startup_or_network(self) -> None:
        for url in (*_INVALID_URLS, "ftp://station.local/live", "station.local/live"):
            with self.subTest(url=url), tempfile.TemporaryDirectory() as directory:
                path = _write_config(directory, url)
                with (
                    patch("builtins.print") as output,
                    patch(
                        "ambientweather2sqlite.cli.commands._resolve_config",
                        return_value=path,
                    ),
                    patch(
                        "ambientweather2sqlite.cli.commands.create_database_if_not_exists"
                    ) as database,
                    patch("ambientweather2sqlite.cli.commands.start_daemon") as daemon,
                    patch("ambientweather2sqlite.station_http._receiver_lock") as lock,
                    patch("socket.getaddrinfo") as dns,
                    patch("ambientweather2sqlite.mureq.get") as get,
                    patch("ambientweather2sqlite.mureq.post") as post,
                    self.assertRaises(SystemExit) as exit_code,
                ):
                    main(["serve", "--config", str(path)])
                self.assertEqual(exit_code.exception.code, 1)
                self.assertIn("Configuration error", output.call_args.args[0])
                self.assertIn("live_data_url", output.call_args.args[0])
                for operation in (database, daemon, lock, dns, get, post):
                    operation.assert_not_called()

    def test_supported_urls_are_preserved_without_locks_or_network(self) -> None:
        for url in _SUPPORTED_URLS:
            with self.subTest(url=url), tempfile.TemporaryDirectory() as directory:
                path = _write_config(
                    directory, url, f"[alerts]\nwebhook_url = {json.dumps(url)}\n"
                )
                with (
                    patch("ambientweather2sqlite.station_http._receiver_lock") as lock,
                    patch("socket.getaddrinfo") as dns,
                    patch("ambientweather2sqlite.mureq.get") as get,
                ):
                    config = load_config(path)
                    self.assertEqual(config.live_data_url, url)
                    assert config.alerts is not None
                    self.assertEqual(config.alerts.webhook_url, url)
                lock.assert_not_called()
                dns.assert_not_called()
                get.assert_not_called()

    def test_malformed_webhooks_exit_before_startup_or_network(self) -> None:
        for url in (*_INVALID_URLS, "ftp://hooks.local/", "hooks.local/alert"):
            with self.subTest(url=url), tempfile.TemporaryDirectory() as directory:
                path = _write_config(
                    directory,
                    "http://station.local/",
                    f"[alerts]\nwebhook_url = {json.dumps(url)}\n",
                )
                with (
                    patch("builtins.print") as output,
                    patch(
                        "ambientweather2sqlite.cli.commands._resolve_config",
                        return_value=path,
                    ),
                    patch(
                        "ambientweather2sqlite.cli.commands.create_database_if_not_exists"
                    ) as database,
                    patch("ambientweather2sqlite.cli.commands.start_daemon") as daemon,
                    patch("ambientweather2sqlite.station_http._receiver_lock") as lock,
                    patch("socket.getaddrinfo") as dns,
                    patch("ambientweather2sqlite.mureq.get") as get,
                    patch("ambientweather2sqlite.mureq.post") as post,
                    self.assertRaises(SystemExit) as exit_code,
                ):
                    main(["serve", "--config", str(path)])
                self.assertEqual(exit_code.exception.code, 1)
                self.assertIn("alerts.webhook_url", output.call_args.args[0])
                for operation in (database, daemon, lock, dns, get, post):
                    operation.assert_not_called()

    @given(
        host=st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=1, max_size=30),
        scheme=st.sampled_from(("http", "https", "HTTP", "HTTPS", "HtTp", "hTtPs")),
        port=st.integers(min_value=1, max_value=65535),
    )
    def test_valid_configuration_preserves_url_spelling(
        self, host: str, scheme: str, port: int
    ) -> None:
        url = f"{scheme}://{host.upper()}.example:{port}/livedata.htm?raw=1"
        with tempfile.TemporaryDirectory() as directory:
            config = load_config(
                _write_config(
                    directory, url, f"[alerts]\nwebhook_url = {json.dumps(url)}\n"
                )
            )
            self.assertEqual(config.live_data_url, url)
            assert config.alerts is not None
            self.assertEqual(config.alerts.webhook_url, url)


class TestReceiverResponseErrors(TestCase):
    def test_supported_urls_reach_transport_without_rewriting(self) -> None:
        for url in _SUPPORTED_URLS:
            with self.subTest(url=url):
                with patch(
                    "ambientweather2sqlite.mureq.get", return_value=_PAGE
                ) as get:
                    self.assertEqual(station_get(url, timeout=1.5), _PAGE)
                get.assert_called_once_with(url, auto_retry=False, timeout=1.5)

    def test_malformed_webhook_does_not_stop_retries_or_committed_recovery(
        self,
    ) -> None:
        logger = Mock()
        notifier = AlertNotifier(
            AlertConfig("http://", failure_threshold=2), "http://station.local/", logger
        )
        with (
            patch("builtins.print"),
            patch(
                "ambientweather2sqlite.mureq.get",
                side_effect=[TimeoutError(), TimeoutError(), _PAGE],
            ),
            patch("ambientweather2sqlite.mureq.post") as post,
            patch(
                "ambientweather2sqlite.daemon.insert_observation", return_value=True
            ) as insert,
            patch("ambientweather2sqlite.daemon._refresh_metadata"),
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=[None, None, KeyboardInterrupt],
            ) as wait,
            self.assertRaises(KeyboardInterrupt),
        ):
            _collect_forever(
                notifier.station_url,
                "unused.db",
                {},
                {},
                logger,
                None,
                60,
                notifier,
                interactive=False,
            )
        self.assertEqual(wait.call_count, 3)
        insert.assert_called_once()
        post.assert_not_called()
        self.assertEqual(logger.warning.call_count, 1)
        self.assertFalse(notifier.alert_sent)
        self.assertEqual(notifier.consecutive_failures, 0)

    def test_invalid_urls_are_diagnosed_before_transport(self) -> None:
        for url in (*_INVALID_URLS, "ftp://station.local/live", "station.local/live"):
            with self.subTest(url=url):
                with (
                    patch("socket.getaddrinfo") as dns,
                    patch("ambientweather2sqlite.mureq.get") as get,
                    self.assertRaisesRegex(
                        HTTPException, "Invalid receiver URL"
                    ) as raised,
                ):
                    station_get(url, auto_retry=True, timeout=1.5)
                self.assertIsInstance(raised.exception.__cause__, ValueError)
                dns.assert_not_called()
                get.assert_not_called()

    def test_invalid_utf8_is_a_response_error_with_its_original_cause(self) -> None:
        url = "http://station.local/livedata.htm"
        logger = Mock()
        with patch(
            "ambientweather2sqlite.mureq.request", return_value=Mock(body=b"\xff")
        ):
            with self.assertRaisesRegex(
                HTTPException, "Receiver request failed"
            ) as raised:
                station_get(url)
            self.assertIsInstance(raised.exception.__cause__, UnicodeDecodeError)
            self.assertNotIn("Invalid receiver URL", str(raised.exception))
            self.assertEqual(
                _fetch_receiver_page(url, logger, interactive=False), ("", {}, 0)
            )
        self.assertIn("Receiver request failed", str(logger.info.call_args))

    def test_decoding_failures_retry_and_only_valid_data_recovers(self) -> None:
        logger = Mock()
        notifier = AlertNotifier(
            AlertConfig("http://hooks.invalid/", failure_threshold=2),
            "http://station.local/livedata.htm",
            logger,
        )
        with (
            patch("builtins.print"),
            patch(
                "ambientweather2sqlite.mureq.request",
                side_effect=[
                    Mock(body=b"\xff"),
                    Mock(body=b"\xff"),
                    Mock(body=_PAGE.encode()),
                ],
            ),
            patch(
                "ambientweather2sqlite.daemon.insert_observation", return_value=True
            ) as insert,
            patch("ambientweather2sqlite.daemon._refresh_metadata"),
            patch.object(AlertNotifier, "_send") as send,
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=[None, None, KeyboardInterrupt],
            ) as wait,
            self.assertRaises(KeyboardInterrupt),
        ):
            _collect_forever(
                notifier.station_url,
                "unused.db",
                {},
                {},
                logger,
                None,
                60,
                notifier,
                interactive=False,
            )
        self.assertEqual(wait.call_count, 3)
        insert.assert_called_once()
        self.assertEqual(
            [
                (call.args[0]["event"], call.args[0]["consecutive_failures"])
                for call in send.call_args_list
            ],
            [("station_offline", 2), ("station_online", 2)],
        )
        self.assertEqual(notifier.consecutive_failures, 0)
        self.assertFalse(notifier.alert_sent)
