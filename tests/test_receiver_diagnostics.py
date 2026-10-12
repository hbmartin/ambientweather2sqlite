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
    "http://station.local:65536/",
    "http://" + "a" * 64 + ".invalid/",
    "http://\u200d.invalid/",
)
_PAGE = '<input name="outTemp" disabled value="72.5">'


def _write_config(directory: str, url: str, extra: str = "") -> Path:
    path = Path(directory) / "config.toml"
    path.write_text(
        f"live_data_url = {json.dumps(url, ensure_ascii=False)}\n"
        'database_path = "unused.db"\n' + extra,
        encoding="utf-8",
    )
    return path


class TestReceiverConfiguration(TestCase):
    def test_malformed_configuration_exits_before_startup_or_network(self) -> None:
        for url in _INVALID_URLS:
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
                self.assertIn("live_data_url is invalid", output.call_args.args[0])
                for operation in (database, daemon, lock, dns, get, post):
                    operation.assert_not_called()

    def test_supported_urls_are_preserved_without_locks_or_network(self) -> None:
        for url in (
            "http://Observer.LOCAL:8080/livedata.htm?raw=1#top",
            "https://127.0.0.1/livedata.htm",
            "http://[2001:0db8:0:0:0:0:0:1]:8080/live",
            "http://[fe80::1%eth0]/live",
            "http://BÜCHER.local/livedata.htm",
            "http://xn--bcher-kva.local/livedata.htm",
        ):
            with self.subTest(url=url), tempfile.TemporaryDirectory() as directory:
                path = _write_config(directory, url)
                with (
                    patch("ambientweather2sqlite.station_http._receiver_lock") as lock,
                    patch("socket.getaddrinfo") as dns,
                    patch("ambientweather2sqlite.mureq.get") as get,
                ):
                    self.assertEqual(load_config(path).live_data_url, url)
                lock.assert_not_called()
                dns.assert_not_called()
                get.assert_not_called()

    def test_other_url_fields_keep_existing_validation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_config(
                directory,
                "http://station.local/",
                '[alerts]\nwebhook_url = "http://"\n',
            )
            config = load_config(path)
        self.assertIsNotNone(config.alerts)
        assert config.alerts is not None
        self.assertEqual(config.alerts.webhook_url, "http://")

    @given(
        host=st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=1, max_size=30),
        scheme=st.sampled_from(("http", "https")),
        port=st.integers(min_value=1, max_value=65535),
    )
    def test_valid_configuration_preserves_url_spelling(
        self, host: str, scheme: str, port: int
    ) -> None:
        url = f"{scheme}://{host.upper()}.example:{port}/livedata.htm?raw=1"
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(
                load_config(_write_config(directory, url)).live_data_url, url
            )


class TestReceiverResponseErrors(TestCase):
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
