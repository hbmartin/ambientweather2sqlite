import itertools
import json
import signal
import sqlite3
import sys
import tempfile
import threading
from contextlib import closing
from http.client import HTTPException
from logging.handlers import RotatingFileHandler
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, call, patch

from ambientweather2sqlite.daemon import (
    _collect_forever,
    _configure_logging,
    _install_sigterm_handler,
    _is_stale_observation,
    _refresh_metadata,
    _store_observation,
    clear_lines,
    fetch_once,
    next_deadline,
    start_daemon,
    wait_for_next_update,
)
from ambientweather2sqlite.database import create_database_if_not_exists
from ambientweather2sqlite.exceptions import InvalidTimestampError
from ambientweather2sqlite.models import AlertConfig, MqttConfig, RediscoveryConfig


def _patch_rediscovery_network(test: TestCase) -> None:
    """Keep default-on rediscovery from spawning subprocesses or subnet scans."""
    test.enterContext(
        patch(
            "ambientweather2sqlite.rediscovery.get_mac_address",
            return_value=None,
        ),
    )
    test.enterContext(
        patch(
            "ambientweather2sqlite.rediscovery.find_station_by_mac",
            return_value=None,
        ),
    )


class TestDaemonHelpers(TestCase):
    @patch("ambientweather2sqlite.daemon.insert_observation")
    def test_store_observation_skips_invalid_timestamps(
        self,
        mock_insert: Mock,
    ) -> None:
        mock_insert.side_effect = InvalidTimestampError("not-a-timestamp")
        logger = Mock()

        _store_observation(
            "weather.db",
            "http://127.0.0.1/livedata.htm",
            {"ts": "not-a-timestamp", "outTemp": 72.0},
            {"outTemp": "Outdoor Temperature"},
            {},
            logger,
            None,
        )

        logger.warning.assert_called_once()

    @patch("ambientweather2sqlite.daemon._refresh_metadata")
    @patch("ambientweather2sqlite.daemon.insert_observation", return_value=[])
    def test_store_observation_retries_missing_metadata_periodically(
        self,
        mock_insert: Mock,
        mock_refresh: Mock,
    ) -> None:
        for collection_attempt in (1, 9, 10, 11, 20):
            _store_observation(
                "weather.db",
                "http://127.0.0.1/livedata.htm",
                {"outTemp": 72.0},
                {},
                {},
                Mock(),
                None,
                collection_attempt=collection_attempt,
            )

        self.assertEqual(mock_refresh.call_count, 2)
        self.assertEqual(mock_insert.call_count, 5)

    @patch("ambientweather2sqlite.daemon._refresh_metadata")
    @patch(
        "ambientweather2sqlite.daemon.insert_observation",
        return_value=["newSensor"],
    )
    def test_store_observation_refreshes_immediately_after_schema_change(
        self,
        mock_insert: Mock,
        mock_refresh: Mock,
    ) -> None:
        _store_observation(
            "weather.db",
            "http://127.0.0.1/livedata.htm",
            {"newSensor": 1.0},
            {},
            {},
            Mock(),
            None,
            collection_attempt=1,
        )

        mock_refresh.assert_called_once()
        mock_insert.assert_called_once()

    def test_clear_lines_prints_escape_sequence_for_each_line(self):
        with patch("builtins.print") as mock_print:
            clear_lines(3)

        self.assertEqual(
            mock_print.call_args_list,
            [call("\033[A\033[K", end="")] * 3,
        )

    def test_wait_for_next_update_counts_down_and_sleeps(self):
        with (
            patch("builtins.print") as mock_print,
            patch("ambientweather2sqlite.daemon.time.sleep") as mock_sleep,
            patch(
                "ambientweather2sqlite.daemon.time.monotonic",
                side_effect=[0.0, 1.0, 2.0, 3.0],
            ),
        ):
            wait_for_next_update(3.0)

        self.assertEqual(
            mock_print.call_args_list,
            [
                call("\033[KNext update in 3 seconds", end="\r"),
                call("\033[KNext update in 2 seconds", end="\r"),
                call("\033[KNext update in 1 seconds", end="\r"),
            ],
        )
        self.assertEqual(
            mock_sleep.call_args_list,
            [call(1.0), call(1.0), call(1.0)],
        )

    def test_wait_for_next_update_returns_immediately_when_deadline_passed(self):
        with (
            patch("builtins.print") as mock_print,
            patch("ambientweather2sqlite.daemon.time.sleep") as mock_sleep,
            patch("ambientweather2sqlite.daemon.time.monotonic", return_value=10.0),
        ):
            wait_for_next_update(5.0)

        mock_print.assert_not_called()
        mock_sleep.assert_not_called()

    def test_wait_for_next_update_skips_countdown_when_disabled(self):
        # When stdout is captured the countdown escapes only pile up as noise,
        # so a single sleep replaces the per-second redraw.
        with (
            patch("builtins.print") as mock_print,
            patch("ambientweather2sqlite.daemon.time.sleep") as mock_sleep,
            patch("ambientweather2sqlite.daemon.time.monotonic", return_value=0.0),
        ):
            wait_for_next_update(5.0, show_countdown=False)

        mock_print.assert_not_called()
        mock_sleep.assert_called_once_with(5.0)

    def test_configure_logging_uses_a_bounded_rotating_handler(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "weather_daemon.log"
            logger = _configure_logging(log_path)
            handlers = [
                handler
                for handler in logger.handlers
                if isinstance(handler, RotatingFileHandler)
                and handler.baseFilename == str(log_path)
            ]
            try:
                self.assertEqual(len(handlers), 1)
                self.assertGreater(handlers[0].maxBytes, 0)
                self.assertGreater(handlers[0].backupCount, 0)
            finally:
                for handler in handlers:
                    logger.removeHandler(handler)
                    handler.close()

    def test_next_deadline_is_aligned_to_epoch(self):
        self.assertEqual(next_deadline(100.0, 60, 100.5), 160.0)
        self.assertEqual(next_deadline(100.0, 60, 159.9), 160.0)

    def test_next_deadline_skips_missed_slots(self):
        # A fetch that took 130s should schedule the next sample at +180s,
        # not queue up the two missed slots.
        self.assertEqual(next_deadline(100.0, 60, 230.0), 280.0)

    def test_install_sigterm_handler_raises_keyboard_interrupt(self):
        original = signal.getsignal(signal.SIGTERM)
        try:
            _install_sigterm_handler()
            handler = signal.getsignal(signal.SIGTERM)
            self.assertNotEqual(handler, original)
            assert callable(handler)
            with self.assertRaises(KeyboardInterrupt):
                handler(signal.SIGTERM, None)
        finally:
            signal.signal(signal.SIGTERM, original)

    def test_install_sigterm_handler_is_noop_off_main_thread(self):
        original = signal.getsignal(signal.SIGTERM)
        thread = threading.Thread(target=_install_sigterm_handler)
        thread.start()
        thread.join()
        self.assertEqual(signal.getsignal(signal.SIGTERM), original)

    @patch("ambientweather2sqlite.daemon._publish_discovery")
    @patch("ambientweather2sqlite.daemon.create_metadata")
    def test_refresh_metadata_updates_mutable_state_and_discovery(
        self,
        mock_create_metadata: Mock,
        mock_publish_discovery: Mock,
    ) -> None:
        mock_create_metadata.return_value = (
            {"newSensor": "New Sensor"},
            {"newSensor": "percent"},
        )
        labels = {"oldSensor": "Old Sensor"}
        units = {"oldSensor": "celsius"}
        logger = Mock()
        mqtt_config = MqttConfig(host="127.0.0.1", topic="weather/obs")

        refreshed = _refresh_metadata(
            "weather.db",
            "http://127.0.0.1/livedata.htm",
            labels,
            units,
            logger,
            mqtt_config,
        )

        self.assertTrue(refreshed)
        self.assertEqual(labels, {"newSensor": "New Sensor"})
        self.assertEqual(units, {"newSensor": "percent"})
        mock_publish_discovery.assert_called_once_with(
            logger,
            mqtt_config,
            labels,
            units,
        )

    @patch("ambientweather2sqlite.daemon.create_metadata")
    def test_refresh_metadata_survives_database_errors(
        self,
        mock_create_metadata: Mock,
    ) -> None:
        for error in (sqlite3.OperationalError("locked"), OSError("disk full")):
            with self.subTest(error=type(error).__name__):
                mock_create_metadata.side_effect = error
                labels = {"oldSensor": "Old Sensor"}
                units = {"oldSensor": "celsius"}
                logger = Mock()

                refreshed = _refresh_metadata(
                    "weather.db",
                    "http://127.0.0.1/livedata.htm",
                    labels,
                    units,
                    logger,
                    None,
                )

                self.assertFalse(refreshed)
                self.assertEqual(labels, {"oldSensor": "Old Sensor"})
                self.assertEqual(units, {"oldSensor": "celsius"})
                logger.info.assert_called_once()

    @patch("ambientweather2sqlite.daemon._publish_discovery")
    @patch("ambientweather2sqlite.daemon.create_metadata")
    def test_refresh_metadata_preserves_state_when_labels_are_partial(
        self,
        mock_create_metadata: Mock,
        mock_publish_discovery: Mock,
    ) -> None:
        mock_create_metadata.return_value = (
            {"newSensor": "New Sensor"},
            {"newSensor": "percent"},
        )
        labels = {"oldSensor": "Old Sensor"}
        units = {"oldSensor": "celsius"}
        logger = Mock()

        refreshed = _refresh_metadata(
            "weather.db",
            "http://127.0.0.1/livedata.htm",
            labels,
            units,
            logger,
            None,
            observed_sensor_names={"oldSensor", "newSensor"},
        )

        self.assertFalse(refreshed)
        self.assertEqual(labels, {"oldSensor": "Old Sensor"})
        self.assertEqual(units, {"oldSensor": "celsius"})
        mock_publish_discovery.assert_not_called()
        logger.info.assert_called_once()


class TestDaemonDiscovery(TestCase):
    _MQTT_DISCOVERY_CONFIG = MqttConfig(
        host="127.0.0.1",
        topic="weather/obs",
        discovery=True,
    )

    def setUp(self):
        _patch_rediscovery_network(self)

    def _run_daemon(
        self,
        mqtt_config,
        create_metadata_result,
    ) -> tuple[Mock, Mock]:
        logger = Mock()
        with (
            patch("builtins.print"),
            patch("ambientweather2sqlite.daemon.clear_lines"),
            patch(
                "ambientweather2sqlite.daemon._configure_logging",
                return_value=logger,
            ),
            patch(
                "ambientweather2sqlite.daemon.create_metadata",
                return_value=create_metadata_result,
            ),
            patch(
                "ambientweather2sqlite.daemon.publish_message",
            ) as mock_publish_message,
            patch("ambientweather2sqlite.daemon.mureq.get", return_value="<html />"),
            patch(
                "ambientweather2sqlite.daemon.extract_values",
                return_value={"outTemp": 72.5},
            ),
            patch(
                "ambientweather2sqlite.daemon.insert_observation",
                return_value=[],
            ),
            patch("ambientweather2sqlite.daemon._publish_observation"),
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=KeyboardInterrupt,
            ),
        ):
            with self.assertRaises(SystemExit):
                start_daemon(
                    "http://127.0.0.1/livedata.htm",
                    "/tmp/weather.db",
                    period_seconds=5,
                    mqtt_config=mqtt_config,
                )
        return logger, mock_publish_message

    def test_discovery_publishes_retained_configs_at_startup(self):
        _logger, mock_publish_message = self._run_daemon(
            self._MQTT_DISCOVERY_CONFIG,
            ({"outTemp": "Outdoor Temperature (degF)"}, {"outTemp": "fahrenheit"}),
        )

        mock_publish_message.assert_called_once()
        topic = mock_publish_message.call_args.args[1]
        self.assertEqual(
            topic,
            "homeassistant/sensor/aw2sqlite_aw2sqlite/outTemp/config",
        )
        self.assertTrue(mock_publish_message.call_args.kwargs["retain"])

    def test_discovery_skipped_when_metadata_empty(self):
        logger, mock_publish_message = self._run_daemon(
            self._MQTT_DISCOVERY_CONFIG,
            ({}, {}),
        )

        mock_publish_message.assert_not_called()
        warning_messages = [c.args[0] for c in logger.warning.call_args_list if c.args]
        self.assertTrue(
            any("Skipping MQTT discovery" in m for m in warning_messages),
        )

    def test_discovery_disabled_by_default(self):
        _logger, mock_publish_message = self._run_daemon(
            MqttConfig(host="127.0.0.1", topic="weather/obs"),
            ({"outTemp": "Outdoor Temperature (degF)"}, {"outTemp": "fahrenheit"}),
        )

        mock_publish_message.assert_not_called()


class TestDaemonAlerts(TestCase):
    def setUp(self):
        _patch_rediscovery_network(self)

    def test_start_daemon_counts_failures_in_both_branches(self):
        alert_config = AlertConfig(webhook_url="http://hooks.local/alert")
        notifier = Mock()

        with (
            patch("builtins.print"),
            patch("ambientweather2sqlite.daemon.clear_lines"),
            patch(
                "ambientweather2sqlite.daemon._configure_logging",
                return_value=Mock(),
            ),
            patch(
                "ambientweather2sqlite.daemon.create_metadata",
                return_value=({}, {}),
            ),
            patch(
                "ambientweather2sqlite.daemon.AlertNotifier",
                return_value=notifier,
            ) as mock_notifier_cls,
            patch(
                "ambientweather2sqlite.daemon.mureq.get",
                side_effect=[
                    TimeoutError,
                    HTTPException("live data down"),
                    KeyboardInterrupt,
                ],
            ),
            patch("ambientweather2sqlite.daemon.wait_for_next_update"),
        ):
            with self.assertRaises(SystemExit) as exc:
                start_daemon(
                    "http://127.0.0.1/livedata.htm",
                    "/tmp/weather.db",
                    period_seconds=5,
                    alert_config=alert_config,
                )

        self.assertEqual(exc.exception.code, 0)
        mock_notifier_cls.assert_called_once()
        self.assertIs(mock_notifier_cls.call_args.args[0], alert_config)
        self.assertEqual(notifier.record_failure.call_count, 2)
        notifier.record_success.assert_not_called()

    def test_start_daemon_counts_zero_sensor_parse_as_failure(self):
        notifier = Mock()

        with (
            patch("builtins.print"),
            patch("ambientweather2sqlite.daemon.clear_lines"),
            patch(
                "ambientweather2sqlite.daemon._configure_logging",
                return_value=Mock(),
            ),
            patch(
                "ambientweather2sqlite.daemon.create_metadata",
                return_value=({}, {}),
            ),
            patch(
                "ambientweather2sqlite.daemon.AlertNotifier",
                return_value=notifier,
            ),
            patch("ambientweather2sqlite.daemon.mureq.get", return_value="<html />"),
            patch("ambientweather2sqlite.daemon.extract_values", return_value={}),
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=KeyboardInterrupt,
            ),
        ):
            with self.assertRaises(SystemExit) as exc:
                start_daemon(
                    "http://127.0.0.1/livedata.htm",
                    "/tmp/weather.db",
                    period_seconds=5,
                    alert_config=AlertConfig(webhook_url="http://hooks.local/alert"),
                )

        self.assertEqual(exc.exception.code, 0)
        notifier.record_failure.assert_called_once_with()
        notifier.record_success.assert_not_called()

    def test_start_daemon_records_success_after_fetch(self):
        notifier = Mock()

        with (
            patch("builtins.print"),
            patch("ambientweather2sqlite.daemon.clear_lines"),
            patch(
                "ambientweather2sqlite.daemon._configure_logging",
                return_value=Mock(),
            ),
            patch(
                "ambientweather2sqlite.daemon.create_metadata",
                return_value=({}, {}),
            ),
            patch(
                "ambientweather2sqlite.daemon.AlertNotifier",
                return_value=notifier,
            ),
            patch("ambientweather2sqlite.daemon.mureq.get", return_value="<html />"),
            patch(
                "ambientweather2sqlite.daemon.extract_values",
                return_value={"tempf": 72.5},
            ),
            patch(
                "ambientweather2sqlite.daemon.insert_observation",
                return_value=[],
            ),
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=KeyboardInterrupt,
            ),
        ):
            with self.assertRaises(SystemExit) as exc:
                start_daemon(
                    "http://127.0.0.1/livedata.htm",
                    "/tmp/weather.db",
                    period_seconds=5,
                    alert_config=AlertConfig(webhook_url="http://hooks.local/alert"),
                )

        self.assertEqual(exc.exception.code, 0)
        notifier.record_success.assert_called_once_with()
        notifier.record_failure.assert_not_called()


class TestDaemonRediscovery(TestCase):
    _OLD_URL = "http://127.0.0.1/livedata.htm"
    _NEW_URL = "http://192.168.0.150/livedata.htm"
    _MAC = "aa:bb:cc:dd:ee:ff"

    def _run_daemon(
        self,
        *,
        fetch_side_effect: list[object],
        wait_side_effect: list[object],
        find_result: tuple[str, str | None] | None = None,
        captured_mac: str | None = None,
        rediscovery_config: RediscoveryConfig | None = None,
        alert_config: AlertConfig | None = None,
        port: int | None = None,
        interactive: bool = True,
    ) -> dict[str, Mock]:
        mocks = {
            "logger": Mock(),
            "server": Mock(),
            "notifier": Mock(),
            "fetch": Mock(side_effect=fetch_side_effect),
        }
        with (
            patch("builtins.print") as mock_print,
            patch("ambientweather2sqlite.daemon.clear_lines"),
            patch(
                "ambientweather2sqlite.daemon._configure_logging",
                return_value=mocks["logger"],
            ),
            patch(
                "ambientweather2sqlite.daemon.create_metadata",
                return_value=({}, {}),
            ),
            patch("ambientweather2sqlite.daemon.Server", return_value=mocks["server"]),
            patch(
                "ambientweather2sqlite.daemon.AlertNotifier",
                return_value=mocks["notifier"],
            ),
            patch("ambientweather2sqlite.daemon.mureq.get", mocks["fetch"]),
            patch(
                "ambientweather2sqlite.daemon.extract_values",
                return_value={"tempf": 72.5},
            ),
            patch(
                "ambientweather2sqlite.daemon.insert_observation",
                return_value=[],
            ) as mock_insert,
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=wait_side_effect,
            ),
            patch(
                "ambientweather2sqlite.rediscovery.find_station_by_mac",
                return_value=find_result,
            ) as mock_find,
            patch(
                "ambientweather2sqlite.rediscovery.get_mac_address",
                return_value=captured_mac,
            ) as mock_mac,
        ):
            with self.assertRaises(SystemExit) as exc:
                start_daemon(
                    self._OLD_URL,
                    "/tmp/weather.db",
                    port=port,
                    period_seconds=5,
                    alert_config=alert_config,
                    rediscovery_config=rediscovery_config,
                    interactive=interactive,
                )
        self.assertEqual(exc.exception.code, 0)
        mocks["find"] = mock_find
        mocks["mac"] = mock_mac
        mocks["insert"] = mock_insert
        mocks["print"] = mock_print
        return mocks

    def test_relocates_after_threshold_and_polls_new_url(self):
        mocks = self._run_daemon(
            fetch_side_effect=[
                HTTPException("station moved"),
                HTTPException("station moved"),
                "<html />",
            ],
            wait_side_effect=[None, None, KeyboardInterrupt],
            find_result=(self._NEW_URL, self._MAC),
            alert_config=AlertConfig(webhook_url="http://hooks.local/alert"),
            port=8080,
        )

        polled_urls = [c.args[0] for c in mocks["fetch"].call_args_list]
        self.assertEqual(polled_urls, [self._OLD_URL, self._OLD_URL, self._NEW_URL])
        mocks["find"].assert_called_once()
        mocks["server"].update_live_data_url.assert_called_once_with(self._NEW_URL)
        self.assertEqual(mocks["notifier"].station_url, self._NEW_URL)
        mocks["insert"].assert_called_once_with("/tmp/weather.db", {"tempf": 72.5})
        warning_args = mocks["logger"].warning.call_args.args
        self.assertIn("Weather station moved", warning_args[0])
        self.assertIn(self._OLD_URL, warning_args)
        self.assertIn(self._NEW_URL, warning_args)
        self.assertIn(
            call(f"Station relocated: now polling {self._NEW_URL}"),
            mocks["print"].call_args_list,
        )

    def test_relocates_after_paced_timeout_retries(self) -> None:
        mocks = self._run_daemon(
            fetch_side_effect=[TimeoutError, TimeoutError, "<html />"],
            wait_side_effect=[None, None, KeyboardInterrupt],
            find_result=(self._NEW_URL, self._MAC),
            interactive=False,
        )

        polled_urls = [c.args[0] for c in mocks["fetch"].call_args_list]
        self.assertEqual(polled_urls, [self._OLD_URL, self._OLD_URL, self._NEW_URL])

    def test_disabled_rediscovery_never_scans(self):
        mocks = self._run_daemon(
            fetch_side_effect=[
                HTTPException("station moved"),
                HTTPException("station moved"),
                HTTPException("station moved"),
            ],
            wait_side_effect=[None, None, KeyboardInterrupt],
            find_result=(self._NEW_URL, self._MAC),
            rediscovery_config=RediscoveryConfig(enabled=False),
        )

        mocks["find"].assert_not_called()
        polled_urls = [c.args[0] for c in mocks["fetch"].call_args_list]
        self.assertEqual(polled_urls, [self._OLD_URL] * 3)

    def test_failed_rescan_keeps_polling_the_old_url(self):
        mocks = self._run_daemon(
            fetch_side_effect=[
                HTTPException("station down"),
                HTTPException("station down"),
                "<html />",
            ],
            wait_side_effect=[None, None, KeyboardInterrupt],
            find_result=None,
        )

        mocks["find"].assert_called_once()
        mocks["server"].update_live_data_url.assert_not_called()
        polled_urls = [c.args[0] for c in mocks["fetch"].call_args_list]
        self.assertEqual(polled_urls, [self._OLD_URL] * 3)

    def test_mac_is_captured_once_after_first_success(self):
        mocks = self._run_daemon(
            fetch_side_effect=["<html />", "<html />"],
            wait_side_effect=[None, KeyboardInterrupt],
            captured_mac=self._MAC,
        )

        mocks["mac"].assert_called_once_with("127.0.0.1")


class TestFetchOnce(TestCase):
    def test_fetch_once_warns_on_zero_sensors(self):
        with (
            patch("builtins.print") as mock_print,
            patch("ambientweather2sqlite.daemon.mureq.get", return_value="<html />"),
            patch("ambientweather2sqlite.daemon.extract_values", return_value={}),
        ):
            fetch_once("http://127.0.0.1/livedata.htm")

        warning_calls = [
            c
            for c in mock_print.call_args_list
            if c.args and "parsed 0 sensors" in str(c.args[0])
        ]
        self.assertEqual(len(warning_calls), 1)
        self.assertIs(warning_calls[0].kwargs.get("file"), sys.stderr)


class TestStartDaemon(TestCase):
    def setUp(self):
        _patch_rediscovery_network(self)

    def test_start_daemon_survives_metadata_database_errors(self) -> None:
        for error in (sqlite3.OperationalError("locked"), OSError("disk full")):
            with self.subTest(error=type(error).__name__):
                logger = Mock()
                with (
                    patch("builtins.print"),
                    patch(
                        "ambientweather2sqlite.daemon._configure_logging",
                        return_value=logger,
                    ),
                    patch(
                        "ambientweather2sqlite.daemon.create_metadata",
                        side_effect=error,
                    ),
                    patch("ambientweather2sqlite.daemon._install_sigterm_handler"),
                    patch(
                        "ambientweather2sqlite.daemon._collect_forever",
                        side_effect=KeyboardInterrupt,
                    ),
                    self.assertRaises(SystemExit) as exc,
                ):
                    start_daemon(
                        "http://127.0.0.1/livedata.htm",
                        "/tmp/weather.db",
                    )

                self.assertEqual(exc.exception.code, 0)
                logger.info.assert_called_once()

    def test_start_daemon_processes_one_update_and_shuts_down_server(self):
        database_path = "/tmp/weather.db"
        server = Mock()
        logger = Mock()

        with (
            patch("builtins.print") as mock_print,
            patch("ambientweather2sqlite.daemon.clear_lines") as mock_clear_lines,
            patch(
                "ambientweather2sqlite.daemon._configure_logging",
                return_value=logger,
            ),
            patch(
                "ambientweather2sqlite.daemon.create_metadata",
                return_value=({"tempf": "Outdoor Temperature"}, {}),
            ) as mock_create_metadata,
            patch(
                "ambientweather2sqlite.daemon.Server",
                return_value=server,
            ) as mock_server,
            patch("ambientweather2sqlite.daemon.mureq.get", return_value="<html />"),
            patch(
                "ambientweather2sqlite.daemon.extract_values",
                return_value={"tempf": 72.5},
            ),
            patch(
                "ambientweather2sqlite.daemon.insert_observation",
                return_value=[],
            ) as mock_insert,
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=KeyboardInterrupt,
            ),
            self.assertRaises(SystemExit) as exc,
        ):
            start_daemon(
                "http://127.0.0.1/livedata.htm",
                database_path,
                port=8080,
                period_seconds=5,
                interactive=True,
            )

        self.assertEqual(exc.exception.code, 0)
        mock_create_metadata.assert_called_once_with(
            database_path,
            "http://127.0.0.1/livedata.htm",
        )
        mock_server.assert_called_once_with(
            "http://127.0.0.1/livedata.htm",
            database_path,
            8080,
            "localhost",
            None,
        )
        server.start.assert_called_once_with()
        server.shutdown.assert_called_once_with()
        mock_insert.assert_called_once_with(database_path, {"tempf": 72.5})
        mock_clear_lines.assert_called_once_with(0)
        self.assertIn(
            call("Starting JSON server on http://localhost:8080"),
            mock_print.call_args_list,
        )
        self.assertIn(
            call(json.dumps({"Outdoor Temperature": 72.5}, indent=4)),
            mock_print.call_args_list,
        )
        self.assertIn(
            call(f"\nStopping... results saved to {database_path}"),
            mock_print.call_args_list,
        )

    def test_start_daemon_non_interactive_suppresses_live_display(self):
        database_path = "/tmp/weather.db"
        logger = Mock()

        with (
            patch("builtins.print") as mock_print,
            patch("ambientweather2sqlite.daemon.clear_lines") as mock_clear_lines,
            patch(
                "ambientweather2sqlite.daemon._configure_logging",
                return_value=logger,
            ),
            patch(
                "ambientweather2sqlite.daemon.create_metadata",
                return_value=({"tempf": "Outdoor Temperature"}, {}),
            ),
            patch("ambientweather2sqlite.daemon.mureq.get", return_value="<html />"),
            patch(
                "ambientweather2sqlite.daemon.extract_values",
                return_value={"tempf": 72.5},
            ),
            patch(
                "ambientweather2sqlite.daemon.insert_observation",
                return_value=[],
            ) as mock_insert,
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=KeyboardInterrupt,
            ) as mock_wait,
        ):
            with self.assertRaises(SystemExit) as exc:
                start_daemon(
                    "http://127.0.0.1/livedata.htm",
                    database_path,
                    period_seconds=5,
                    interactive=False,
                )

        self.assertEqual(exc.exception.code, 0)
        mock_insert.assert_called_once_with(database_path, {"tempf": 72.5})
        # No terminal cursor control is emitted when stdout is captured.
        mock_clear_lines.assert_not_called()
        # The live countdown is disabled so captured logs stay bounded.
        mock_wait.assert_called_once()
        self.assertFalse(mock_wait.call_args.kwargs["show_countdown"])
        # A concise one-line summary replaces the multi-line JSON redraw.
        printed = [c.args[0] for c in mock_print.call_args_list if c.args]
        self.assertTrue(any("Recorded 1 sensors" in line for line in printed))
        self.assertFalse(any(line.startswith("Updated at:") for line in printed))

    def test_start_daemon_timeout_is_quiet_when_non_interactive(self):
        database_path = "/tmp/weather.db"
        logger = Mock()

        with (
            patch("builtins.print") as mock_print,
            patch("ambientweather2sqlite.daemon.clear_lines") as mock_clear_lines,
            patch(
                "ambientweather2sqlite.daemon._configure_logging",
                return_value=logger,
            ),
            patch(
                "ambientweather2sqlite.daemon.create_metadata",
                return_value=({}, {}),
            ),
            patch(
                "ambientweather2sqlite.daemon.mureq.get",
                side_effect=[TimeoutError, KeyboardInterrupt],
            ),
            patch("ambientweather2sqlite.daemon.wait_for_next_update") as mock_wait,
        ):
            with self.assertRaises(SystemExit) as exc:
                start_daemon(
                    "http://127.0.0.1/livedata.htm",
                    database_path,
                    period_seconds=5,
                    interactive=False,
                )

        self.assertEqual(exc.exception.code, 0)
        # The timeout is still recorded to the (rotating) daemon log...
        self.assertIn(call("TimeoutError"), logger.info.call_args_list)
        # ...but the redundant status line is not reprinted to captured stdout.
        printed = [c.args[0] for c in mock_print.call_args_list if c.args]
        self.assertFalse(any("Warming up" in line for line in printed))
        # No terminal cursor control; timeout retries still wait a full interval.
        mock_clear_lines.assert_not_called()
        mock_wait.assert_called_once()

    def test_start_daemon_logs_metadata_and_live_data_http_errors(self):
        database_path = "/tmp/weather.db"
        logger = Mock()
        server = Mock()

        with (
            patch("builtins.print") as mock_print,
            patch("ambientweather2sqlite.daemon.clear_lines"),
            patch(
                "ambientweather2sqlite.daemon._configure_logging",
                return_value=logger,
            ),
            patch(
                "ambientweather2sqlite.daemon.create_metadata",
                side_effect=HTTPException("metadata down"),
            ),
            patch("ambientweather2sqlite.daemon.Server", return_value=server),
            patch(
                "ambientweather2sqlite.daemon.mureq.get",
                side_effect=HTTPException("live data down"),
            ),
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=KeyboardInterrupt,
            ),
            patch("ambientweather2sqlite.daemon.insert_observation") as mock_insert,
        ):
            with self.assertRaises(SystemExit) as exc:
                start_daemon(
                    "http://127.0.0.1/livedata.htm",
                    database_path,
                    port=8080,
                    period_seconds=5,
                )

        self.assertEqual(exc.exception.code, 0)
        self.assertEqual(logger.info.call_count, 2)
        server.start.assert_called_once_with()
        server.shutdown.assert_called_once_with()
        mock_insert.assert_not_called()
        self.assertIn(
            call("Error fetching metadata: metadata down"),
            mock_print.call_args_list,
        )
        self.assertIn(
            call("Error fetching live data: live data down"),
            mock_print.call_args_list,
        )

    def test_start_daemon_survives_insert_errors_without_publishing(self) -> None:
        database_path = "/tmp/weather.db"
        logger = Mock()

        with (
            patch("builtins.print"),
            patch("ambientweather2sqlite.daemon.clear_lines"),
            patch(
                "ambientweather2sqlite.daemon._configure_logging",
                return_value=logger,
            ),
            patch(
                "ambientweather2sqlite.daemon.create_metadata",
                return_value=({}, {}),
            ),
            patch("ambientweather2sqlite.daemon.mureq.get", return_value="<html />"),
            patch(
                "ambientweather2sqlite.daemon.extract_values",
                return_value={"tempf": 72.5},
            ),
            patch(
                "ambientweather2sqlite.daemon.insert_observation",
                side_effect=sqlite3.OperationalError("database is locked"),
            ) as mock_insert,
            patch(
                "ambientweather2sqlite.daemon._publish_observation",
            ) as mock_publish,
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=KeyboardInterrupt,
            ),
        ):
            with self.assertRaises(SystemExit) as exc:
                start_daemon(
                    "http://127.0.0.1/livedata.htm",
                    database_path,
                    period_seconds=5,
                )

        self.assertEqual(exc.exception.code, 0)
        mock_insert.assert_called_once_with(database_path, {"tempf": 72.5})
        logger.warning.assert_called_once()
        self.assertIn(
            "Failed to insert observation",
            logger.warning.call_args.args[0],
        )
        mock_publish.assert_not_called()

    def test_start_daemon_skips_insert_when_fetch_parses_zero_sensors(self):
        database_path = "/tmp/weather.db"
        logger = Mock()

        with (
            patch("builtins.print") as mock_print,
            patch("ambientweather2sqlite.daemon.clear_lines"),
            patch(
                "ambientweather2sqlite.daemon._configure_logging",
                return_value=logger,
            ),
            patch(
                "ambientweather2sqlite.daemon.create_metadata",
                return_value=({}, {}),
            ),
            patch("ambientweather2sqlite.daemon.mureq.get", return_value="<html />"),
            patch(
                "ambientweather2sqlite.daemon.extract_values",
                return_value={},
            ),
            patch("ambientweather2sqlite.daemon.insert_observation") as mock_insert,
            patch(
                "ambientweather2sqlite.daemon._publish_observation",
            ) as mock_publish,
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=KeyboardInterrupt,
            ) as mock_wait,
        ):
            with self.assertRaises(SystemExit) as exc:
                start_daemon(
                    "http://127.0.0.1/livedata.htm",
                    database_path,
                    period_seconds=5,
                )

        self.assertEqual(exc.exception.code, 0)
        mock_insert.assert_not_called()
        mock_publish.assert_not_called()
        mock_wait.assert_called_once()
        logger.warning.assert_called_once()
        self.assertIn("Parsed 0 sensors", logger.warning.call_args.args[0])
        self.assertIn(
            call("Warning: parsed 0 sensors from station page"),
            mock_print.call_args_list,
        )

    def test_start_daemon_paces_retries_after_timeout(self) -> None:
        database_path = "/tmp/weather.db"
        logger = Mock()

        with (
            patch("builtins.print") as mock_print,
            patch(
                "ambientweather2sqlite.daemon.clear_lines",
                side_effect=[None, KeyboardInterrupt],
            ) as mock_clear_lines,
            patch(
                "ambientweather2sqlite.daemon._configure_logging",
                return_value=logger,
            ),
            patch(
                "ambientweather2sqlite.daemon.create_metadata",
                return_value=({}, {}),
            ),
            patch("ambientweather2sqlite.daemon.mureq.get", side_effect=TimeoutError),
            patch("ambientweather2sqlite.daemon.wait_for_next_update") as mock_wait,
            patch("ambientweather2sqlite.daemon.insert_observation") as mock_insert,
        ):
            with self.assertRaises(SystemExit) as exc:
                start_daemon(
                    "http://127.0.0.1/livedata.htm",
                    database_path,
                    period_seconds=5,
                    interactive=True,
                )

        self.assertEqual(exc.exception.code, 0)
        self.assertEqual(logger.info.call_args_list, [call("TimeoutError")])
        self.assertEqual(mock_clear_lines.call_args_list, [call(0), call(1)])
        mock_wait.assert_called_once()
        mock_insert.assert_not_called()
        self.assertIn(
            call("Warming up weather station's server..."),
            mock_print.call_args_list,
        )

    def test_start_daemon_continues_after_live_data_http_error(self):
        database_path = "/tmp/weather.db"
        logger = Mock()

        with (
            patch("builtins.print") as mock_print,
            patch(
                "ambientweather2sqlite.daemon.clear_lines",
                side_effect=[None, KeyboardInterrupt],
            ) as mock_clear_lines,
            patch(
                "ambientweather2sqlite.daemon._configure_logging",
                return_value=logger,
            ),
            patch(
                "ambientweather2sqlite.daemon.create_metadata",
                return_value=({}, {}),
            ),
            patch(
                "ambientweather2sqlite.daemon.mureq.get",
                side_effect=HTTPException("live data down"),
            ),
            patch("ambientweather2sqlite.daemon.wait_for_next_update") as mock_wait,
            patch("ambientweather2sqlite.daemon.insert_observation") as mock_insert,
        ):
            with self.assertRaises(SystemExit) as exc:
                start_daemon(
                    "http://127.0.0.1/livedata.htm",
                    database_path,
                    period_seconds=5,
                    interactive=True,
                )

        self.assertEqual(exc.exception.code, 0)
        self.assertEqual(logger.info.call_count, 1)
        self.assertEqual(mock_clear_lines.call_args_list, [call(0), call(1)])
        mock_wait.assert_called_once()
        mock_insert.assert_not_called()
        self.assertIn(
            call("Error fetching live data: live data down"),
            mock_print.call_args_list,
        )


def _livedata_page(station_time: str, out_temp: str = "72.5") -> str:
    """Build a livedata page carrying a station `CurrTime` and one sensor."""
    return (
        "<html><body><table>"
        "<tr><td>Receiver Time</td><td>"
        f'<input name="CurrTime" value="{station_time}" disabled></td></tr>'
        "<tr><td>Outdoor Temperature</td><td>"
        f'<input name="outTemp" value="{out_temp}" disabled></td></tr>'
        "</table></body></html>"
    )


def _sensor_only_page(out_temp: str) -> str:
    """Build a livedata page with no station time (forces value-equality)."""
    return (
        "<html><body><table>"
        "<tr><td>Outdoor Temperature</td><td>"
        f'<input name="outTemp" value="{out_temp}" disabled></td></tr>'
        "</table></body></html>"
    )


class TestIsStaleObservation(TestCase):
    _TIME = "14:47 9/21/2021"
    _LATER = "14:48 9/21/2021"

    def test_first_poll_is_never_stale(self):
        # No previous reading exists on the first poll after a (re)start.
        self.assertFalse(
            _is_stale_observation(None, self._TIME, None, {"outTemp": 72.5}),
        )

    def test_unchanged_station_time_with_changed_values_is_fresh(self):
        # A repeated station time but changed values is a genuine sub-minute update
        # (e.g. a fast-updating wind sensor within the same console minute), not a
        # stale page. Only advancing station time OR identical values would skip it.
        self.assertFalse(
            _is_stale_observation(
                self._TIME,
                self._TIME,
                {"outTemp": 72.5},
                {"outTemp": 99.9},
            ),
        )

    def test_unchanged_station_time_and_values_is_stale(self):
        # A repeated station time AND identical values is a stale page.
        self.assertTrue(
            _is_stale_observation(
                self._TIME,
                self._TIME,
                {"outTemp": 72.5},
                {"outTemp": 72.5},
            ),
        )

    def test_advanced_station_time_is_fresh(self):
        # A newer station time means a genuine reading, even if values match.
        self.assertFalse(
            _is_stale_observation(
                self._TIME,
                self._LATER,
                {"outTemp": 72.5},
                {"outTemp": 72.5},
            ),
        )

    def test_identical_values_are_stale_without_station_time(self):
        self.assertTrue(
            _is_stale_observation(None, None, {"outTemp": 72.5}, {"outTemp": 72.5}),
        )

    def test_changed_values_are_fresh_without_station_time(self):
        self.assertFalse(
            _is_stale_observation(None, None, {"outTemp": 72.5}, {"outTemp": 73.0}),
        )

    def test_falls_back_to_values_when_previous_time_missing(self):
        # A newly appearing station time cannot be compared, so values decide.
        self.assertTrue(
            _is_stale_observation(
                None,
                self._TIME,
                {"outTemp": 72.5},
                {"outTemp": 72.5},
            ),
        )


class TestCollectForeverStaleDedup(TestCase):
    """End-to-end stale-page dedup through the real parser and database."""

    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        Path(self.db_path).unlink(missing_ok=True)
        create_database_if_not_exists(self.db_path)
        # A monotonic counter guarantees distinct wall-clock timestamps so that
        # genuinely-new rows are never collapsed by the UNIQUE ts index.
        self._ts_counter = itertools.count(1)

    def tearDown(self):
        Path(self.db_path).unlink(missing_ok=True)

    def _next_ts(self) -> str:
        n = next(self._ts_counter)
        return f"2026-01-01 00:{n // 60:02d}:{n % 60:02d}.000000"

    def _row_count(self) -> int:
        with closing(sqlite3.connect(self.db_path)) as conn:
            return conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]

    def _run_cycles(self, bodies: list[str]) -> None:
        waits: list[object] = [None] * (len(bodies) - 1)
        waits.append(KeyboardInterrupt)
        with (
            patch("builtins.print"),
            patch("ambientweather2sqlite.daemon.mureq.get", side_effect=bodies),
            patch("ambientweather2sqlite.daemon._refresh_metadata"),
            patch(
                "ambientweather2sqlite.database.writes._current_observation_timestamp",
                side_effect=self._next_ts,
            ),
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=waits,
            ),
            self.assertRaises(KeyboardInterrupt),
        ):
            _collect_forever(
                "http://127.0.0.1/livedata.htm",
                self.db_path,
                {},
                {},
                Mock(),
                None,
                60,
                interactive=False,
            )

    def test_identical_stale_pages_store_single_row(self):
        page = _livedata_page("14:47 9/21/2021")
        self._run_cycles([page, page, page])

        self.assertEqual(self._row_count(), 1)

    def test_advancing_station_time_stores_new_rows(self):
        # Identical sensor values, but the station time advances each cycle, so
        # every page is a genuine observation that must be stored.
        self._run_cycles(
            [
                _livedata_page("14:47 9/21/2021"),
                _livedata_page("14:48 9/21/2021"),
            ],
        )

        self.assertEqual(self._row_count(), 2)

    def test_value_equality_dedup_without_station_time(self):
        page = _sensor_only_page("72.5")
        self._run_cycles([page, page])

        self.assertEqual(self._row_count(), 1)

    def test_value_change_without_station_time_stores_new_row(self):
        self._run_cycles([_sensor_only_page("72.5"), _sensor_only_page("73.0")])

        self.assertEqual(self._row_count(), 2)

    def test_restart_reinserts_first_page(self):
        page = _livedata_page("14:47 9/21/2021")
        # First daemon lifetime stores the page once.
        self._run_cycles([page])
        self.assertEqual(self._row_count(), 1)
        # A restart has no in-memory previous state, so the identical page on
        # the first poll after restart is stored again under a fresh ts.
        self._run_cycles([page])
        self.assertEqual(self._row_count(), 2)


class TestStartDaemonStaleDedup(TestCase):
    def setUp(self):
        _patch_rediscovery_network(self)

    def _run_two_cycles(
        self,
        *,
        values_side_effect,
        station_time_side_effect,
    ) -> tuple[Mock, Mock]:
        mock_insert = Mock(return_value=[])
        mock_publish = Mock()
        with (
            patch("builtins.print"),
            patch("ambientweather2sqlite.daemon.clear_lines"),
            patch(
                "ambientweather2sqlite.daemon._configure_logging",
                return_value=Mock(),
            ),
            patch(
                "ambientweather2sqlite.daemon.create_metadata",
                return_value=({}, {}),
            ),
            patch("ambientweather2sqlite.daemon.mureq.get", return_value="<html />"),
            patch(
                "ambientweather2sqlite.daemon.extract_values",
                side_effect=values_side_effect,
            ),
            patch(
                "ambientweather2sqlite.daemon.extract_station_time",
                side_effect=station_time_side_effect,
            ),
            patch(
                "ambientweather2sqlite.daemon.insert_observation",
                mock_insert,
            ),
            patch(
                "ambientweather2sqlite.daemon._publish_observation",
                mock_publish,
            ),
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=[None, KeyboardInterrupt],
            ),
        ):
            with self.assertRaises(SystemExit) as exc:
                start_daemon(
                    "http://127.0.0.1/livedata.htm",
                    "/tmp/weather.db",
                    period_seconds=5,
                    interactive=False,
                )
        self.assertEqual(exc.exception.code, 0)
        return mock_insert, mock_publish

    def test_stale_page_skips_store_and_publish(self):
        # Two identical pages sharing one station time: only the first is new.
        mock_insert, mock_publish = self._run_two_cycles(
            values_side_effect=[{"outTemp": 72.5}, {"outTemp": 72.5}],
            station_time_side_effect=["14:47 9/21/2021", "14:47 9/21/2021"],
        )

        mock_insert.assert_called_once_with("/tmp/weather.db", {"outTemp": 72.5})
        mock_publish.assert_called_once()

    def test_changed_page_stores_each_cycle(self):
        mock_insert, mock_publish = self._run_two_cycles(
            values_side_effect=[{"outTemp": 72.5}, {"outTemp": 73.0}],
            station_time_side_effect=["14:47 9/21/2021", "14:48 9/21/2021"],
        )

        self.assertEqual(mock_insert.call_count, 2)
        self.assertEqual(mock_publish.call_count, 2)
