import json
import signal
import sqlite3
import sys
import tempfile
import threading
from http.client import HTTPException
from logging.handlers import RotatingFileHandler
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, call, patch

from ambientweather2sqlite.daemon import (
    _configure_logging,
    _install_sigterm_handler,
    clear_lines,
    fetch_once,
    next_deadline,
    start_daemon,
    wait_for_next_update,
)
from ambientweather2sqlite.models import AlertConfig, MqttConfig


class TestDaemonHelpers(TestCase):
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


class TestDaemonDiscovery(TestCase):
    _MQTT_DISCOVERY_CONFIG = MqttConfig(
        host="127.0.0.1",
        topic="weather/obs",
        discovery=True,
    )

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
            patch("ambientweather2sqlite.daemon.insert_observation"),
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
            patch("ambientweather2sqlite.daemon.insert_observation"),
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
            patch("ambientweather2sqlite.daemon.insert_observation") as mock_insert,
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
            patch("ambientweather2sqlite.daemon.insert_observation") as mock_insert,
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
        # No terminal cursor control, and the fast warmup retry never waits.
        mock_clear_lines.assert_not_called()
        mock_wait.assert_not_called()

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

    def test_start_daemon_survives_insert_errors_and_still_publishes(self):
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
        mock_publish.assert_called_once()

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

    def test_start_daemon_retries_after_timeout_without_waiting(self):
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
        mock_wait.assert_not_called()
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
