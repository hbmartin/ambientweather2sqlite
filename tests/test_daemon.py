import json
from http.client import HTTPException
from unittest import TestCase
from unittest.mock import Mock, call, patch

from ambientweather2sqlite.daemon import clear_lines, start_daemon, wait_for_next_update


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
        ):
            wait_for_next_update(3)

        self.assertEqual(
            mock_print.call_args_list,
            [
                call("\033[KNext update in 3 seconds", end="\r"),
                call("\033[KNext update in 2 seconds", end="\r"),
                call("\033[KNext update in 1 seconds", end="\r"),
            ],
        )
        self.assertEqual(mock_sleep.call_args_list, [call(1), call(1), call(1)])


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
                )

        self.assertEqual(exc.exception.code, 0)
        self.assertEqual(logger.info.call_count, 1)
        self.assertEqual(mock_clear_lines.call_args_list, [call(0), call(1)])
        mock_wait.assert_called_once_with(5)
        mock_insert.assert_not_called()
        self.assertIn(
            call("Error fetching live data: live data down"),
            mock_print.call_args_list,
        )
