import io
import json
from contextlib import redirect_stdout
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from ambientweather2sqlite.cli.commands import main
from ambientweather2sqlite.cli.parser import parse_args
from ambientweather2sqlite.models import AppConfig


class TestMainShim(TestCase):
    def test_dunder_main_reexports_cli_main(self):
        import ambientweather2sqlite.__main__ as dunder_main

        self.assertIs(dunder_main.main, main)


class TestParseArgs(TestCase):
    def test_parse_serve_with_all_flags(self):
        args = parse_args(
            ["serve", "--port", "8080", "--config", "1234", "--log-format", "json"],
        )

        self.assertEqual(args.command, "serve")
        self.assertEqual(args.port, 8080)
        self.assertEqual(args.config_path, Path("1234"))
        self.assertEqual(args.log_format, "json")

    def test_parse_serve_defaults_log_format_to_none(self):
        args = parse_args(["serve"])

        self.assertEqual(args.command, "serve")
        self.assertIsNone(args.log_format)

    def test_bare_flags_default_to_serve(self):
        args = parse_args(["--port", "8080", "--config", "1234"])

        self.assertEqual(args.command, "serve")
        self.assertEqual(args.port, 8080)
        self.assertEqual(args.config_path, Path("1234"))

    def test_no_args_defaults_to_serve(self):
        args = parse_args([])

        self.assertEqual(args.command, "serve")

    def test_parse_config_subcommand(self):
        args = parse_args(["config", "--config", "myconfig.toml"])

        self.assertEqual(args.command, "config")
        self.assertEqual(args.config_path, Path("myconfig.toml"))

    def test_parse_once_subcommand(self):
        args = parse_args(["once"])

        self.assertEqual(args.command, "once")

    def test_parse_status_subcommand(self):
        args = parse_args(["status", "--config", "myconfig.toml"])

        self.assertEqual(args.command, "status")
        self.assertEqual(args.config_path, Path("myconfig.toml"))

    def test_parse_install_launchd_subcommand(self):
        args = parse_args(["install-launchd"])

        self.assertEqual(args.command, "install-launchd")

    def test_top_level_help_lists_subcommands(self):
        stdout = io.StringIO()

        with (
            self.assertRaises(SystemExit),
            redirect_stdout(stdout),
        ):
            parse_args(["--help"])

        help_output = stdout.getvalue()
        self.assertIn("config", help_output)
        self.assertIn("once", help_output)
        self.assertIn("install-launchd", help_output)


class TestMainServe(TestCase):
    @patch("ambientweather2sqlite.cli.commands.start_daemon")
    @patch("ambientweather2sqlite.cli.commands.create_database_if_not_exists")
    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_serve_uses_cli_flags(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
        mock_create_database: Mock,
        mock_start_daemon: Mock,
    ):
        config_override = Path("1234")
        resolved_config_path = Path("resolved-config.toml")
        mock_get_config_path.return_value = Path("default-config.toml")
        mock_create_config_file.return_value = resolved_config_path
        mock_load_config.return_value = AppConfig(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
            port=9000,
        )

        main(
            [
                "serve",
                "--config",
                str(config_override),
                "--port",
                "8080",
                "--log-format",
                "json",
            ],
        )

        mock_create_config_file.assert_called_once_with(config_override)
        mock_load_config.assert_called_once_with(resolved_config_path)
        mock_create_database.assert_called_once_with("weather.db")
        mock_start_daemon.assert_called_once_with(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
            port=8080,
            log_format="json",
            host="localhost",
            auth_token=None,
            mqtt_config=None,
        )

    @patch("ambientweather2sqlite.cli.commands.start_daemon")
    @patch("ambientweather2sqlite.cli.commands.create_database_if_not_exists")
    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_backward_compat_flags(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
        mock_create_database: Mock,
        mock_start_daemon: Mock,
    ):
        resolved_config_path = Path("resolved-config.toml")
        mock_get_config_path.return_value = None
        mock_create_config_file.return_value = resolved_config_path
        mock_load_config.return_value = AppConfig(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
            port=9000,
        )

        # Old-style flags without subcommand
        main(["--config", str(Path("1234")), "--port", "8080"])

        mock_start_daemon.assert_called_once_with(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
            port=8080,
            log_format="text",
            host="localhost",
            auth_token=None,
            mqtt_config=None,
        )


class TestMainOnce(TestCase):
    @patch("ambientweather2sqlite.cli.commands.fetch_once")
    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_once_calls_fetch_once(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
        mock_fetch_once: Mock,
    ):
        mock_get_config_path.return_value = Path("config.toml")
        mock_create_config_file.return_value = Path("config.toml")
        mock_load_config.return_value = AppConfig(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
        )

        main(["once"])

        mock_fetch_once.assert_called_once_with("http://127.0.0.1/livedata.htm")


class TestMainConfig(TestCase):
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    def test_main_config_overwrites_existing_file(self, mock_create_config_file: Mock):
        config_path = Path("config.toml")
        mock_create_config_file.return_value = config_path

        main(["config", "--config", str(config_path)])

        mock_create_config_file.assert_called_once_with(
            config_path,
            overwrite_existing=True,
        )


class TestMainStatus(TestCase):
    @patch("builtins.print")
    @patch("ambientweather2sqlite.cli.commands.query_observation_gaps")
    @patch("ambientweather2sqlite.cli.commands.query_db_metrics")
    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_status_prints_metrics(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
        mock_query_db_metrics: Mock,
        mock_query_observation_gaps: Mock,
        mock_print: Mock,
    ):
        config_path = Path("config.toml")
        metrics = {
            "row_count": 1,
            "db_file_size_bytes": 512,
            "earliest_ts": "2026-01-01 12:00:00",
            "latest_ts": "2026-01-01 12:00:00",
            "column_count": 3,
        }
        mock_get_config_path.return_value = config_path
        mock_create_config_file.return_value = config_path
        mock_load_config.return_value = AppConfig(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
        )
        mock_query_db_metrics.return_value = metrics
        mock_query_observation_gaps.return_value = []

        with patch("ambientweather2sqlite.cli.commands.Path.exists", return_value=True):
            main(["status"])

        mock_query_db_metrics.assert_called_once_with("weather.db")
        mock_query_observation_gaps.assert_called_once_with(
            "weather.db",
            hours=24,
            threshold_seconds=180.0,
        )
        mock_print.assert_called_once_with(
            json.dumps({**metrics, "gaps": []}, indent=2),
        )


class TestMainBackup(TestCase):
    @patch("builtins.print")
    @patch("ambientweather2sqlite.cli.commands.backup_database")
    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_backup_calls_backup_database(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
        mock_backup_database: Mock,
        mock_print: Mock,
    ):
        config_path = Path("config.toml")
        mock_get_config_path.return_value = config_path
        mock_create_config_file.return_value = config_path
        mock_load_config.return_value = AppConfig(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
        )
        mock_backup_database.return_value = 4096

        with patch(
            "ambientweather2sqlite.cli.commands.Path.exists",
            return_value=True,
        ):
            main(["backup", "copy.db"])

        mock_backup_database.assert_called_once_with("weather.db", "copy.db")
        mock_print.assert_called_once_with(f"Backed up 4096 bytes to {Path('copy.db')}")

    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_backup_exits_when_database_missing(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
    ):
        config_path = Path("config.toml")
        mock_get_config_path.return_value = config_path
        mock_create_config_file.return_value = config_path
        mock_load_config.return_value = AppConfig(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="missing.db",
        )

        with (
            patch(
                "ambientweather2sqlite.cli.commands.Path.exists",
                return_value=False,
            ),
            self.assertRaises(SystemExit) as exc,
        ):
            main(["backup", "copy.db"])

        self.assertEqual(exc.exception.code, 1)

    @patch("ambientweather2sqlite.cli.commands.backup_database")
    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_backup_exits_when_destination_exists(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
        mock_backup_database: Mock,
    ):
        config_path = Path("config.toml")
        mock_get_config_path.return_value = config_path
        mock_create_config_file.return_value = config_path
        mock_load_config.return_value = AppConfig(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
        )
        mock_backup_database.side_effect = FileExistsError("already exists")

        with (
            patch(
                "ambientweather2sqlite.cli.commands.Path.exists",
                return_value=True,
            ),
            self.assertRaises(SystemExit) as exc,
        ):
            main(["backup", "copy.db"])

        self.assertEqual(exc.exception.code, 1)


class TestMainInstallLaunchd(TestCase):
    @patch("ambientweather2sqlite.launchd.install_launchd")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_install_launchd_calls_handler(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_install_launchd: Mock,
    ):
        config_path = Path("config.toml")
        mock_get_config_path.return_value = config_path
        mock_create_config_file.return_value = config_path

        main(["install-launchd"])

        mock_install_launchd.assert_called_once_with(config_path)
