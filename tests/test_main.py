import io
from contextlib import redirect_stdout
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from ambientweather2sqlite.__main__ import main, parse_args
from ambientweather2sqlite.models import AppConfig


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
    @patch("ambientweather2sqlite.__main__.start_daemon")
    @patch("ambientweather2sqlite.__main__.create_database_if_not_exists")
    @patch("ambientweather2sqlite.__main__.load_config")
    @patch("ambientweather2sqlite.__main__.create_config_file")
    @patch("ambientweather2sqlite.__main__.get_config_path")
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
        )

    @patch("ambientweather2sqlite.__main__.start_daemon")
    @patch("ambientweather2sqlite.__main__.create_database_if_not_exists")
    @patch("ambientweather2sqlite.__main__.load_config")
    @patch("ambientweather2sqlite.__main__.create_config_file")
    @patch("ambientweather2sqlite.__main__.get_config_path")
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
        )


class TestMainOnce(TestCase):
    @patch("ambientweather2sqlite.__main__.fetch_once")
    @patch("ambientweather2sqlite.__main__.load_config")
    @patch("ambientweather2sqlite.__main__.create_config_file")
    @patch("ambientweather2sqlite.__main__.get_config_path")
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
    @patch("ambientweather2sqlite.__main__.create_config_file")
    def test_main_config_overwrites_existing_file(self, mock_create_config_file: Mock):
        config_path = Path("config.toml")
        mock_create_config_file.return_value = config_path

        main(["config", "--config", str(config_path)])

        mock_create_config_file.assert_called_once_with(
            config_path,
            overwrite_existing=True,
        )
