from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from ambientweather2sqlite.__main__ import main, parse_args
from ambientweather2sqlite.models import AppConfig


class TestMain(TestCase):
    def test_parse_args_supports_named_flags(self):
        args = parse_args(["--port", "8080", "--config", "1234"])

        self.assertEqual(args.port, 8080)
        self.assertEqual(args.config_path, Path("1234"))

    @patch("ambientweather2sqlite.__main__.start_daemon")
    @patch("ambientweather2sqlite.__main__.create_database_if_not_exists")
    @patch("ambientweather2sqlite.__main__.load_config")
    @patch("ambientweather2sqlite.__main__.create_config_file")
    @patch("ambientweather2sqlite.__main__.get_config_path")
    def test_main_uses_cli_flags_without_type_ambiguity(
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

        main(["--config", str(config_override), "--port", "8080"])

        mock_create_config_file.assert_called_once_with(config_override)
        mock_load_config.assert_called_once_with(resolved_config_path)
        mock_create_database.assert_called_once_with("weather.db")
        mock_start_daemon.assert_called_once_with(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
            port=8080,
        )
