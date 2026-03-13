"""Tests for configuration.py interactive wizard paths."""

import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from ambientweather2sqlite.configuration import (
    _optional_str,
    create_config_file,
    get_config_path,
    load_config,
)


class TestLoadConfig(TestCase):
    def test_load_config_full(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "test.toml"
            config_path.write_text(
                'live_data_url = "http://192.168.0.1/livedata.htm"\n'
                'database_path = "/tmp/test.db"\n'
                "port = 8080\n"
                'log_format = "json"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

            self.assertEqual(config.live_data_url, "http://192.168.0.1/livedata.htm")
            self.assertEqual(config.database_path, "/tmp/test.db")
            self.assertEqual(config.port, 8080)
            self.assertEqual(config.log_format, "json")

    def test_load_config_defaults_log_format_to_text(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "test.toml"
            config_path.write_text(
                'live_data_url = "http://192.168.0.1/livedata.htm"\n'
                'database_path = "/tmp/test.db"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

            self.assertEqual(config.log_format, "text")

    def test_load_config_rejects_boolean_port(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "test.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "port = true\n",
                encoding="utf-8",
            )

            with self.assertRaises(TypeError):
                load_config(config_path)

    def test_load_config_rejects_missing_url(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "test.toml"
            config_path.write_text(
                'database_path = "/tmp/test.db"\n',
                encoding="utf-8",
            )

            with self.assertRaises(TypeError):
                load_config(config_path)

    def test_load_config_rejects_non_string_log_format(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "test.toml"
            config_path.write_text(
                'live_data_url = "http://192.168.0.1/livedata.htm"\n'
                'database_path = "/tmp/test.db"\n'
                "log_format = 42\n",
                encoding="utf-8",
            )

            with self.assertRaises(TypeError):
                load_config(config_path)

    def test_load_config_rejects_unknown_log_format(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "test.toml"
            config_path.write_text(
                'live_data_url = "http://192.168.0.1/livedata.htm"\n'
                'database_path = "/tmp/test.db"\n'
                'log_format = "yaml"\n',
                encoding="utf-8",
            )

            with self.assertRaises(ValueError):
                load_config(config_path)


class TestOptionalStr(TestCase):
    def test_returns_default_when_missing(self):
        result = _optional_str({}, "key", "default")
        self.assertEqual(result, "default")

    def test_returns_value_when_present(self):
        result = _optional_str({"key": "value"}, "key", "default")
        self.assertEqual(result, "value")

    def test_raises_for_non_string(self):
        with self.assertRaises(TypeError):
            _optional_str({"key": 42}, "key", "default")


class TestGetConfigPath(TestCase):
    def test_returns_none_when_no_config_files_exist(self):
        with (
            tempfile.TemporaryDirectory() as temp_dir,
            patch(
                "ambientweather2sqlite.configuration._CURRENT_PATH",
                Path(temp_dir),
            ),
            patch(
                "ambientweather2sqlite.configuration.Path.home",
                return_value=Path(temp_dir),
            ),
        ):
            result = get_config_path()

            self.assertIsNone(result)

    def test_returns_cwd_config_when_exists(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text("", encoding="utf-8")
            with patch(
                "ambientweather2sqlite.configuration._CURRENT_PATH",
                Path(temp_dir),
            ):
                result = get_config_path()

                self.assertEqual(result, config_path)


class TestCreateConfigFile(TestCase):
    def test_returns_existing_path_without_prompting(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "existing.toml"
            config_path.write_text("test", encoding="utf-8")

            result = create_config_file(config_path)

            self.assertEqual(result, config_path)

    @patch("builtins.input")
    def test_overwrites_existing_path_when_requested(self, mock_input):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "existing.toml"
            config_path.write_text("test", encoding="utf-8")
            mock_input.side_effect = [
                "n",
                "http://192.168.0.99/livedata.htm",
                str(Path(temp_dir) / "replacement.db"),
                "",
            ]

            result = create_config_file(config_path, overwrite_existing=True)

            self.assertEqual(result, config_path)
            content = result.read_text(encoding="utf-8")
            self.assertIn("http://192.168.0.99/livedata.htm", content)

    @patch("builtins.input")
    def test_returns_existing_default_path_without_overwriting(self, mock_input):
        with tempfile.TemporaryDirectory() as temp_dir:
            current_path = Path(temp_dir)
            config_path = current_path / "aw2sqlite.toml"
            config_path.write_text("existing", encoding="utf-8")
            mock_input.side_effect = [
                "n",
                "http://192.168.0.99/livedata.htm",
                str(current_path / "replacement.db"),
                "",
                "",
            ]

            with patch(
                "ambientweather2sqlite.configuration._CURRENT_PATH",
                current_path,
            ):
                result = create_config_file(None)

            self.assertEqual(result, config_path)
            self.assertEqual(result.read_text(encoding="utf-8"), "existing")

    @patch("builtins.input")
    def test_returns_existing_prompted_path_without_overwriting(self, mock_input):
        with tempfile.TemporaryDirectory() as temp_dir:
            current_path = Path(temp_dir)
            output_path = current_path / "existing.toml"
            output_path.write_text("existing", encoding="utf-8")
            mock_input.side_effect = [
                "n",
                "http://192.168.0.99/livedata.htm",
                str(current_path / "replacement.db"),
                "",
                str(output_path),
            ]

            with patch(
                "ambientweather2sqlite.configuration._CURRENT_PATH",
                current_path,
            ):
                result = create_config_file(None)

            self.assertEqual(result, output_path)
            self.assertEqual(result.read_text(encoding="utf-8"), "existing")

    @patch("builtins.input")
    def test_creates_config_file_with_manual_url(self, mock_input):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "aw2sqlite.toml"
            mock_input.side_effect = [
                "n",  # Don't auto-scan
                "http://192.168.0.1/livedata.htm",  # URL
                str(Path(temp_dir) / "test.db"),  # DB path
                "8080",  # Port
                str(output_path),  # Output file
            ]

            result = create_config_file(None)

            self.assertTrue(result.exists())
            content = result.read_text(encoding="utf-8")
            self.assertIn("http://192.168.0.1/livedata.htm", content)
            self.assertIn("8080", content)

    @patch("builtins.input")
    def test_creates_config_without_port(self, mock_input):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "aw2sqlite.toml"
            mock_input.side_effect = [
                "n",  # Don't auto-scan
                "http://192.168.0.1/livedata.htm",  # URL
                str(Path(temp_dir) / "test.db"),  # DB path
                "",  # No port
                str(output_path),  # Output file
            ]

            result = create_config_file(None)

            content = result.read_text(encoding="utf-8")
            self.assertNotIn("port", content)

    @patch("builtins.input")
    def test_creates_config_with_explicit_path(self, mock_input):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "custom.toml"
            mock_input.side_effect = [
                "n",  # Don't auto-scan
                "http://192.168.0.1/livedata.htm",  # URL
                "",  # Default DB path
                "",  # No port
            ]

            result = create_config_file(output_path)

            self.assertEqual(result, output_path)
            self.assertTrue(result.exists())

    @patch("ambientweather2sqlite.scanner.scan_for_stations")
    @patch("builtins.input")
    def test_creates_config_with_auto_scan_single_station(self, mock_input, mock_scan):
        mock_scan.return_value = ["http://192.168.0.10/livedata.htm"]
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "aw2sqlite.toml"
            mock_input.side_effect = [
                "y",  # Auto-scan
                str(Path(temp_dir) / "test.db"),  # DB path
                "",  # No port
                str(output_path),  # Output file
            ]

            result = create_config_file(None)

            content = result.read_text(encoding="utf-8")
            self.assertIn("http://192.168.0.10/livedata.htm", content)

    @patch("ambientweather2sqlite.scanner.scan_for_stations")
    @patch("builtins.input")
    def test_creates_config_with_auto_scan_multiple_stations(
        self,
        mock_input,
        mock_scan,
    ):
        mock_scan.return_value = [
            "http://192.168.0.10/livedata.htm",
            "http://192.168.0.20/livedata.htm",
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "aw2sqlite.toml"
            mock_input.side_effect = [
                "y",  # Auto-scan
                "2",  # Select second station
                str(Path(temp_dir) / "test.db"),  # DB path
                "",  # No port
                str(output_path),  # Output file
            ]

            result = create_config_file(None)

            content = result.read_text(encoding="utf-8")
            self.assertIn("http://192.168.0.20/livedata.htm", content)

    @patch("builtins.input")
    @patch("ambientweather2sqlite.scanner.detect_local_subnet")
    def test_falls_back_to_manual_url_when_auto_scan_fails(
        self,
        mock_detect,
        mock_input,
    ):
        mock_detect.side_effect = OSError("offline")
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "aw2sqlite.toml"
            mock_input.side_effect = [
                "y",  # Auto-scan
                "http://192.168.0.99/livedata.htm",  # Manual URL after fallback
                str(Path(temp_dir) / "test.db"),  # DB path
                "",  # No port
                str(output_path),  # Output file
            ]

            result = create_config_file(None)

            content = result.read_text(encoding="utf-8")
            self.assertIn("http://192.168.0.99/livedata.htm", content)
