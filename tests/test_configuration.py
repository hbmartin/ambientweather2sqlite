import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from ambientweather2sqlite import configuration
from ambientweather2sqlite.configuration import load_config


class TestConfiguration(TestCase):
    def test_load_config_parses_valid_config_with_port(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "port = 8080\n",
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(config.live_data_url, "http://127.0.0.1/livedata.htm")
        self.assertEqual(config.database_path, "/tmp/aw2sqlite.db")
        self.assertEqual(config.port, 8080)

    def test_load_config_allows_missing_port(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertIsNone(config.port)

    def test_load_config_rejects_non_string_live_data_url(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = 123\ndatabase_path = "/tmp/aw2sqlite.db"\n',
                encoding="utf-8",
            )

            with self.assertRaisesRegex(TypeError, "live_data_url must be a string"):
                load_config(config_path)

    def test_load_config_rejects_boolean_port(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "port = true\n",
                encoding="utf-8",
            )

            with self.assertRaises(TypeError):
                load_config(config_path)

    def test_get_config_path_prefers_cwd_then_home(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            cwd = root / "cwd"
            home = root / "home"
            cwd.mkdir()
            home.mkdir()
            cwd_config = cwd / "aw2sqlite.toml"
            home_config = home / ".aw2sqlite.toml"

            with (
                patch.object(configuration, "_CURRENT_PATH", cwd),
                patch("pathlib.Path.home", return_value=home),
            ):
                self.assertIsNone(configuration.get_config_path())

                home_config.write_text("live_data_url = 'home'\n", encoding="utf-8")
                self.assertEqual(configuration.get_config_path(), home_config)

                cwd_config.write_text("live_data_url = 'cwd'\n", encoding="utf-8")
                self.assertEqual(configuration.get_config_path(), cwd_config)

    def test_create_config_file_returns_existing_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text("existing = true\n", encoding="utf-8")

            created_path = configuration.create_config_file(config_path)
            config_text = created_path.read_text(encoding="utf-8")

        self.assertEqual(created_path, config_path)
        self.assertEqual(
            config_text,
            "existing = true\n",
        )

    def test_create_config_file_uses_defaults_when_prompts_are_blank(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            current_path = Path(temp_dir)
            expected_path = current_path / "aw2sqlite.toml"
            expected_db_path = current_path / "aw2sqlite.db"

            with (
                patch.object(configuration, "_CURRENT_PATH", current_path),
                patch(
                    "builtins.input",
                    side_effect=[
                        "n",  # Skip auto-scan
                        "not-a-url",
                        "http://127.0.0.1/livedata.htm",
                        "",
                        "",
                        "",
                    ],
                ),
            ):
                created_path = configuration.create_config_file(None)
                config_text = created_path.read_text(encoding="utf-8")

        self.assertEqual(created_path, expected_path)
        self.assertEqual(
            config_text,
            'live_data_url = "http://127.0.0.1/livedata.htm"\n'
            f'database_path = "{expected_db_path}"\n',
        )

    def test_create_config_file_writes_to_explicit_path_with_port(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "custom.toml"

            with patch(
                "builtins.input",
                side_effect=[
                    "n",  # Skip auto-scan
                    "http://127.0.0.1/livedata.htm",
                    "/tmp/weather.db",
                    "8123",
                ],
            ):
                created_path = configuration.create_config_file(output_path)
                config_text = created_path.read_text(encoding="utf-8")

        self.assertEqual(created_path, output_path)
        self.assertEqual(
            config_text,
            'live_data_url = "http://127.0.0.1/livedata.htm"\n'
            'database_path = "/tmp/weather.db"\n'
            "port = 8123\n",
        )

    def test_create_config_file_uses_prompted_output_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            current_path = Path(temp_dir)
            output_path = current_path / "aw2sqlite-custom.toml"

            with (
                patch.object(configuration, "_CURRENT_PATH", current_path),
                patch(
                    "builtins.input",
                    side_effect=[
                        "n",  # Skip auto-scan
                        "http://127.0.0.1/livedata.htm",
                        "",
                        "",
                        str(output_path),
                    ],
                ),
            ):
                created_path = configuration.create_config_file(None)
                config_text = created_path.read_text(encoding="utf-8")

        self.assertEqual(created_path, output_path)
        self.assertEqual(
            config_text,
            'live_data_url = "http://127.0.0.1/livedata.htm"\n'
            f'database_path = "{current_path / "aw2sqlite.db"}"\n',
        )
