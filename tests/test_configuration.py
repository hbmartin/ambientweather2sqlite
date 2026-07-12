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

    def test_load_config_defaults_for_host_auth_and_mqtt(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(config.host, "localhost")
        self.assertIsNone(config.auth_token)
        self.assertIsNone(config.mqtt)

    def test_load_config_parses_host_and_auth_token(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                'host = "0.0.0.0"\n'
                'auth_token = "sekrit"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(config.host, "0.0.0.0")
        self.assertEqual(config.auth_token, "sekrit")

    def test_load_config_parses_mqtt_table(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "\n"
                "[mqtt]\n"
                'host = "broker.local"\n'
                'topic = "weather/observations"\n'
                "port = 8883\n"
                'username = "user"\n'
                'password = "pass"\n'
                "tls = true\n",
                encoding="utf-8",
            )

            config = load_config(config_path)

        assert config.mqtt is not None
        self.assertEqual(config.mqtt.host, "broker.local")
        self.assertEqual(config.mqtt.topic, "weather/observations")
        self.assertEqual(config.mqtt.port, 8883)
        self.assertEqual(config.mqtt.username, "user")
        self.assertEqual(config.mqtt.password, "pass")
        self.assertTrue(config.mqtt.tls)
        self.assertEqual(config.mqtt.client_id, "aw2sqlite")

    def test_load_config_parses_mqtt_discovery_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "\n"
                "[mqtt]\n"
                'host = "broker.local"\n'
                'topic = "weather/observations"\n'
                "discovery = true\n"
                'discovery_prefix = "ha"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        assert config.mqtt is not None
        self.assertTrue(config.mqtt.discovery)
        self.assertEqual(config.mqtt.discovery_prefix, "ha")

    def test_load_config_mqtt_discovery_defaults(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "\n"
                "[mqtt]\n"
                'host = "broker.local"\n'
                'topic = "weather/observations"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        assert config.mqtt is not None
        self.assertFalse(config.mqtt.discovery)
        self.assertEqual(config.mqtt.discovery_prefix, "homeassistant")

    def test_load_config_rejects_non_bool_discovery(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "\n"
                "[mqtt]\n"
                'host = "broker.local"\n'
                'topic = "weather/observations"\n'
                'discovery = "yes"\n',
                encoding="utf-8",
            )

            with self.assertRaisesRegex(TypeError, "discovery must be a boolean"):
                load_config(config_path)

    def test_load_config_parses_alerts_table(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "\n"
                "[alerts]\n"
                'webhook_url = "https://hooks.local/alert"\n'
                "failure_threshold = 5\n"
                "timeout_seconds = 2.5\n",
                encoding="utf-8",
            )

            config = load_config(config_path)

        assert config.alerts is not None
        self.assertEqual(config.alerts.webhook_url, "https://hooks.local/alert")
        self.assertEqual(config.alerts.failure_threshold, 5)
        self.assertEqual(config.alerts.timeout_seconds, 2.5)

    def test_load_config_alerts_defaults(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "\n"
                "[alerts]\n"
                'webhook_url = "https://hooks.local/alert"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        assert config.alerts is not None
        self.assertEqual(config.alerts.failure_threshold, 3)
        self.assertEqual(config.alerts.timeout_seconds, 5.0)

    def test_load_config_defaults_alerts_to_none(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertIsNone(config.alerts)

    def test_load_config_rejects_alerts_without_webhook_url(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "\n"
                "[alerts]\n"
                "failure_threshold = 5\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(TypeError, "webhook_url must be a string"):
                load_config(config_path)

    def test_load_config_rejects_non_http_webhook_url(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "\n"
                "[alerts]\n"
                'webhook_url = "hooks.local/alert"\n',
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError,
                "alerts.webhook_url must start with http:// or https://",
            ):
                load_config(config_path)

    def test_load_config_rejects_non_positive_timeout_seconds(self):
        for bad_timeout in (0, -1.5):
            with (
                self.subTest(timeout=bad_timeout),
                tempfile.TemporaryDirectory() as temp_dir,
            ):
                config_path = Path(temp_dir) / "aw2sqlite.toml"
                config_path.write_text(
                    'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                    'database_path = "/tmp/aw2sqlite.db"\n'
                    "\n"
                    "[alerts]\n"
                    'webhook_url = "https://hooks.local/alert"\n'
                    f"timeout_seconds = {bad_timeout}\n",
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(
                    ValueError,
                    "alerts.timeout_seconds must be greater than 0",
                ):
                    load_config(config_path)

    def test_load_config_rejects_zero_failure_threshold(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "\n"
                "[alerts]\n"
                'webhook_url = "https://hooks.local/alert"\n'
                "failure_threshold = 0\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError,
                "alerts.failure_threshold must be at least 1",
            ):
                load_config(config_path)

    def test_load_config_rejects_mqtt_port_zero(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "\n"
                "[mqtt]\n"
                'host = "broker.local"\n'
                'topic = "weather/observations"\n'
                "port = 0\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError,
                "mqtt.port must be between 1 and 65535",
            ):
                load_config(config_path)

    def test_load_config_rejects_out_of_range_port(self):
        for bad_port in (0, -1, 65536):
            with self.subTest(port=bad_port), tempfile.TemporaryDirectory() as temp_dir:
                config_path = Path(temp_dir) / "aw2sqlite.toml"
                config_path.write_text(
                    'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                    'database_path = "/tmp/aw2sqlite.db"\n'
                    f"port = {bad_port}\n",
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(
                    ValueError,
                    "port must be between 1 and 65535",
                ):
                    load_config(config_path)

    def test_load_config_accepts_valid_port_bounds(self):
        for good_port in (1, 443, 65535):
            with (
                self.subTest(port=good_port),
                tempfile.TemporaryDirectory() as temp_dir,
            ):
                config_path = Path(temp_dir) / "aw2sqlite.toml"
                config_path.write_text(
                    'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                    'database_path = "/tmp/aw2sqlite.db"\n'
                    f"port = {good_port}\n",
                    encoding="utf-8",
                )

                config = load_config(config_path)

                self.assertEqual(config.port, good_port)

    def test_load_config_rejects_non_http_live_data_url(self):
        for bad_url in ("ftp://127.0.0.1/livedata.htm", "station.local"):
            with self.subTest(url=bad_url), tempfile.TemporaryDirectory() as temp_dir:
                config_path = Path(temp_dir) / "aw2sqlite.toml"
                config_path.write_text(
                    f'live_data_url = "{bad_url}"\n'
                    'database_path = "/tmp/aw2sqlite.db"\n',
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(
                    ValueError,
                    "live_data_url must start with http:// or https://",
                ):
                    load_config(config_path)

    def test_load_config_accepts_https_live_data_url(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "https://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(config.live_data_url, "https://127.0.0.1/livedata.htm")

    def test_load_config_rejects_mqtt_without_topic(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "\n"
                "[mqtt]\n"
                'host = "broker.local"\n',
                encoding="utf-8",
            )

            with self.assertRaises(TypeError):
                load_config(config_path)

    def test_load_config_rejects_non_string_host(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "host = 8080\n",
                encoding="utf-8",
            )

            with self.assertRaises(TypeError):
                load_config(config_path)

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
