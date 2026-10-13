import tempfile
import tomllib
from collections.abc import Iterator
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from hypothesis import example, given
from hypothesis import strategies as st

from ambientweather2sqlite import configuration
from ambientweather2sqlite.configuration import load_config
from ambientweather2sqlite.models import AppConfig, RecoveryConfig


class TestTomlStrings(TestCase):
    @given(value=st.text(alphabet=st.characters(exclude_categories=("Cs",))))
    @example(value='\x00\b\f\n\r\t\x1f\x7f"\\')
    @example(value='気象🌧️ "rain" ' + r"\u0061\n\t")
    def test_encoded_strings_round_trip_through_toml(self, value: str) -> None:
        encoded = configuration._toml_string(value)  # noqa: SLF001
        self.assertEqual(tomllib.loads(f"value = {encoded}")["value"], value)

    @given(
        prefix=st.text(alphabet=st.characters(exclude_categories=("Cs",))),
        surrogate=st.integers(min_value=0xD800, max_value=0xDFFF),
        suffix=st.text(alphabet=st.characters(exclude_categories=("Cs",))),
    )
    def test_encoder_rejects_surrogates(
        self, prefix: str, surrogate: int, suffix: str
    ) -> None:
        with self.assertRaisesRegex(ValueError, "surrogate characters"):
            configuration._toml_string(prefix + chr(surrogate) + suffix)  # noqa: SLF001


class TestRecoveryConfiguration(TestCase):
    def _load(self, recovery: str = "") -> RecoveryConfig:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text(
                'live_data_url = "http://station/livedata.htm"\n'
                'database_path = "/tmp/weather.db"\n' + recovery,
                encoding="utf-8",
            )
            return load_config(path).recovery

    def test_defaults_enable_embedded_monitor(self) -> None:
        self.assertEqual(self._load(), RecoveryConfig())

    def test_custom_monitor_settings_and_disable(self) -> None:
        self.assertEqual(
            self._load(
                "[recovery]\nenabled = false\nstale_seconds = 600\n"
                "check_interval_seconds = 30\nrecovery_observations = 4\n",
            ),
            RecoveryConfig(
                enabled=False,
                stale_seconds=600,
                check_interval_seconds=30,
                recovery_observations=4,
            ),
        )

    def test_invalid_settings_are_rejected(self) -> None:
        for key in ("stale_seconds", "check_interval_seconds", "recovery_observations"):
            for value in ("0", "-1", "true", "1.5", '"bad"'):
                with (
                    self.subTest(key=key, value=value),
                    self.assertRaises(
                        (TypeError, ValueError),
                    ),
                ):
                    self._load(f"[recovery]\n{key} = {value}\n")
        with self.assertRaises(TypeError):
            self._load("[recovery]\nenabled = 1\n")
        with self.assertRaises(TypeError):
            self._load("recovery = true\n")


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

    def test_load_config_defaults_rediscovery_when_table_absent(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n',
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertTrue(config.rediscovery.enabled)
        self.assertEqual(config.rediscovery.failure_threshold, 2)

    def test_load_config_parses_rediscovery_table(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "\n"
                "[rediscovery]\n"
                "enabled = false\n"
                "failure_threshold = 5\n",
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertFalse(config.rediscovery.enabled)
        self.assertEqual(config.rediscovery.failure_threshold, 5)

    def test_load_config_rejects_zero_rediscovery_failure_threshold(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "\n"
                "[rediscovery]\n"
                "failure_threshold = 0\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError,
                "rediscovery.failure_threshold must be at least 1",
            ):
                load_config(config_path)

    def test_load_config_rejects_non_bool_rediscovery_enabled(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "\n"
                "[rediscovery]\n"
                'enabled = "yes"\n',
                encoding="utf-8",
            )

            with self.assertRaisesRegex(TypeError, "enabled must be a boolean"):
                load_config(config_path)

    def test_load_config_rejects_non_table_rediscovery(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                'rediscovery = "always"\n',
                encoding="utf-8",
            )

            with self.assertRaisesRegex(TypeError, "rediscovery must be a table"):
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

    def test_setup_preserves_mixed_case_url_and_reprompts_with_diagnostics(
        self,
    ) -> None:
        invalid_urls = (
            ("httpfoo", "Live data URL must start with http:// or https://"),
            ("http://", "Invalid live data URL: URL is missing a hostname"),
            (
                "http://station.local:0/",
                "Invalid live data URL: URL port must be between 1 and 65535",
            ),
            (
                "http://station.local:abc:70000/",
                "Invalid live data URL: Port could not be cast",
            ),
            (
                "http://station local/",
                "Invalid live data URL: URL must not contain whitespace or ASCII control characters",
            ),
        )
        for url in (
            "HTTP://Observer.LOCAL:8080/live?raw=1#top",
            "hTtPs://[fe80::1%ETH0]:65535/live",
        ):
            with self.subTest(url=url), tempfile.TemporaryDirectory() as directory:
                output_path = Path(directory) / "aw2sqlite.toml"
                with (
                    patch("builtins.print") as output,
                    patch(
                        "builtins.input",
                        side_effect=[
                            "n",
                            "",
                            "  \t  ",
                            *(invalid for invalid, _ in invalid_urls),
                            f"  {url}  ",
                            "/tmp/weather.db",
                            "",
                        ],
                    ) as prompt,
                    patch("socket.getaddrinfo") as dns,
                    patch("ambientweather2sqlite.station_http._receiver_lock") as lock,
                    patch("ambientweather2sqlite.mureq.get") as get,
                    patch("ambientweather2sqlite.mureq.post") as post,
                ):
                    created = configuration.create_config_file(output_path)
                    config = load_config(created)
                self.assertEqual(prompt.call_count, len(invalid_urls) + 6)
                self.assertEqual(config.live_data_url, url)
                self.assertIn(
                    f'live_data_url = "{url}"', created.read_text(encoding="utf-8")
                )
                diagnostics = [str(call.args[0]) for call in output.call_args_list]
                self.assertEqual(len(diagnostics), len(invalid_urls) + 2)
                for actual, (_, expected) in zip(
                    diagnostics[2:], invalid_urls, strict=True
                ):
                    self.assertIn(expected, actual)
                for operation in (dns, lock, get, post):
                    operation.assert_not_called()

    def test_setup_strings_survive_toml_and_config_loading(self) -> None:
        cases = (
            ('http://station.local/live?name="rain"', '/tmp/weather"station".db'),
            (r"http://station.local/live\weather?name=\path", r"C:\Weather\station.db"),
            (r"http://station.local/live?name=\u0061\n\t", r"/tmp/\u0061\n\t.db"),
            ("HTTP://BÜCHER.local/気象🌧️?name=☀️", "/tmp/気象🌧️.db"),
            (
                'https://station.local/🌧️?name="rain"' + r"\u0061",
                'weather"🌧️\\\x00\x01\b\f\n\r\t\x1f\x7f.db',
            ),
        )
        for url, database_path in cases:
            with self.subTest(url=url), tempfile.TemporaryDirectory() as directory:
                output_path = Path(directory) / "aw2sqlite.toml"
                with (
                    patch(
                        "builtins.input",
                        side_effect=[
                            "n",
                            f"  {url}  ",
                            f"  {database_path}  ",
                            "8080",
                        ],
                    ),
                    patch("builtins.print") as output,
                    patch("socket.getaddrinfo") as dns,
                    patch("ambientweather2sqlite.station_http._receiver_lock") as lock,
                    patch("ambientweather2sqlite.mureq.get") as get,
                    patch("ambientweather2sqlite.mureq.post") as post,
                ):
                    created = configuration.create_config_file(output_path)
                    parsed = tomllib.loads(created.read_text(encoding="utf-8"))
                    loaded = load_config(created)
                self.assertEqual(created, output_path)
                self.assertEqual(parsed["live_data_url"], url)
                self.assertEqual(parsed["database_path"], database_path)
                self.assertEqual(loaded.live_data_url, url)
                self.assertEqual(loaded.database_path, database_path)
                self.assertEqual(loaded.port, 8080)
                self.assertEqual(output.call_count, 2)
                for operation in (dns, lock, get, post):
                    operation.assert_not_called()

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


class TestSetupInputValidation(TestCase):
    def test_invalid_ports_reprompt_before_saving_a_loadable_config(self) -> None:
        invalid_ports = (
            "oops",
            "8080.0",
            "+8080",
            "-1",
            "0x1f90",
            "8_080",
            "true",
            "８０８０",  # noqa: RUF001
            "٨٠٨٠",
            "0",
            "000",
            "65536",
            "9" * 5000,
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            responses = iter(
                [
                    "n",
                    "http://station.local/",
                    "weather.db",
                    *invalid_ports,
                    "008080",
                ]
            )

            def prompt(_message: str) -> str:
                self.assertFalse(path.exists())
                return next(responses)

            with (
                patch("builtins.input", side_effect=prompt) as inputs,
                patch("builtins.print") as output,
            ):
                created = configuration.create_config_file(path)
            self.assertEqual(inputs.call_count, len(invalid_ports) + 4)
            self.assertEqual(load_config(created).port, 8080)
            self.assertTrue(
                created.read_text(encoding="utf-8").endswith("port = 8080\n")
            )
            diagnostics = [str(call.args[0]) for call in output.call_args_list][2:]
            self.assertEqual(len(diagnostics), len(invalid_ports))
            for diagnostic in diagnostics:
                self.assertTrue(
                    "decimal digits" in diagnostic
                    or "between 1 and 65535" in diagnostic
                )

    def test_valid_ports_are_saved_as_canonical_integers_or_omitted(self) -> None:
        for entered, expected in (
            ("", None),
            ("   ", None),
            ("1", 1),
            ("65535", 65535),
            (" 00080 ", 80),
            ("0" * 5000 + "443", 443),
        ):
            with (
                self.subTest(port=entered[:20]),
                tempfile.TemporaryDirectory() as directory,
            ):
                path = Path(directory) / "config.toml"
                with (
                    patch(
                        "builtins.input",
                        side_effect=[
                            "n",
                            "http://station.local/",
                            "weather.db",
                            entered,
                        ],
                    ),
                    patch("builtins.print") as output,
                ):
                    created = configuration.create_config_file(path)
                parsed = tomllib.loads(created.read_text(encoding="utf-8"))
                self.assertEqual(load_config(created).port, expected)
                if expected is None:
                    self.assertNotIn("port", parsed)
                else:
                    self.assertIs(type(parsed["port"]), int)
                    self.assertEqual(parsed["port"], expected)
                self.assertEqual(output.call_count, 2)

    def test_existing_toml_integer_syntax_remains_supported(self) -> None:
        for spelling in ("0x1f90", "+8080", "8_080"):
            with (
                self.subTest(port=spelling),
                tempfile.TemporaryDirectory() as directory,
            ):
                path = Path(directory) / "config.toml"
                path.write_text(
                    'live_data_url = "http://station.local/"\n'
                    'database_path = "weather.db"\n'
                    f"port = {spelling}\n",
                    encoding="utf-8",
                )
                self.assertEqual(load_config(path).port, 8080)

    def test_surrogate_inputs_reprompt_without_creating_or_truncating_output(
        self,
    ) -> None:
        for surrogate in (
            b"\xff".decode("utf-8", "surrogateescape"),
            "\ud800",
            "\udfff",
        ):
            for existing in (False, True):
                with (
                    self.subTest(surrogate=ascii(surrogate), existing=existing),
                    tempfile.TemporaryDirectory() as directory,
                ):
                    path = Path(directory) / "config.toml"
                    original = b"original = true\n"
                    if existing:
                        path.write_bytes(original)
                    responses = iter(
                        [
                            "n",
                            "http://station.local/" + surrogate,
                            "http://station.local/",
                            "weather" + surrogate + ".db",
                            "  気象🌧️.db  ",
                            "",
                        ]
                    )

                    def prompt(
                        _message: str,
                        *,
                        target: Path = path,
                        expected: bytes | None = original if existing else None,
                        answers: Iterator[str] = responses,
                    ) -> str:
                        if expected is not None:
                            self.assertEqual(target.read_bytes(), expected)
                        else:
                            self.assertFalse(target.exists())
                        return next(answers)

                    with (
                        patch("builtins.input", side_effect=prompt) as inputs,
                        patch("builtins.print") as output,
                    ):
                        created = configuration.create_config_file(
                            path, overwrite_existing=True
                        )
                    config = load_config(created)
                    self.assertEqual(config.live_data_url, "http://station.local/")
                    self.assertEqual(config.database_path, "気象🌧️.db")
                    self.assertEqual(inputs.call_count, 6)
                    diagnostics = [call.args[0] for call in output.call_args_list][2:]
                    self.assertEqual(len(diagnostics), 2)
                    self.assertTrue(diagnostics[0].startswith("Invalid live data URL:"))
                    self.assertTrue(diagnostics[1].startswith("Invalid database path:"))
                    self.assertTrue(
                        all("surrogate characters" in item for item in diagnostics)
                    )

    def test_invalid_database_default_reprompts_for_a_valid_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            responses = iter(["n", "http://station.local/", "", "weather.db", ""])

            def prompt(message: str) -> str:
                message.encode("utf-8")
                self.assertFalse(path.exists())
                return next(responses)

            with (
                patch.object(
                    configuration, "_CURRENT_PATH", Path(directory) / "\udcff"
                ),
                patch(
                    "builtins.input",
                    side_effect=prompt,
                ) as inputs,
                patch("builtins.print") as output,
            ):
                created = configuration.create_config_file(path)
            self.assertEqual(load_config(created).database_path, "weather.db")
            self.assertEqual(inputs.call_count, 5)
            self.assertIn(
                "Invalid database path: surrogate characters", output.call_args.args[0]
            )

    def test_encoder_defense_precedes_output_open_for_direct_invalid_values(
        self,
    ) -> None:
        for field in ("url", "database_path"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "config.toml"
                path.write_bytes(b"original = true\n")
                url = "http://station.local/" + ("\udcff" if field == "url" else "")
                database_path = "weather.db" + (
                    "\ud800" if field == "database_path" else ""
                )
                with (
                    patch.object(configuration, "_prompt_for_url", return_value=url),
                    patch.object(
                        configuration,
                        "_prompt_database_path",
                        return_value=database_path,
                    ),
                    patch("builtins.input", return_value=""),
                    patch("builtins.print"),
                    self.assertRaisesRegex(ValueError, "surrogate characters"),
                ):
                    configuration.create_config_file(path, overwrite_existing=True)
                self.assertEqual(path.read_bytes(), b"original = true\n")


class TestPositiveIntegerInvariant(TestCase):
    def _load(self, section: str, value: str) -> AppConfig:
        key = "stale_seconds" if section == "recovery" else "failure_threshold"
        extra = 'webhook_url = "http://hooks.invalid/"\n' if section == "alerts" else ""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text(
                'live_data_url = "http://station/livedata.htm"\n'
                'database_path = "/tmp/weather.db"\n'
                f"[{section}]\n{extra}{key} = {value}\n",
                encoding="utf-8",
            )
            return load_config(path)

    @given(
        section=st.sampled_from(("alerts", "rediscovery", "recovery")),
        value=st.integers(min_value=-100, max_value=10000),
    )
    def test_positive_settings_accept_exactly_positive_integers(
        self,
        section: str,
        value: int,
    ) -> None:
        key = "stale_seconds" if section == "recovery" else "failure_threshold"
        if value < 1:
            with self.assertRaisesRegex(
                ValueError, rf"{section}\.{key} must be at least 1"
            ):
                self._load(section, str(value))
        else:
            table = getattr(self._load(section, str(value)), section)
            self.assertEqual(getattr(table, key), value)

    def test_boolean_settings_keep_existing_type_error_messages(self) -> None:
        for section in ("alerts", "rediscovery", "recovery"):
            key = "stale_seconds" if section == "recovery" else "failure_threshold"
            with (
                self.subTest(section=section),
                self.assertRaisesRegex(TypeError, f"{key} must be an integer"),
            ):
                self._load(section, "true")
