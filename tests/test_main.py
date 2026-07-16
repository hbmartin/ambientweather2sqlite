import io
import json
import sqlite3
import sys
from contextlib import redirect_stdout
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from ambientweather2sqlite.cli.commands import main
from ambientweather2sqlite.cli.parser import parse_args
from ambientweather2sqlite.exceptions import UnsupportedSchemaVersionError
from ambientweather2sqlite.models import AppConfig, RediscoveryConfig


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

    def test_parse_migrate_check_subcommand(self) -> None:
        args = parse_args(["migrate", "--check"])

        self.assertEqual(args.command, "migrate")
        self.assertTrue(args.check)
        self.assertIsNone(args.backup)

    def test_parse_migrate_backup_subcommand(self) -> None:
        args = parse_args(["migrate", "--backup", "before.db"])

        self.assertEqual(args.command, "migrate")
        self.assertFalse(args.check)
        self.assertEqual(args.backup, Path("before.db"))

    def test_parse_migrate_rejects_check_with_backup(self) -> None:
        with self.assertRaises(SystemExit):
            parse_args(["migrate", "--check", "--backup", "before.db"])

    def test_parse_metadata_offline_subcommand(self) -> None:
        args = parse_args(["metadata", "--offline"])

        self.assertEqual(args.command, "metadata")
        self.assertTrue(args.offline)

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
    @patch("builtins.print")
    @patch("ambientweather2sqlite.cli.commands.start_daemon")
    @patch("ambientweather2sqlite.cli.commands.create_database_if_not_exists")
    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_serve_reports_database_open_errors(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
        mock_create_database: Mock,
        mock_start_daemon: Mock,
        mock_print: Mock,
    ) -> None:
        config_path = Path("config.toml")
        mock_get_config_path.return_value = config_path
        mock_create_config_file.return_value = config_path
        mock_load_config.return_value = AppConfig(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
        )
        mock_create_database.side_effect = sqlite3.OperationalError("locked")

        with self.assertRaises(SystemExit) as exc:
            main(["serve"])

        self.assertEqual(exc.exception.code, 1)
        mock_start_daemon.assert_not_called()
        mock_print.assert_called_once_with("locked", file=sys.stderr)

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
            alert_config=None,
            rediscovery_config=RediscoveryConfig(),
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
            alert_config=None,
            rediscovery_config=RediscoveryConfig(),
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


class TestMainConfigErrors(TestCase):
    @patch("builtins.print")
    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_prints_one_line_error_on_invalid_config(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
        mock_print: Mock,
    ):
        config_path = Path("config.toml")
        mock_get_config_path.return_value = config_path
        mock_create_config_file.return_value = config_path
        mock_load_config.side_effect = ValueError(
            "port must be between 1 and 65535",
        )

        with self.assertRaises(SystemExit) as exc:
            main(["status"])

        self.assertEqual(exc.exception.code, 1)
        message = mock_print.call_args.args[0]
        self.assertIn("Configuration error in config.toml", message)
        self.assertIn("port must be between 1 and 65535", message)


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


class TestMainMigrate(TestCase):
    @patch("builtins.print")
    @patch("ambientweather2sqlite.cli.commands.inspect_database_migration")
    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_migrate_check_prints_plan(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
        mock_inspect: Mock,
        mock_print: Mock,
    ) -> None:
        config_path = Path("config.toml")
        mock_get_config_path.return_value = config_path
        mock_create_config_file.return_value = config_path
        mock_load_config.return_value = AppConfig(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
        )
        mock_inspect.return_value.as_dict.return_value = {"required": True}

        with patch("ambientweather2sqlite.cli.commands.Path.exists", return_value=True):
            main(["migrate", "--check"])

        mock_inspect.assert_called_once_with("weather.db")
        mock_print.assert_called_once_with(
            json.dumps({"required": True}, indent=2),
        )

    @patch("builtins.print")
    @patch("ambientweather2sqlite.cli.commands.migrate_database")
    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_migrate_applies_with_requested_backup(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
        mock_migrate: Mock,
        mock_print: Mock,
    ) -> None:
        config_path = Path("config.toml")
        mock_get_config_path.return_value = config_path
        mock_create_config_file.return_value = config_path
        mock_load_config.return_value = AppConfig(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
        )
        mock_migrate.return_value.as_dict.return_value = {"schema_version": 1}

        with patch("ambientweather2sqlite.cli.commands.Path.exists", return_value=True):
            main(["migrate", "--backup", "before.db"])

        mock_migrate.assert_called_once_with(
            "weather.db",
            backup_path="before.db",
        )
        mock_print.assert_called_once_with(
            json.dumps({"schema_version": 1}, indent=2),
        )

    @patch("builtins.print")
    @patch("ambientweather2sqlite.cli.commands.migrate_database")
    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_migrate_applies_with_default_backup(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
        mock_migrate: Mock,
        mock_print: Mock,
    ) -> None:
        config_path = Path("config.toml")
        mock_get_config_path.return_value = config_path
        mock_create_config_file.return_value = config_path
        mock_load_config.return_value = AppConfig(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
        )
        mock_migrate.return_value.as_dict.return_value = {"schema_version": 1}

        with patch("ambientweather2sqlite.cli.commands.Path.exists", return_value=True):
            main(["migrate"])

        mock_migrate.assert_called_once_with("weather.db", backup_path=None)
        mock_print.assert_called_once_with(
            json.dumps({"schema_version": 1}, indent=2),
        )

    @patch("builtins.print")
    @patch("ambientweather2sqlite.cli.commands.inspect_database_migration")
    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_migrate_reports_domain_errors_without_traceback(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
        mock_inspect: Mock,
        mock_print: Mock,
    ) -> None:
        config_path = Path("config.toml")
        mock_get_config_path.return_value = config_path
        mock_create_config_file.return_value = config_path
        mock_load_config.return_value = AppConfig(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
        )
        mock_inspect.side_effect = UnsupportedSchemaVersionError(2, 1)

        with (
            patch("ambientweather2sqlite.cli.commands.Path.exists", return_value=True),
            self.assertRaises(SystemExit) as exc,
        ):
            main(["migrate", "--check"])

        self.assertEqual(exc.exception.code, 1)
        self.assertIn("newer", mock_print.call_args.args[0])


class TestMainMetadata(TestCase):
    @patch("builtins.print")
    @patch("ambientweather2sqlite.cli.commands.metadata_path")
    @patch(
        "ambientweather2sqlite.cli.commands.create_metadata",
        return_value=({"outTemp": "Outdoor Temperature"}, {}),
    )
    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_metadata_refreshes_online(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
        mock_create_metadata: Mock,
        mock_metadata_path: Mock,
        mock_print: Mock,
    ) -> None:
        config_path = Path("config.toml")
        sidecar_path = Path("weather_metadata.json")
        mock_get_config_path.return_value = config_path
        mock_create_config_file.return_value = config_path
        mock_load_config.return_value = AppConfig(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
        )
        mock_metadata_path.return_value = sidecar_path

        with patch("ambientweather2sqlite.cli.commands.Path.exists", return_value=True):
            main(["metadata"])

        mock_create_metadata.assert_called_once_with(
            "weather.db",
            "http://127.0.0.1/livedata.htm",
        )
        mock_metadata_path.assert_called_once_with("weather.db")
        mock_print.assert_called_once_with(f"Metadata written to {sidecar_path}")

    @patch("builtins.print")
    @patch("ambientweather2sqlite.cli.commands.create_metadata")
    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_metadata_reports_domain_errors_without_traceback(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
        mock_create_metadata: Mock,
        mock_print: Mock,
    ) -> None:
        config_path = Path("config.toml")
        mock_get_config_path.return_value = config_path
        mock_create_config_file.return_value = config_path
        mock_load_config.return_value = AppConfig(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
        )
        mock_create_metadata.side_effect = UnsupportedSchemaVersionError(2, 1)

        with (
            patch("ambientweather2sqlite.cli.commands.Path.exists", return_value=True),
            self.assertRaises(SystemExit) as exc,
        ):
            main(["metadata"])

        self.assertEqual(exc.exception.code, 1)
        self.assertIn("Metadata generation failed", mock_print.call_args.args[0])

    @patch("builtins.print")
    @patch("ambientweather2sqlite.cli.commands.write_metadata_from_database")
    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_metadata_offline_regenerates_sidecar(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
        mock_write_metadata: Mock,
        mock_print: Mock,
    ) -> None:
        config_path = Path("config.toml")
        metadata = Path("weather_metadata.json")
        mock_get_config_path.return_value = config_path
        mock_create_config_file.return_value = config_path
        mock_load_config.return_value = AppConfig(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
        )
        mock_write_metadata.return_value = metadata

        with patch("ambientweather2sqlite.cli.commands.Path.exists", return_value=True):
            main(["metadata", "--offline"])

        mock_write_metadata.assert_called_once_with(
            "weather.db",
            "http://127.0.0.1/livedata.htm",
        )
        mock_print.assert_called_once_with(f"Metadata written to {metadata}")

    @patch("builtins.print")
    @patch(
        "ambientweather2sqlite.cli.commands.write_metadata_from_database",
        return_value=None,
    )
    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_metadata_offline_reports_empty_database(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
        mock_write_metadata: Mock,
        mock_print: Mock,
    ) -> None:
        config_path = Path("config.toml")
        mock_get_config_path.return_value = config_path
        mock_create_config_file.return_value = config_path
        mock_load_config.return_value = AppConfig(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
        )

        with (
            patch("ambientweather2sqlite.cli.commands.Path.exists", return_value=True),
            self.assertRaises(SystemExit) as exc,
        ):
            main(["metadata", "--offline"])

        self.assertEqual(exc.exception.code, 1)
        mock_write_metadata.assert_called_once_with(
            "weather.db",
            "http://127.0.0.1/livedata.htm",
        )
        mock_print.assert_called_once_with("Metadata refresh failed", file=sys.stderr)

    @patch("builtins.print")
    @patch("ambientweather2sqlite.cli.commands.create_metadata", return_value=({}, {}))
    @patch("ambientweather2sqlite.cli.commands.load_config")
    @patch("ambientweather2sqlite.cli.commands.create_config_file")
    @patch("ambientweather2sqlite.cli.commands.get_config_path")
    def test_main_metadata_reports_empty_refresh(
        self,
        mock_get_config_path: Mock,
        mock_create_config_file: Mock,
        mock_load_config: Mock,
        mock_create_metadata: Mock,
        mock_print: Mock,
    ) -> None:
        config_path = Path("config.toml")
        mock_get_config_path.return_value = config_path
        mock_create_config_file.return_value = config_path
        mock_load_config.return_value = AppConfig(
            live_data_url="http://127.0.0.1/livedata.htm",
            database_path="weather.db",
        )

        with (
            patch("ambientweather2sqlite.cli.commands.Path.exists", return_value=True),
            self.assertRaises(SystemExit) as exc,
        ):
            main(["metadata"])

        self.assertEqual(exc.exception.code, 1)
        mock_create_metadata.assert_called_once_with(
            "weather.db",
            "http://127.0.0.1/livedata.htm",
        )
        mock_print.assert_called_once_with("Metadata refresh failed", file=sys.stderr)


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
