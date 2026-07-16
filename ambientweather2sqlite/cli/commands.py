import argparse
import json
import sqlite3
import sys
import tomllib
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from ambientweather2sqlite.configuration import (
    create_config_file,
    get_config_path,
    load_config,
)
from ambientweather2sqlite.daemon import fetch_once, start_daemon
from ambientweather2sqlite.database import (
    backup_database,
    create_database_if_not_exists,
    export_observations,
    inspect_database_migration,
    migrate_database,
    query_db_metrics,
    query_observation_gaps,
)
from ambientweather2sqlite.exceptions import Aw2SqliteError
from ambientweather2sqlite.metadata import (
    create_metadata,
    metadata_path,
    write_metadata_from_database,
)
from ambientweather2sqlite.models import AppConfig

from .parser import parse_args


def _resolve_config(args: argparse.Namespace) -> Path:
    """Resolve config path, creating if necessary."""
    default_config_path = getattr(args, "config_path", None) or get_config_path()
    return create_config_file(default_config_path)


def _load_config_or_exit(config_path: Path) -> AppConfig:
    """Load the config, printing a one-line error instead of a traceback."""
    try:
        return load_config(config_path)
    except (TypeError, ValueError, OSError, tomllib.TOMLDecodeError) as e:
        print(f"Configuration error in {config_path}: {e}", file=sys.stderr)
        sys.exit(1)


@contextmanager
def _report_command_errors(action: str | None) -> Iterator[None]:
    """Print expected command failures without exposing a traceback."""
    try:
        yield
    except (Aw2SqliteError, OSError, sqlite3.Error) as e:
        message = f"{action} failed: {e}" if action is not None else str(e)
        print(message, file=sys.stderr)
        sys.exit(1)


def _cmd_serve(args: argparse.Namespace) -> None:
    config_path = _resolve_config(args)
    config = _load_config_or_exit(config_path)
    with _report_command_errors(None):
        create_database_if_not_exists(config.database_path)
    start_daemon(
        live_data_url=config.live_data_url,
        database_path=config.database_path,
        port=args.port if args.port is not None else config.port,
        log_format=args.log_format or config.log_format,
        host=args.host or config.host,
        auth_token=config.auth_token,
        mqtt_config=config.mqtt,
        alert_config=config.alerts,
        rediscovery_config=config.rediscovery,
    )


def _cmd_config(args: argparse.Namespace) -> None:
    config_path = args.config_path
    created_path = create_config_file(config_path, overwrite_existing=True)
    print(f"Configuration saved to {created_path}")


def _cmd_once(args: argparse.Namespace) -> None:
    config_path = _resolve_config(args)
    config = _load_config_or_exit(config_path)
    fetch_once(config.live_data_url)


def _require_database(database_path: str) -> None:
    """Exit with an error if the configured database does not exist."""
    if not Path(database_path).exists():
        print(f"Database not found at {database_path}", file=sys.stderr)
        sys.exit(1)


def _cmd_status(args: argparse.Namespace) -> None:
    config_path = _resolve_config(args)
    config = _load_config_or_exit(config_path)
    _require_database(config.database_path)
    metrics = query_db_metrics(config.database_path)
    gaps = query_observation_gaps(
        config.database_path,
        hours=args.gap_hours,
        threshold_seconds=args.gap_threshold,
    )
    print(json.dumps({**metrics, "gaps": gaps}, indent=2))


def _cmd_export(args: argparse.Namespace) -> None:
    config_path = _resolve_config(args)
    config = _load_config_or_exit(config_path)
    _require_database(config.database_path)

    if args.output is None:
        export_observations(
            config.database_path,
            sys.stdout,
            fmt=args.export_format,
            start=args.start,
            end=args.end,
        )
        return

    with args.output.open("w", encoding="utf-8") as output:
        row_count = export_observations(
            config.database_path,
            output,
            fmt=args.export_format,
            start=args.start,
            end=args.end,
        )
    print(f"Exported {row_count} observations to {args.output}")


def _cmd_backup(args: argparse.Namespace) -> None:
    config_path = _resolve_config(args)
    config = _load_config_or_exit(config_path)
    _require_database(config.database_path)
    with _report_command_errors("Backup"):
        backup_size = backup_database(config.database_path, str(args.destination))
    print(f"Backed up {backup_size} bytes to {args.destination}")


def _cmd_migrate(args: argparse.Namespace) -> None:
    config_path = _resolve_config(args)
    config = _load_config_or_exit(config_path)
    _require_database(config.database_path)
    with _report_command_errors("Migration"):
        if args.check:
            plan = inspect_database_migration(config.database_path)
            print(json.dumps(plan.as_dict(), indent=2))
            return
        result = migrate_database(
            config.database_path,
            backup_path=str(args.backup) if args.backup is not None else None,
        )
        print(json.dumps(result.as_dict(), indent=2))


def _cmd_metadata(args: argparse.Namespace) -> None:
    config_path = _resolve_config(args)
    config = _load_config_or_exit(config_path)
    _require_database(config.database_path)
    with _report_command_errors("Metadata generation"):
        if args.offline:
            path = write_metadata_from_database(
                config.database_path,
                config.live_data_url,
            )
            if path is None:
                print("Metadata refresh failed", file=sys.stderr)
                sys.exit(1)
        else:
            labels, _ = create_metadata(
                config.database_path,
                config.live_data_url,
            )
            if not labels:
                print("Metadata refresh failed", file=sys.stderr)
                sys.exit(1)
            path = metadata_path(config.database_path)
    print(f"Metadata written to {path}")


def _cmd_install_launchd(args: argparse.Namespace) -> None:
    from ambientweather2sqlite.launchd import install_launchd

    config_path = _resolve_config(args)
    install_launchd(config_path)


def _cmd_install_systemd(args: argparse.Namespace) -> None:
    from ambientweather2sqlite.systemd import install_systemd

    config_path = _resolve_config(args)
    install_systemd(config_path)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    handlers = {
        "serve": _cmd_serve,
        "config": _cmd_config,
        "once": _cmd_once,
        "status": _cmd_status,
        "export": _cmd_export,
        "backup": _cmd_backup,
        "migrate": _cmd_migrate,
        "metadata": _cmd_metadata,
        "install-launchd": _cmd_install_launchd,
        "install-systemd": _cmd_install_systemd,
    }
    if (handler := handlers.get(args.command)) is None:
        msg = f"No handler registered for command: {args.command!r}"
        raise AssertionError(msg)
    handler(args)
