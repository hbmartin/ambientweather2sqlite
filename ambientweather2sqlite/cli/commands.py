import argparse
import json
import sqlite3
import sys
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
    query_db_metrics,
    query_observation_gaps,
)

from .parser import parse_args


def _resolve_config(args: argparse.Namespace) -> Path:
    """Resolve config path, creating if necessary."""
    default_config_path = getattr(args, "config_path", None) or get_config_path()
    return create_config_file(default_config_path)


def _cmd_serve(args: argparse.Namespace) -> None:
    config_path = _resolve_config(args)
    config = load_config(config_path)
    create_database_if_not_exists(config.database_path)
    start_daemon(
        live_data_url=config.live_data_url,
        database_path=config.database_path,
        port=args.port if args.port is not None else config.port,
        log_format=args.log_format or config.log_format,
        host=args.host or config.host,
        auth_token=config.auth_token,
        mqtt_config=config.mqtt,
    )


def _cmd_config(args: argparse.Namespace) -> None:
    config_path = args.config_path
    created_path = create_config_file(config_path, overwrite_existing=True)
    print(f"Configuration saved to {created_path}")


def _cmd_once(args: argparse.Namespace) -> None:
    config_path = _resolve_config(args)
    config = load_config(config_path)
    fetch_once(config.live_data_url)


def _cmd_status(args: argparse.Namespace) -> None:
    config_path = _resolve_config(args)
    config = load_config(config_path)
    if not Path(config.database_path).exists():
        print(f"Database not found at {config.database_path}")
        sys.exit(1)
    metrics = query_db_metrics(config.database_path)
    gaps = query_observation_gaps(
        config.database_path,
        hours=args.gap_hours,
        threshold_seconds=args.gap_threshold,
    )
    print(json.dumps({**metrics, "gaps": gaps}, indent=2))


def _cmd_export(args: argparse.Namespace) -> None:
    config_path = _resolve_config(args)
    config = load_config(config_path)
    if not Path(config.database_path).exists():
        print(f"Database not found at {config.database_path}", file=sys.stderr)
        sys.exit(1)

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
    config = load_config(config_path)
    if not Path(config.database_path).exists():
        print(f"Database not found at {config.database_path}", file=sys.stderr)
        sys.exit(1)
    try:
        backup_size = backup_database(config.database_path, str(args.destination))
    except (FileExistsError, sqlite3.Error) as e:
        print(f"Backup failed: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"Backed up {backup_size} bytes to {args.destination}")


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
    match args.command:
        case "serve":
            _cmd_serve(args)
        case "config":
            _cmd_config(args)
        case "once":
            _cmd_once(args)
        case "status":
            _cmd_status(args)
        case "export":
            _cmd_export(args)
        case "backup":
            _cmd_backup(args)
        case "install-launchd":
            _cmd_install_launchd(args)
        case "install-systemd":
            _cmd_install_systemd(args)
