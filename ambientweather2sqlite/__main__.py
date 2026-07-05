import argparse
import json
import sys
from pathlib import Path

from .configuration import create_config_file, get_config_path, load_config
from .daemon import fetch_once, start_daemon
from .database import (
    create_database_if_not_exists,
    export_observations,
    query_db_metrics,
    query_observation_gaps,
)

_SUBCOMMANDS = {
    "serve",
    "config",
    "once",
    "status",
    "export",
    "install-launchd",
    "install-systemd",
}
_TOP_LEVEL_HELP_FLAGS = {"-h", "--help"}


def _add_config_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config",
        dest="config_path",
        type=Path,
        help="Path to a TOML config file.",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    raw_args = argv if argv is not None else sys.argv[1:]

    # Default bare flags to serve for backward compatibility.
    if not raw_args:
        raw_args = ["serve"]
    elif (
        raw_args[0] not in _SUBCOMMANDS
        and raw_args[0].startswith("-")
        and raw_args[0] not in _TOP_LEVEL_HELP_FLAGS
    ):
        raw_args = ["serve", *raw_args]

    parser = argparse.ArgumentParser(
        prog="aw2sqlite",
        description="AmbientWeather to SQLite — record local weather station data.",
    )
    subparsers = parser.add_subparsers(dest="command")

    # serve
    serve_parser = subparsers.add_parser(
        "serve",
        help="Start the daemon and optional API server.",
    )
    serve_parser.add_argument(
        "--port",
        type=int,
        help="Port number for the HTTP JSON API server.",
    )
    serve_parser.add_argument(
        "--host",
        help="Bind address for the HTTP JSON API server (default: localhost).",
    )
    _add_config_arg(serve_parser)
    serve_parser.add_argument(
        "--log-format",
        choices=["text", "json"],
        default=None,
        help="Log output format (default: text).",
    )

    # config
    config_parser = subparsers.add_parser(
        "config",
        help="Run the configuration wizard.",
    )
    _add_config_arg(config_parser)

    # once
    once_parser = subparsers.add_parser(
        "once",
        help="Fetch a single observation and print it (no DB write).",
    )
    _add_config_arg(once_parser)

    # status
    status_parser = subparsers.add_parser(
        "status",
        help="Show last observation, row count, and DB file size.",
    )
    _add_config_arg(status_parser)
    status_parser.add_argument(
        "--gap-hours",
        type=int,
        default=24,
        help="Window (in hours) to scan for observation gaps (default: 24).",
    )
    status_parser.add_argument(
        "--gap-threshold",
        type=float,
        default=180.0,
        help="Minimum seconds between observations to report a gap (default: 180).",
    )

    # export
    export_parser = subparsers.add_parser(
        "export",
        help="Export observations as CSV or JSON.",
    )
    _add_config_arg(export_parser)
    export_parser.add_argument(
        "--format",
        dest="export_format",
        choices=["csv", "json"],
        default="csv",
        help="Output format (default: csv).",
    )
    export_parser.add_argument(
        "--start",
        help="Only include observations at or after this UTC timestamp "
        "(e.g. 2026-01-01 or '2026-01-01 12:00:00').",
    )
    export_parser.add_argument(
        "--end",
        help="Only include observations before this UTC timestamp (exclusive).",
    )
    export_parser.add_argument(
        "--output",
        type=Path,
        help="Output file path (default: stdout).",
    )

    # install-launchd
    launchd_parser = subparsers.add_parser(
        "install-launchd",
        help="Generate a macOS launchd service plist file.",
    )
    _add_config_arg(launchd_parser)

    # install-systemd
    systemd_parser = subparsers.add_parser(
        "install-systemd",
        help="Generate a systemd user service unit file.",
    )
    _add_config_arg(systemd_parser)

    return parser.parse_args(raw_args)


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


def _cmd_install_launchd(args: argparse.Namespace) -> None:
    from .launchd import install_launchd

    config_path = _resolve_config(args)
    install_launchd(config_path)


def _cmd_install_systemd(args: argparse.Namespace) -> None:
    from .systemd import install_systemd

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
        case "install-launchd":
            _cmd_install_launchd(args)
        case "install-systemd":
            _cmd_install_systemd(args)


if __name__ == "__main__":
    main()
