import argparse
import json
import sys
from pathlib import Path

from .configuration import create_config_file, get_config_path, load_config
from .daemon import fetch_once, start_daemon
from .database import create_database_if_not_exists, query_db_metrics

_SUBCOMMANDS = {"serve", "config", "once", "status", "install-launchd"}


def _add_config_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config",
        dest="config_path",
        type=Path,
        help="Path to a TOML config file.",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    raw_args = argv if argv is not None else sys.argv[1:]

    # Default to serve when no subcommand is given
    if not raw_args or raw_args[0] not in _SUBCOMMANDS:
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

    # install-launchd
    launchd_parser = subparsers.add_parser(
        "install-launchd",
        help="Generate a macOS launchd service plist file.",
    )
    _add_config_arg(launchd_parser)

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
    )


def _cmd_config(args: argparse.Namespace) -> None:
    config_path = args.config_path
    created_path = create_config_file(config_path)
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
    print(json.dumps(metrics, indent=2))


def _cmd_install_launchd(args: argparse.Namespace) -> None:
    from .launchd import install_launchd

    config_path = _resolve_config(args)
    install_launchd(config_path)


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
        case "install-launchd":
            _cmd_install_launchd(args)


if __name__ == "__main__":
    main()
