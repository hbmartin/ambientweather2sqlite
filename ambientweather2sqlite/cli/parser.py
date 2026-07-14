import argparse
import sys
from pathlib import Path

from ambientweather2sqlite.database import DEFAULT_GAP_THRESHOLD_SECONDS

_SUBCOMMANDS = {
    "serve",
    "config",
    "once",
    "status",
    "export",
    "backup",
    "migrate",
    "metadata",
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
        default=DEFAULT_GAP_THRESHOLD_SECONDS,
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

    # backup
    backup_parser = subparsers.add_parser(
        "backup",
        help="Write a compacted backup copy of the database (VACUUM INTO).",
    )
    _add_config_arg(backup_parser)
    backup_parser.add_argument(
        "destination",
        type=Path,
        help="Path for the backup file (must not already exist).",
    )

    # migrate
    migrate_parser = subparsers.add_parser(
        "migrate",
        help="Inspect or apply a backed-up database migration.",
    )
    _add_config_arg(migrate_parser)
    migrate_mode = migrate_parser.add_mutually_exclusive_group()
    migrate_mode.add_argument(
        "--check",
        action="store_true",
        help="Print the migration plan without changing the database.",
    )
    migrate_mode.add_argument(
        "--backup",
        type=Path,
        help="Pre-migration backup path (default: timestamped beside the database).",
    )

    # metadata
    metadata_parser = subparsers.add_parser(
        "metadata",
        help="Refresh or regenerate the Datasette metadata sidecar.",
    )
    _add_config_arg(metadata_parser)
    metadata_parser.add_argument(
        "--offline",
        action="store_true",
        help=(
            "Regenerate from metadata embedded in the database without network access."
        ),
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
