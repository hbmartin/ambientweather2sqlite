import argparse
from pathlib import Path

from .configuration import create_config_file, get_config_path, load_config
from .daemon import start_daemon
from .database import create_database_if_not_exists


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the AmbientWeather to SQLite daemon.",
    )
    parser.add_argument(
        "--port",
        type=int,
        help="Port number for the HTTP JSON API server.",
    )
    parser.add_argument(
        "--config",
        dest="config_path",
        type=Path,
        help="Path to a TOML config file.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    default_config_path = args.config_path or get_config_path()
    config_path = create_config_file(default_config_path)
    config = load_config(config_path)
    create_database_if_not_exists(config.database_path)
    start_daemon(
        live_data_url=config.live_data_url,
        database_path=config.database_path,
        port=args.port if args.port is not None else config.port,
    )


if __name__ == "__main__":
    main()
