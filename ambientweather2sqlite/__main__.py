import sys
from pathlib import Path

from .configuration import create_config_file, get_config_path, load_config
from .daemon import start_daemon
from .database import create_database_if_not_exists


def get_int_argument(args: list[str]) -> int | None:
    for arg in args:
        try:
            return int(arg)
        except ValueError:
            pass
    return None


def get_str_argument(args: list[str]) -> str | None:
    """Find the first non-numerical argument in the list and return it as string."""
    for arg in args:
        try:
            int(arg)
        except ValueError:
            return arg
    return None


def main() -> None:
    default_config_path = get_config_path()
    port: int | None = None
    if len(sys.argv) > 1:
        args = sys.argv[1:]
        port = get_int_argument(args)
        if config_override := get_str_argument(args):
            default_config_path = Path(config_override)
    config_path = create_config_file(default_config_path)
    config = load_config(config_path)
    create_database_if_not_exists(config.database_path)
    start_daemon(
        live_data_url=config.live_data_url,
        database_path=config.database_path,
        port=port or config.port,
    )


if __name__ == "__main__":
    main()
