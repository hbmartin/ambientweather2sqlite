import sys
import tomllib
from typing import TYPE_CHECKING

from .configuration import create_config_file, get_config_path
from .daemon import start_daemon
from .database import create_database_if_not_exists, ensure_forecast_tables
from .exceptions import ForecastCooldownError
from .forecast import fetch_and_store_forecast

if TYPE_CHECKING:
    from pathlib import Path


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


def _run_forecast(args: list[str], config: dict) -> None:
    force = "--force" in args
    forecast_api_url = config.get("forecast_api_url")
    if not forecast_api_url:
        print("Error: forecast_api_url not set in config")
        sys.exit(1)
    db_path = config["database_path"]
    ensure_forecast_tables(db_path)
    try:
        batch_id = fetch_and_store_forecast(forecast_api_url, db_path, force=force)
        print(f"Forecast stored (batch_id={batch_id})")
    except ForecastCooldownError as e:
        print(str(e))
        sys.exit(1)


def main() -> None:
    default_config_path: str | Path | None = get_config_path()
    port: int | None = None
    is_forecast = len(sys.argv) > 1 and sys.argv[1] == "forecast"
    if len(sys.argv) > 1:
        args = sys.argv[1:]
        if is_forecast:
            args = args[1:]  # skip "forecast"
        port = get_int_argument(args)
        default_config_path = (
            get_str_argument(
                [a for a in args if a != "--force"],
            )
            or default_config_path
        )
    config_path = create_config_file(default_config_path)
    config = tomllib.loads(config_path.read_text())
    if is_forecast:
        _run_forecast(sys.argv[2:], config)
        return
    create_database_if_not_exists(config["database_path"])
    start_daemon(
        live_data_url=config["live_data_url"],
        database_path=config["database_path"],
        port=port or config.get("port"),
    )


if __name__ == "__main__":
    main()
