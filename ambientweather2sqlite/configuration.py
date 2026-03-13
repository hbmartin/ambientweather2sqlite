import tomllib
from pathlib import Path

from .models import AppConfig

_CURRENT_PATH = Path.cwd()
_DEFAULT_CONFIG_NAME = "aw2sqlite.toml"
_DEFAULT_DATABASE_NAME = "aw2sqlite.db"


def _config_type_error(key: str, expected_type: str) -> TypeError:
    message = f"{key} must be {expected_type}"
    return TypeError(message)


def _require_str(config_data: dict[str, object], key: str) -> str:
    value = config_data.get(key)
    if not isinstance(value, str):
        raise _config_type_error(key, "a string")
    return value


def _optional_int(config_data: dict[str, object], key: str) -> int | None:
    value = config_data.get(key)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise _config_type_error(key, "an integer")
    return value


def _optional_str(
    config_data: dict[str, object],
    key: str,
    default: str,
) -> str:
    value = config_data.get(key, default)
    if not isinstance(value, str):
        raise _config_type_error(key, "a string")
    return value


def load_config(config_path: Path) -> AppConfig:
    config_data = tomllib.loads(config_path.read_text(encoding="utf-8"))
    return AppConfig(
        live_data_url=_require_str(config_data, "live_data_url"),
        database_path=_require_str(config_data, "database_path"),
        port=_optional_int(config_data, "port"),
        log_format=_optional_str(config_data, "log_format", "text"),
    )


def get_config_path() -> Path | None:
    cwd_config = _CURRENT_PATH / _DEFAULT_CONFIG_NAME
    if cwd_config.exists():
        return cwd_config
    home_config = Path.home() / ".aw2sqlite.toml"
    if home_config.exists():
        return home_config
    return None


def _prompt_for_url() -> str:
    """Prompt for a live data URL, with an option to auto-scan the network."""
    scan_choice = input(
        "Would you like to auto-scan your network for a weather station? [Y/n]: ",
    ).strip()

    if scan_choice.lower() != "n":
        from .scanner import scan_for_stations

        stations = scan_for_stations()
        if stations:
            if len(stations) == 1:
                print(f"Using discovered station: {stations[0]}")
                return stations[0]
            print("\nMultiple stations found:")
            for i, url in enumerate(stations, 1):
                print(f"  {i}. {url}")
            while True:
                choice = input("Select a station number: ").strip()
                if choice.isdigit() and 1 <= int(choice) <= len(stations):
                    return stations[int(choice) - 1]
        else:
            retry = input("Scan again? [y/N]: ").strip()
            if retry.lower() == "y":
                return _prompt_for_url()

    ambient_url = ""
    while not ambient_url.startswith("http"):
        ambient_url = input(
            "Enter AmbientWeather Live Data URL: (e.g. http://192.168.0.226/livedata.htm)\n",
        ).strip()
    return ambient_url


def create_config_file(config_path: str | Path | None) -> Path:
    if (
        config_path is not None
        and (output_path := Path(config_path))
        and output_path.exists()
    ):
        return output_path

    print("Configuration Setup")
    print("-" * 20)

    ambient_url = _prompt_for_url()

    database_path = input(
        f"Enter Database Path (leave blank for default: {_CURRENT_PATH / _DEFAULT_DATABASE_NAME}):\n",
    ).strip()
    if not database_path:
        database_path = str(_CURRENT_PATH / _DEFAULT_DATABASE_NAME)
    port = input(
        "Enter port number to server JSON data (leave blank to disable):\n",
    ).strip()

    output_file = (
        _CURRENT_PATH / _DEFAULT_CONFIG_NAME
        if config_path is None
        else Path(config_path)
    )
    if config_path is None:
        output_path_input = input(
            f"Enter output TOML filename (leave blank for default: {output_file}):\n",
        ).strip()
        if output_path_input:
            output_file = Path(output_path_input)

    config = f'live_data_url = "{ambient_url}"\ndatabase_path = "{database_path}"\n'
    if port:
        config += f"port = {port}\n"
    output_path = Path(output_file)
    output_path.write_text(config, encoding="utf-8")

    return output_path
