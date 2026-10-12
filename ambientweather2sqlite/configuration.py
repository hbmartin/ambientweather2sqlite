import tomllib
from pathlib import Path
from typing import overload

from .models import (
    AlertConfig,
    AppConfig,
    MqttConfig,
    RecoveryConfig,
    RediscoveryConfig,
)

_CURRENT_PATH = Path.cwd()
_DEFAULT_CONFIG_NAME = "aw2sqlite.toml"
_DEFAULT_DATABASE_NAME = "aw2sqlite.db"
_VALID_LOG_FORMATS = frozenset({"text", "json"})
_MAX_PORT = 65535


def _config_type_error(key: str, expected_type: str) -> TypeError:
    message = f"{key} must be {expected_type}"
    return TypeError(message)


def _require_str(config_data: dict[str, object], key: str) -> str:
    value = config_data.get(key)
    if not isinstance(value, str):
        raise _config_type_error(key, "a string")
    return value


@overload
def _optional_int(
    config_data: dict[str, object],
    key: str,
    *,
    default: int,
) -> int: ...


@overload
def _optional_int(
    config_data: dict[str, object],
    key: str,
    *,
    default: None = None,
) -> int | None: ...


def _optional_int(
    config_data: dict[str, object],
    key: str,
    *,
    default: int | None = None,
) -> int | None:
    value = config_data.get(key, default)
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


def _optional_log_format(config_data: dict[str, object]) -> str:
    value = _optional_str(config_data, "log_format", "text")
    if value not in _VALID_LOG_FORMATS:
        valid_formats = ", ".join(sorted(_VALID_LOG_FORMATS))
        msg = f"log_format must be one of: {valid_formats}"
        raise ValueError(msg)
    return value


def _optional_str_or_none(config_data: dict[str, object], key: str) -> str | None:
    value = config_data.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise _config_type_error(key, "a string")
    return value


def _optional_bool(
    config_data: dict[str, object],
    key: str,
    *,
    default: bool,
) -> bool:
    value = config_data.get(key, default)
    if not isinstance(value, bool):
        raise _config_type_error(key, "a boolean")
    return value


def _optional_float(
    config_data: dict[str, object],
    key: str,
    *,
    default: float,
) -> float:
    value = config_data.get(key, default)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise _config_type_error(key, "a number")
    return float(value)


def _validate_url(value: str, key: str) -> str:
    if not value.startswith(("http://", "https://")):
        msg = f"{key} must start with http:// or https://"
        raise ValueError(msg)
    return value


def _validate_port(value: int | None, key: str) -> int | None:
    if value is not None and not (1 <= value <= _MAX_PORT):
        msg = f"{key} must be between 1 and {_MAX_PORT}"
        raise ValueError(msg)
    return value


def _require_table(value: object, key: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise _config_type_error(key, "a table")
    return {str(table_key): table_value for table_key, table_value in value.items()}


def _optional_mqtt(config_data: dict[str, object]) -> MqttConfig | None:
    mqtt_data = config_data.get("mqtt")
    if mqtt_data is None:
        return None
    mqtt_table = _require_table(mqtt_data, "mqtt")
    mqtt_port = _optional_int(mqtt_table, "port", default=1883)
    _validate_port(mqtt_port, "mqtt.port")
    return MqttConfig(
        host=_require_str(mqtt_table, "host"),
        topic=_require_str(mqtt_table, "topic"),
        port=mqtt_port,
        username=_optional_str_or_none(mqtt_table, "username"),
        password=_optional_str_or_none(mqtt_table, "password"),
        client_id=_optional_str(mqtt_table, "client_id", "aw2sqlite"),
        tls=_optional_bool(mqtt_table, "tls", default=False),
        discovery=_optional_bool(mqtt_table, "discovery", default=False),
        discovery_prefix=_optional_str(
            mqtt_table,
            "discovery_prefix",
            "homeassistant",
        ),
    )


def _optional_alerts(config_data: dict[str, object]) -> AlertConfig | None:
    alerts_data = config_data.get("alerts")
    if alerts_data is None:
        return None
    alerts_table = _require_table(alerts_data, "alerts")
    failure_threshold = _optional_int(alerts_table, "failure_threshold", default=3)
    if failure_threshold < 1:
        msg = "alerts.failure_threshold must be at least 1"
        raise ValueError(msg)
    timeout_seconds = _optional_float(alerts_table, "timeout_seconds", default=5.0)
    if timeout_seconds <= 0:
        msg = "alerts.timeout_seconds must be greater than 0"
        raise ValueError(msg)
    return AlertConfig(
        webhook_url=_validate_url(
            _require_str(alerts_table, "webhook_url"),
            "alerts.webhook_url",
        ),
        failure_threshold=failure_threshold,
        timeout_seconds=timeout_seconds,
    )


def _optional_rediscovery(config_data: dict[str, object]) -> RediscoveryConfig:
    rediscovery_data = config_data.get("rediscovery")
    if rediscovery_data is None:
        return RediscoveryConfig()
    rediscovery_table = _require_table(rediscovery_data, "rediscovery")
    failure_threshold = _optional_int(rediscovery_table, "failure_threshold", default=2)
    if failure_threshold < 1:
        msg = "rediscovery.failure_threshold must be at least 1"
        raise ValueError(msg)
    return RediscoveryConfig(
        enabled=_optional_bool(rediscovery_table, "enabled", default=True),
        failure_threshold=failure_threshold,
    )


def _recovery_positive_int(table: dict[str, object], key: str, default: int) -> int:
    value = _optional_int(table, key, default=default)
    if value < 1:
        msg = f"recovery.{key} must be at least 1"
        raise ValueError(msg)
    return value


def _optional_recovery(config_data: dict[str, object]) -> RecoveryConfig:
    value = config_data.get("recovery")
    if value is None:
        return RecoveryConfig()
    table = _require_table(value, "recovery")
    return RecoveryConfig(
        enabled=_optional_bool(table, "enabled", default=True),
        stale_seconds=_recovery_positive_int(table, "stale_seconds", 300),
        check_interval_seconds=_recovery_positive_int(
            table,
            "check_interval_seconds",
            60,
        ),
        recovery_observations=_recovery_positive_int(
            table,
            "recovery_observations",
            3,
        ),
    )


def load_config(config_path: Path) -> AppConfig:
    config_data = tomllib.loads(config_path.read_text(encoding="utf-8"))
    return AppConfig(
        live_data_url=_validate_url(
            _require_str(config_data, "live_data_url"),
            "live_data_url",
        ),
        database_path=_require_str(config_data, "database_path"),
        port=_validate_port(_optional_int(config_data, "port"), "port"),
        log_format=_optional_log_format(config_data),
        host=_optional_str(config_data, "host", "localhost"),
        auth_token=_optional_str_or_none(config_data, "auth_token"),
        mqtt=_optional_mqtt(config_data),
        alerts=_optional_alerts(config_data),
        rediscovery=_optional_rediscovery(config_data),
        recovery=_optional_recovery(config_data),
    )


def get_config_path() -> Path | None:
    cwd_config = _CURRENT_PATH / _DEFAULT_CONFIG_NAME
    if cwd_config.exists():
        return cwd_config
    home_config = Path.home() / ".aw2sqlite.toml"
    if home_config.exists():
        return home_config
    return None


def _prompt_for_discovered_station(stations: list[str]) -> str:
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


def _prompt_manual_url() -> str:
    ambient_url = ""
    while not ambient_url.startswith("http"):
        ambient_url = input(
            "Enter AmbientWeather Live Data URL: (e.g. http://192.168.0.226/livedata.htm)\n",
        ).strip()
    return ambient_url


def _prompt_for_url() -> str:
    """Prompt for a live data URL, with an option to auto-scan the network."""
    while True:
        scan_choice = input(
            "Would you like to auto-scan your network for a weather station? [Y/n]: ",
        ).strip()

        if scan_choice.lower() == "n":
            break

        from .scanner import scan_for_stations

        try:
            stations = scan_for_stations()
        except (OSError, ValueError) as exc:
            print(f"Auto-scan failed: {exc}")
            break

        if stations:
            return _prompt_for_discovered_station(stations)
        retry = input("Scan again? [y/N]: ").strip()
        if retry.lower() != "y":
            break

    return _prompt_manual_url()


def create_config_file(
    config_path: str | Path | None,
    *,
    overwrite_existing: bool = False,
) -> Path:
    output_path = (
        _CURRENT_PATH / _DEFAULT_CONFIG_NAME
        if config_path is None
        else Path(config_path)
    )
    if not overwrite_existing and output_path.exists():
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

    if config_path is None:
        output_path_input = input(
            f"Enter output TOML filename (leave blank for default: {output_path}):\n",
        ).strip()
        if output_path_input:
            output_path = Path(output_path_input)

    config = f'live_data_url = "{ambient_url}"\ndatabase_path = "{database_path}"\n'
    if port:
        config += f"port = {port}\n"
    if not overwrite_existing and output_path.exists():
        return output_path
    output_path.write_text(config, encoding="utf-8")

    return output_path
