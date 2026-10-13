import json
import os
import tomllib
from pathlib import Path
from typing import overload

from .http_validation import (
    UnsupportedHttpSchemeError,
    parse_http_url,
    validate_port,
)
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
_SURROGATE_START = 0xD800
_SURROGATE_END = 0xDFFF
_MAX_SERVER_PORT = 65535
_SERVER_PORT_DIGITS = len(str(_MAX_SERVER_PORT))


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
    try:
        parse_http_url(value)
    except (UnsupportedHttpSchemeError,) as error:  # noqa: B013
        msg = f"{key} must start with http:// or https://"
        raise ValueError(msg) from error
    except (ValueError,) as error:  # noqa: B013
        msg = f"{key} is invalid: {error}"
        raise ValueError(msg) from error
    return value


def _configured_database_path(config_data: dict[str, object]) -> str:
    value = _require_str(config_data, "database_path")
    try:
        _validate_database_path(value)
    except (ValueError,) as error:  # noqa: B013
        msg = f"database_path is invalid: {error}"
        raise ValueError(msg) from error
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
    validate_port(mqtt_port, "mqtt.port")
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
    failure_threshold = _positive_int(alerts_table, "alerts", "failure_threshold", 3)
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
    failure_threshold = _positive_int(
        rediscovery_table,
        "rediscovery",
        "failure_threshold",
        2,
    )
    return RediscoveryConfig(
        enabled=_optional_bool(rediscovery_table, "enabled", default=True),
        failure_threshold=failure_threshold,
    )


def _positive_int(
    table: dict[str, object],
    section: str,
    key: str,
    default: int,
) -> int:
    value = _optional_int(table, key, default=default)
    if value < 1:
        msg = f"{section}.{key} must be at least 1"
        raise ValueError(msg)
    return value


def _optional_recovery(config_data: dict[str, object]) -> RecoveryConfig:
    value = config_data.get("recovery")
    if value is None:
        return RecoveryConfig()
    table = _require_table(value, "recovery")
    return RecoveryConfig(
        enabled=_optional_bool(table, "enabled", default=True),
        stale_seconds=_positive_int(table, "recovery", "stale_seconds", 300),
        check_interval_seconds=_positive_int(
            table,
            "recovery",
            "check_interval_seconds",
            60,
        ),
        recovery_observations=_positive_int(
            table,
            "recovery",
            "recovery_observations",
            3,
        ),
    )


def load_config(config_path: Path) -> AppConfig:
    config_data = tomllib.loads(config_path.read_text(encoding="utf-8"))
    return AppConfig(
        live_data_url=_validate_url(
            _require_str(config_data, "live_data_url"), "live_data_url"
        ),
        database_path=_configured_database_path(config_data),
        port=validate_port(_optional_int(config_data, "port"), "port"),
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
        if not (choice.isascii() and choice.isdecimal()):
            continue
        significant = choice.lstrip("0") or "0"
        if len(significant) > len(str(len(stations))):
            continue
        selected = int(significant)
        if 1 <= selected <= len(stations):
            return stations[selected - 1]


def _prompt_manual_url() -> str:
    while True:
        ambient_url = input(
            "Enter AmbientWeather Live Data URL: (e.g. http://192.168.0.226/livedata.htm)\n",
        ).strip()
        if not ambient_url:
            continue
        try:
            _validate_unicode_scalars(ambient_url)
            parse_http_url(ambient_url)
        except (UnsupportedHttpSchemeError,):  # noqa: B013
            print("Live data URL must start with http:// or https://")
        except (ValueError,) as error:  # noqa: B013
            print(f"Invalid live data URL: {error}")
        else:
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


def _validate_unicode_scalars(value: str) -> None:
    if any(_SURROGATE_START <= ord(character) <= _SURROGATE_END for character in value):
        msg = "surrogate characters cannot be saved in TOML; enter valid Unicode text"
        raise ValueError(msg)


def _display_setup_text(value: str) -> str:
    """Escape surrogate characters for display without changing stored values."""
    return value.encode("utf-8", errors="backslashreplace").decode("utf-8")


def _validate_database_path(value: str) -> None:
    _validate_unicode_scalars(value)
    if "\x00" in value:
        msg = "NUL characters cannot be used in database paths"
        raise ValueError(msg)


def _prompt_database_path() -> str:
    default = str(_CURRENT_PATH / _DEFAULT_DATABASE_NAME)
    try:
        _validate_database_path(default)
    except (ValueError,) as error:  # noqa: B013
        print(
            f"Default database path is unavailable: {_display_setup_text(str(error))}"
        )
        default = ""
        message = "Enter Database Path (an explicit path is required):\n"
    else:
        message = f"Enter Database Path (leave blank for default: {_display_setup_text(default)}):\n"
    while True:
        database_path = input(message).strip() or default
        if not database_path:
            continue
        try:
            _validate_database_path(database_path)
        except (ValueError,) as error:  # noqa: B013
            print(f"Invalid database path: {error}")
        else:
            return database_path


def _prompt_server_port() -> int | None:
    while True:
        raw_port = input(
            "Enter port number to serve JSON data (leave blank to disable):\n",
        ).strip()
        if not raw_port:
            return None
        if not (raw_port.isascii() and raw_port.isdecimal()):
            print(
                "Invalid server port: enter decimal digits, or leave blank to disable"
            )
            continue
        significant = raw_port.lstrip("0") or "0"
        port = (
            int(significant)
            if len(significant) <= _SERVER_PORT_DIGITS
            else _MAX_SERVER_PORT + 1
        )
        try:
            validate_port(port, "Server port")
        except (ValueError,) as error:  # noqa: B013
            print(error)
        else:
            return port


def _toml_string(value: str) -> str:
    """Encode a Unicode string as a TOML basic string."""
    _validate_unicode_scalars(value)
    # JSON escapes match TOML basic strings for scalars, except raw DEL.
    return json.dumps(value, ensure_ascii=False).replace("\x7f", r"\u007f")


def _validate_output_path(path: Path) -> None:
    if "\x00" in str(path):
        msg = "output filenames must not contain NUL characters"
        raise ValueError(msg)
    try:
        os.fsencode(path)
    except (UnicodeEncodeError,) as error:  # noqa: B013
        msg = "output filename cannot be encoded for this filesystem"
        raise ValueError(msg) from error


def _prompt_output_path(default: Path) -> Path:
    try:
        _validate_output_path(default)
    except (ValueError,) as error:  # noqa: B013
        print(
            f"Default output filename is unavailable: {_display_setup_text(str(error))}"
        )
        message = "Enter output TOML filename (an explicit filename is required):\n"
        default_available = False
    else:
        message = f"Enter output TOML filename (leave blank for default: {_display_setup_text(str(default))}):\n"
        default_available = True
    while True:
        entered = input(message).strip()
        if not entered and not default_available:
            continue
        path = Path(entered) if entered else default
        try:
            _validate_output_path(path)
        except (ValueError,) as error:  # noqa: B013
            print(f"Invalid output filename: {_display_setup_text(str(error))}")
        else:
            return path


def _write_config(path: Path, config: str, *, overwrite_existing: bool) -> Path:
    _validate_output_path(path)
    if not overwrite_existing and path.exists():
        return path
    path.write_text(config, encoding="utf-8")
    return path


def _write_interactive_config(
    default: Path, config: str, *, overwrite_existing: bool
) -> Path:
    while True:
        path = _prompt_output_path(default)
        try:
            return _write_config(path, config, overwrite_existing=overwrite_existing)
        except (OSError, ValueError) as error:
            print(f"Could not save configuration: {_display_setup_text(str(error))}")


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
    try:
        _validate_output_path(output_path)
        if not overwrite_existing and output_path.exists():
            return output_path
    except (OSError, ValueError):
        if config_path is not None:
            raise

    print("Configuration Setup")
    print("-" * 20)

    ambient_url = _prompt_for_url()

    database_path = _prompt_database_path()
    port = _prompt_server_port()

    config = (
        f"live_data_url = {_toml_string(ambient_url)}\n"
        f"database_path = {_toml_string(database_path)}\n"
    )
    if port is not None:
        config += f"port = {port}\n"
    if config_path is None:
        return _write_interactive_config(
            output_path, config, overwrite_existing=overwrite_existing
        )
    return _write_config(output_path, config, overwrite_existing=overwrite_existing)
