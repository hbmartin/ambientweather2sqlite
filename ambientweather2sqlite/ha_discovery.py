"""Build Home Assistant MQTT discovery messages for station sensors.

https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from .units_mapping import PINT_UNIT_TO_AW_UNIT, Units, is_rain_rate, match_label_unit

if TYPE_CHECKING:
    from .models import ColumnUnitMap, LabelMap, MqttConfig

# Station unit strings to Home Assistant unit_of_measurement values.
AW_UNIT_TO_HA_UNIT: dict[str, str] = {
    "m/s": "m/s",
    "km/h": "km/h",
    "ft/s": "ft/s",
    "mph": "mph",
    "knot": "kn",
    "mm": "mm",
    "in": "in",
    "in/hr": "in/h",
    "mm/hr": "mm/h",
    "hpa": "hPa",
    "inhg": "inHg",
    "mmhg": "mmHg",
    "degC": "°C",
    "degF": "°F",
    "lux": "lx",
    "w/m2": "W/m²",
    "fc": "lx",
    "ug/m3": "µg/m³",
    "%": "%",
    "°": "°",
}

# Unit categories to Home Assistant device_class values; None where HA has
# no matching device class (wind direction).
UNIT_TO_DEVICE_CLASS: dict[Units, str | None] = {
    Units.TEMPERATURE: "temperature",
    Units.HUMIDITY: "humidity",
    Units.PRESSURE: "pressure",
    Units.WIND: "wind_speed",
    Units.RAINFALL: "precipitation",
    Units.SOLAR_RADIATION: "irradiance",
    Units.PM2_5: "pm25",
    Units.WIND_DIRECTION: None,
}


def _display_name(label: str) -> str:
    """Strip the trailing " (unit)" suffix that create_metadata appends."""
    if label.endswith(")") and " (" in label:
        return label.rsplit(" (", 1)[0]
    return label


def _ha_unit(column: str, column_to_unit: ColumnUnitMap) -> str | None:
    pint_unit = column_to_unit.get(column)
    if pint_unit is None:
        return None
    aw_unit = PINT_UNIT_TO_AW_UNIT.get(pint_unit, pint_unit)
    return AW_UNIT_TO_HA_UNIT.get(aw_unit)


def build_discovery_messages(
    labels: LabelMap,
    column_to_unit: ColumnUnitMap,
    config: MqttConfig,
) -> list[tuple[str, str]]:
    """Build one (topic, JSON payload) discovery message per sensor.

    Sensor values are read from the existing observation topic via a
    value_template, so no extra state publishing is needed.
    """
    node_id = f"aw2sqlite_{config.client_id}"
    device = {
        "identifiers": [node_id],
        "name": "AmbientWeather Station",
        "manufacturer": "AmbientWeather",
    }
    messages: list[tuple[str, str]] = []
    for column, label in labels.items():
        payload: dict[str, object] = {
            "name": _display_name(label),
            "state_topic": config.topic,
            "value_template": f"{{{{ value_json.{column} }}}}",
            "unique_id": f"{node_id}_{column}",
            "state_class": "measurement",
            "device": device,
        }
        if (ha_unit := _ha_unit(column, column_to_unit)) is not None:
            payload["unit_of_measurement"] = ha_unit
        unit_category = match_label_unit(label)
        if is_rain_rate(column, label):
            payload["device_class"] = "precipitation_intensity"
        elif (
            unit_category is not None
            and (device_class := UNIT_TO_DEVICE_CLASS.get(unit_category)) is not None
        ):
            payload["device_class"] = device_class
        topic = f"{config.discovery_prefix}/sensor/{node_id}/{column}/config"
        messages.append((topic, json.dumps(payload)))
    return messages
