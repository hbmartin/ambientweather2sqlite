from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import ColumnUnitMap, LabelMap


class Units(StrEnum):
    WIND = "Wind"
    RAINFALL = "Rainfall"
    PRESSURE = "Pressure"
    TEMPERATURE = "Temperature"
    SOLAR_RADIATION = "Solar Radiation"
    PM2_5 = "PM2.5"
    HUMIDITY = "Humidity"
    WIND_DIRECTION = "Wind Direction"


LABEL_TO_UNIT: dict[str, Units] = {
    "gust": Units.WIND,
    "rain": Units.RAINFALL,
    "press": Units.PRESSURE,
    "temp": Units.TEMPERATURE,
    "solar": Units.SOLAR_RADIATION,
    "pm": Units.PM2_5,
    "humi": Units.HUMIDITY,
    "winddir": Units.WIND_DIRECTION,
    "wind": Units.WIND,  # this line must be below winddir
}

# https://github.com/hgrecco/pint/blob/master/pint/default_en.txt
AW_UNIT_TO_PINT_UNIT: dict[str, str] = {
    "m/s": "mps",
    "km/h": "kph",
    "ft/s": "fps",
    "mph": "mph",
    "knot": "knot",
    "mm": "millimeter",
    "in": "inch",
    "hpa": "1e2 * pascal",
    "inhg": "inch_Hg",
    "mmhg": "millimeter_Hg",
    "degC": "celsius",
    "degF": "fahrenheit",
    "lux": "lux",
    "w/m2": "watt / meter ** 2",
    "fc": "lumen / foot ** 2",
    "ug/m3": "microgram / meter ** 3",
    "%": "percent",
    "°": "angular_degree",
}


# Pint unit names back to the station's raw unit strings (values are unique).
PINT_UNIT_TO_AW_UNIT: dict[str, str] = {
    pint_unit: aw_unit for aw_unit, pint_unit in AW_UNIT_TO_PINT_UNIT.items()
}


def match_label_unit(label: str) -> Units | None:
    """Match a sensor label to its unit category by substring.

    LABEL_TO_UNIT is ordered so that "winddir" matches before "wind".
    """
    for substr, unit in LABEL_TO_UNIT.items():
        if substr.lower() in label.lower():
            return unit
    return None


def units_for_columns(
    labels: LabelMap,
    units: dict[Units, str],
) -> tuple[LabelMap, ColumnUnitMap]:
    column_to_unit: ColumnUnitMap = {}
    labels_with_units: LabelMap = {}
    for column, label in labels.items():
        labels_with_units[column] = label
        unit = match_label_unit(label)
        if unit is None:
            continue
        raw_unit = units.get(unit)
        if raw_unit is None:
            continue
        column_to_unit[column] = AW_UNIT_TO_PINT_UNIT.get(raw_unit, raw_unit)
        labels_with_units[column] = f"{label} ({raw_unit})"

    return labels_with_units, column_to_unit
