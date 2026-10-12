import json
from unittest import TestCase
from unittest.mock import patch

from hypothesis import given
from hypothesis import strategies as st

from ambientweather2sqlite.ha_discovery import build_discovery_messages
from ambientweather2sqlite.models import MqttConfig
from ambientweather2sqlite.units_mapping import (
    LABEL_TO_UNIT,
    Units,
    match_label_unit,
    units_for_columns,
)

_CONFIG = MqttConfig(
    host="broker.local",
    topic="weather/observations",
    discovery=True,
)

_LABELS = {
    "outTemp": "Outdoor Temperature (degF)",
    "outHumi": "Outdoor Humidity (%)",
    "winddir": "WindDir (°)",
    "AbsPress": "Absolute Pressure (hpa)",
    "CO2": "Carbon Dioxide",
}

_COLUMN_TO_UNIT = {
    "outTemp": "fahrenheit",
    "outHumi": "percent",
    "winddir": "angular_degree",
    "AbsPress": "1e2 * pascal",
}


def _messages_by_column() -> dict[str, tuple[str, dict]]:
    messages = build_discovery_messages(_LABELS, _COLUMN_TO_UNIT, _CONFIG)
    by_column = {}
    for topic, payload in messages:
        column = topic.split("/")[-2]
        by_column[column] = (topic, json.loads(payload))
    return by_column


class TestBuildDiscoveryMessages(TestCase):
    def test_solar_discovery_pairs_supported_units_without_changing_metadata(
        self,
    ) -> None:
        for unit, device_class, ha_unit in (
            ("w/m2", "irradiance", "W/m²"),
            ("watt / meter ** 2", "irradiance", "W/m²"),
            ("lux", "illuminance", "lx"),
            ("fc", None, "fc"),
            ("lumen / foot ** 2", None, "fc"),
        ):
            for label in ("Solar Radiation", f"Solar Radiation ({unit})"):
                with self.subTest(unit=unit, label=label):
                    labels = {"solarrad": label}
                    units = {"solarrad": unit}
                    original_labels = labels.copy()
                    original_units = units.copy()
                    messages = build_discovery_messages(labels, units, _CONFIG)
                    self.assertEqual(len(messages), 1)
                    topic, body = messages[0]
                    payload = json.loads(body)
                    self.assertEqual(labels, original_labels)
                    self.assertEqual(units, original_units)
                    self.assertEqual(
                        topic,
                        "homeassistant/sensor/aw2sqlite_aw2sqlite/solarrad/config",
                    )
                    self.assertEqual(payload["name"], "Solar Radiation")
                    self.assertEqual(
                        payload["unique_id"], "aw2sqlite_aw2sqlite_solarrad"
                    )
                    self.assertEqual(payload["state_topic"], _CONFIG.topic)
                    self.assertEqual(
                        payload["value_template"], "{{ value_json.solarrad }}"
                    )
                    self.assertEqual(payload["state_class"], "measurement")
                    self.assertEqual(payload["unit_of_measurement"], ha_unit)
                    if device_class is None:
                        self.assertNotIn("device_class", payload)
                        self.assertNotEqual(payload["unit_of_measurement"], "lx")
                    else:
                        self.assertEqual(payload["device_class"], device_class)

    def test_unrecognized_solar_units_produce_generic_sensors(self) -> None:
        labels = {"solarrad": "Solar Radiation"}
        for unit in (
            None,
            "unknown",
            "in",
            "inch",
            "degF",
            "fahrenheit",
            "%",
            "percent",
        ):
            with self.subTest(unit=unit):
                units = {} if unit is None else {"solarrad": unit}
                original_units = units.copy()
                messages = build_discovery_messages(labels, units, _CONFIG)
                self.assertEqual(len(messages), 1)
                payload = json.loads(messages[0][1])
                self.assertNotIn("device_class", payload)
                self.assertNotIn("unit_of_measurement", payload)
                self.assertEqual(payload["name"], "Solar Radiation")
                self.assertEqual(payload["unique_id"], "aw2sqlite_aw2sqlite_solarrad")
                self.assertEqual(payload["value_template"], "{{ value_json.solarrad }}")
                self.assertEqual(payload["state_class"], "measurement")
                self.assertEqual(units, original_units)

    @given(
        unit=st.one_of(
            st.none(),
            st.sampled_from(
                ("w/m2", "watt / meter ** 2", "lux", "fc", "lumen / foot ** 2")
            ),
            st.text(),
        ),
    )
    def test_solar_discovery_always_uses_valid_pairs(self, unit: str | None) -> None:
        labels = {"solarrad": "Solar Radiation"}
        units = {} if unit is None else {"solarrad": unit}
        original_units = units.copy()
        messages = build_discovery_messages(labels, units, _CONFIG)
        self.assertEqual(len(messages), 1)
        payload = json.loads(messages[0][1])
        self.assertIn(
            (payload.get("device_class"), payload.get("unit_of_measurement")),
            {
                ("irradiance", "W/m²"),
                ("illuminance", "lx"),
                (None, "fc"),
                (None, None),
            },
        )
        if payload.get("device_class") is None:
            self.assertNotIn("device_class", payload)
        if payload.get("unit_of_measurement") is None:
            self.assertNotIn("unit_of_measurement", payload)
        self.assertEqual(units, original_units)

    def test_generic_foot_candle_sensor_preserves_native_units(self) -> None:
        for unit in ("fc", "lumen / foot ** 2"):
            with self.subTest(unit=unit):
                messages = build_discovery_messages(
                    {"light": "Light Level"}, {"light": unit}, _CONFIG
                )
                payload = json.loads(messages[0][1])
                self.assertEqual(payload["unit_of_measurement"], "fc")
                self.assertNotIn("device_class", payload)
                self.assertEqual(payload["value_template"], "{{ value_json.light }}")

    def test_rain_rate_discovery_uses_intensity_and_retains_sensor_identity(
        self,
    ) -> None:
        for raw in ("in", "mm"):
            with self.subTest(unit=raw):
                labels, units = units_for_columns(
                    {
                        "rainofhourly": "Hourly Rain Rate",
                        "rate2": "Rain Rate",
                        "hourrain": "Hourly Rain",
                    },
                    {Units.RAINFALL: raw},
                )
                messages = build_discovery_messages(labels, units, _CONFIG)
                by_column = {
                    topic.split("/")[-2]: (topic, json.loads(body))
                    for topic, body in messages
                }
                for column in ("rainofhourly", "rate2"):
                    topic, payload = by_column[column]
                    self.assertEqual(
                        topic,
                        f"homeassistant/sensor/aw2sqlite_aw2sqlite/{column}/config",
                    )
                    self.assertEqual(
                        payload["unique_id"], f"aw2sqlite_aw2sqlite_{column}"
                    )
                    self.assertEqual(
                        payload["value_template"], f"{{{{ value_json.{column} }}}}"
                    )
                    self.assertEqual(payload["device_class"], "precipitation_intensity")
                    self.assertEqual(payload["unit_of_measurement"], f"{raw}/h")
                    self.assertEqual(payload["state_class"], "measurement")
                self.assertEqual(
                    by_column["hourrain"][1]["device_class"], "precipitation"
                )
                self.assertEqual(by_column["hourrain"][1]["unit_of_measurement"], raw)

    def test_rain_discovery_normalizes_known_units_without_changing_metadata(
        self,
    ) -> None:
        for unit, rate_unit, total_unit in (
            ("in", "in/h", "in"),
            ("inch", "in/h", "in"),
            ("mm", "mm/h", "mm"),
            ("millimeter", "mm/h", "mm"),
            ("in/hr", "in/h", None),
            ("inch / hour", "in/h", None),
            ("mm/hr", "mm/h", None),
            ("millimeter / hour", "mm/h", None),
        ):
            with self.subTest(unit=unit):
                labels = {
                    "rainofhourly": "Intensity",
                    "rate2": "Rainfall Rate",
                    "eventrain": "Calibrated Rainfall",
                    "hourrain": "Hourly Rain",
                }
                units = dict.fromkeys(labels, unit)
                original_units = units.copy()
                messages = build_discovery_messages(labels, units, _CONFIG)
                self.assertEqual(units, original_units)
                for topic, body in messages:
                    column = topic.split("/")[-2]
                    payload = json.loads(body)
                    self.assertEqual(
                        topic,
                        f"homeassistant/sensor/aw2sqlite_aw2sqlite/{column}/config",
                    )
                    self.assertEqual(
                        payload["unique_id"], f"aw2sqlite_aw2sqlite_{column}"
                    )
                    self.assertEqual(
                        payload["value_template"], f"{{{{ value_json.{column} }}}}"
                    )
                    self.assertEqual(payload["state_topic"], _CONFIG.topic)
                    self.assertEqual(payload["state_class"], "measurement")
                    if column in {"rainofhourly", "rate2"}:
                        self.assertEqual(
                            payload["device_class"], "precipitation_intensity"
                        )
                        self.assertEqual(payload["unit_of_measurement"], rate_unit)
                    elif total_unit is not None:
                        self.assertEqual(payload["device_class"], "precipitation")
                        self.assertEqual(payload["unit_of_measurement"], total_unit)
                    else:
                        self.assertNotIn("device_class", payload)
                        self.assertNotIn("unit_of_measurement", payload)

    def test_unrecognized_rain_units_produce_generic_sensors(self) -> None:
        labels = {
            "rainofhourly": "Hourly Rain Rate",
            "rate2": "Rain-Rate",
            "eventrain": "Calibrated Rainfall",
        }
        for unit in (None, "unknown", "cm", "fahrenheit", "percent"):
            with self.subTest(unit=unit):
                units = {} if unit is None else dict.fromkeys(labels, unit)
                messages = build_discovery_messages(labels, units, _CONFIG)
                self.assertEqual(len(messages), len(labels))
                for topic, body in messages:
                    column = topic.split("/")[-2]
                    payload = json.loads(body)
                    self.assertNotIn("device_class", payload)
                    self.assertNotIn("unit_of_measurement", payload)
                    self.assertEqual(payload["name"], labels[column])
                    self.assertEqual(
                        payload["unique_id"], f"aw2sqlite_aw2sqlite_{column}"
                    )
                    self.assertEqual(
                        payload["value_template"], f"{{{{ value_json.{column} }}}}"
                    )
                    self.assertEqual(payload["state_class"], "measurement")

    @given(
        unit=st.one_of(
            st.none(),
            st.sampled_from(
                ("inch", "millimeter", "inch / hour", "millimeter / hour", "fahrenheit")
            ),
            st.text(),
        ),
    )
    def test_rain_discovery_always_pairs_class_with_valid_unit(
        self, unit: str | None
    ) -> None:
        labels = {"rainofhourly": "Intensity", "eventrain": "Calibrated Rainfall"}
        units = {} if unit is None else dict.fromkeys(labels, unit)
        original_units = units.copy()
        for topic, body in build_discovery_messages(labels, units, _CONFIG):
            column = topic.split("/")[-2]
            payload = json.loads(body)
            if "device_class" not in payload:
                self.assertNotIn("unit_of_measurement", payload)
            elif column == "rainofhourly":
                self.assertEqual(payload["device_class"], "precipitation_intensity")
                self.assertIn(payload["unit_of_measurement"], {"in/h", "mm/h"})
            else:
                self.assertEqual(payload["device_class"], "precipitation")
                self.assertIn(payload["unit_of_measurement"], {"in", "mm"})
        self.assertEqual(units, original_units)

    def test_one_message_per_labeled_column(self):
        messages = build_discovery_messages(_LABELS, _COLUMN_TO_UNIT, _CONFIG)
        self.assertEqual(len(messages), len(_LABELS))

    def test_temperature_sensor_payload(self):
        topic, payload = _messages_by_column()["outTemp"]

        self.assertEqual(
            topic,
            "homeassistant/sensor/aw2sqlite_aw2sqlite/outTemp/config",
        )
        self.assertEqual(payload["name"], "Outdoor Temperature")
        self.assertEqual(payload["state_topic"], "weather/observations")
        self.assertEqual(payload["value_template"], "{{ value_json.outTemp }}")
        self.assertEqual(payload["unique_id"], "aw2sqlite_aw2sqlite_outTemp")
        self.assertEqual(payload["unit_of_measurement"], "°F")
        self.assertEqual(payload["device_class"], "temperature")
        self.assertEqual(payload["state_class"], "measurement")

    def test_pressure_unit_maps_from_pint_string(self):
        _topic, payload = _messages_by_column()["AbsPress"]

        self.assertEqual(payload["unit_of_measurement"], "hPa")
        self.assertEqual(payload["device_class"], "pressure")

    def test_wind_direction_omits_device_class(self):
        _topic, payload = _messages_by_column()["winddir"]

        self.assertNotIn("device_class", payload)
        self.assertEqual(payload["unit_of_measurement"], "°")

    def test_unitless_column_omits_unit_and_device_class(self):
        _topic, payload = _messages_by_column()["CO2"]

        self.assertNotIn("unit_of_measurement", payload)
        self.assertNotIn("device_class", payload)
        self.assertEqual(payload["name"], "Carbon Dioxide")

    def test_shared_device_block(self):
        by_column = _messages_by_column()

        devices = [payload["device"] for _topic, payload in by_column.values()]
        self.assertEqual(len({json.dumps(d) for d in devices}), 1)
        self.assertEqual(devices[0]["identifiers"], ["aw2sqlite_aw2sqlite"])

    def test_custom_discovery_prefix_and_client_id(self):
        config = MqttConfig(
            host="broker.local",
            topic="weather/observations",
            client_id="porch",
            discovery=True,
            discovery_prefix="ha",
        )

        messages = build_discovery_messages(
            {"outTemp": "Outdoor Temperature (degF)"},
            {"outTemp": "fahrenheit"},
            config,
        )

        topic, _payload = messages[0]
        self.assertEqual(topic, "ha/sensor/aw2sqlite_porch/outTemp/config")


class TestMatchLabelUnit(TestCase):
    def test_mixed_case_keys_preserve_match_precedence(self) -> None:
        with patch.dict(
            LABEL_TO_UNIT,
            {
                "WindDir": Units.WIND_DIRECTION,
                "WIND": Units.WIND,
                "Solar": Units.SOLAR_RADIATION,
            },
            clear=True,
        ):
            self.assertEqual(match_label_unit("WINDDIR (°)"), Units.WIND_DIRECTION)
            self.assertEqual(match_label_unit("Wind Speed"), Units.WIND)
            self.assertEqual(match_label_unit("SOLAR RADIATION"), Units.SOLAR_RADIATION)

    def test_winddir_matches_before_wind(self):
        self.assertEqual(match_label_unit("WindDir (°)"), Units.WIND_DIRECTION)
        self.assertEqual(match_label_unit("Wind Speed"), Units.WIND)
        self.assertEqual(match_label_unit("Gust Speed"), Units.WIND)

    def test_unmatched_label_returns_none(self):
        self.assertIsNone(match_label_unit("Carbon Dioxide"))
