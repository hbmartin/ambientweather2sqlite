import json
from unittest import TestCase

from ambientweather2sqlite.ha_discovery import build_discovery_messages
from ambientweather2sqlite.models import MqttConfig
from ambientweather2sqlite.units_mapping import (
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
    def test_rain_rate_discovery_uses_intensity_and_retains_sensor_identity(self):
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
    def test_winddir_matches_before_wind(self):
        self.assertEqual(match_label_unit("WindDir (°)"), Units.WIND_DIRECTION)
        self.assertEqual(match_label_unit("Wind Speed"), Units.WIND)
        self.assertEqual(match_label_unit("Gust Speed"), Units.WIND)

    def test_unmatched_label_returns_none(self):
        self.assertIsNone(match_label_unit("Carbon Dioxide"))
