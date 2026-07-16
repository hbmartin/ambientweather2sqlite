"""Parser tests against captured-style livedata.htm / station.htm fixtures.

Each fixture mirrors the HTML shape of a real station firmware so parser
regressions against actual devices are caught. To add coverage for another
firmware, drop its page into tests/fixtures/ and add a test class here.
"""

from pathlib import Path
from unittest import TestCase

from ambientweather2sqlite.awparser import (
    extract_labels,
    extract_station_time,
    extract_units,
    extract_values,
)
from ambientweather2sqlite.units_mapping import Units

_FIXTURES = Path(__file__).parent / "fixtures"


def _read_fixture(name: str) -> str:
    return (_FIXTURES / name).read_text(encoding="utf-8")


class TestObserverIpFixture(TestCase):
    def setUp(self):
        self.html = _read_fixture("livedata_observerip.htm")

    def test_extracts_all_sensor_values(self):
        values = extract_values(self.html)

        self.assertEqual(values["inTemp"], 72.3)
        self.assertEqual(values["inHumi"], 41.0)
        self.assertEqual(values["AbsPress"], 28.79)
        self.assertEqual(values["RelPress"], 29.91)
        self.assertEqual(values["outTemp"], 64.9)
        self.assertEqual(values["outHumi"], 78.0)
        self.assertEqual(values["windir"], 271.0)
        self.assertEqual(values["avgwind"], 2.7)
        self.assertEqual(values["gustspeed"], 4.5)
        self.assertEqual(values["solarrad"], 105.98)
        self.assertEqual(values["uv"], 351.0)
        self.assertEqual(values["uvi"], 1.0)
        self.assertEqual(values["rainofhourly"], 0.0)
        self.assertEqual(values["eventrain"], 0.12)
        self.assertEqual(values["rainofdaily"], 0.24)

    def test_excludes_time_battery_and_id_fields(self):
        values = extract_values(self.html)

        self.assertNotIn("CurrTime", values)
        self.assertNotIn("inBattSta", values)
        self.assertNotIn("outBattSta1", values)
        self.assertNotIn("IDACC", values)
        self.assertNotIn("IDTran", values)

    def test_captures_station_reported_time(self):
        self.assertEqual(extract_station_time(self.html), "14:47 9/21/2021")

    def test_excludes_enabled_inputs(self):
        values = extract_values(self.html)

        self.assertNotIn("rainreset", values)

    def test_extracts_labels_from_row_text(self):
        labels = extract_labels(self.html)

        self.assertEqual(labels["outTemp"], "Outdoor Temperature")
        self.assertEqual(labels["gustspeed"], "Gust Speed")
        self.assertEqual(labels["rainofhourly"], "Hourly Rain Rate")


class TestWs2000Pm25Fixture(TestCase):
    def setUp(self):
        self.html = _read_fixture("livedata_ws2000_pm25.htm")

    def test_extracts_pm25_and_negative_temperature(self):
        values = extract_values(self.html)

        self.assertEqual(values["outTemp"], -3.4)
        self.assertEqual(values["pm25"], 12.0)
        self.assertEqual(values["pm25_24h"], 9.8)
        self.assertEqual(values["avgwind"], 0.0)

    def test_disconnected_sensor_value_is_none(self):
        values = extract_values(self.html)

        self.assertIn("soilmoisture1", values)
        self.assertIsNone(values["soilmoisture1"])

    def test_excludes_pm25_battery_field(self):
        values = extract_values(self.html)

        self.assertNotIn("pm25BattSta", values)

    def test_extracts_labels(self):
        labels = extract_labels(self.html)

        self.assertEqual(labels["pm25"], "PM2.5")
        self.assertEqual(labels["soilmoisture1"], "Soil Moisture")


class TestStationSettingsFixture(TestCase):
    def setUp(self):
        self.html = _read_fixture("station_settings.htm")

    def test_extracts_selected_units(self):
        units = extract_units(self.html)

        self.assertEqual(units[Units.WIND], "mph")
        self.assertEqual(units[Units.RAINFALL], "in")
        self.assertEqual(units[Units.PRESSURE], "inhg")
        self.assertEqual(units[Units.TEMPERATURE], "degF")
        self.assertEqual(units[Units.SOLAR_RADIATION], "w/m2")

    def test_defaults_applied_for_missing_units(self):
        units = extract_units(self.html)

        self.assertEqual(units[Units.HUMIDITY], "%")
        self.assertEqual(units[Units.WIND_DIRECTION], "°")
