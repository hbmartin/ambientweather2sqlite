from unittest import TestCase

from ambientweather2sqlite.units_mapping import Units, units_for_columns


class TestRainUnits(TestCase):
    def test_rate_and_accumulation_have_distinct_dimensions(self) -> None:
        sensors = {
            "rainofhourly": "Hourly Rain Rate",
            "rainrate": "RAIN RATE",
            "eventrain": "Event Rain",
            "hourrain": "Hourly Rain",
            "rainofdaily": "Daily Rain",
        }
        for raw, length in (("in", "inch"), ("mm", "millimeter")):
            with self.subTest(unit=raw):
                labels, units = units_for_columns(sensors, {Units.RAINFALL: raw})
                for column in ("rainofhourly", "rainrate"):
                    self.assertEqual(units[column], f"{length} / hour")
                    self.assertEqual(labels[column], f"{sensors[column]} ({raw}/hr)")
                for column in ("eventrain", "hourrain", "rainofdaily"):
                    self.assertEqual(units[column], length)
                    self.assertEqual(labels[column], f"{sensors[column]} ({raw})")

    def test_known_rate_field_does_not_depend_on_label(self) -> None:
        labels, units = units_for_columns(
            {"rainofhourly": "Intensity"},
            {Units.RAINFALL: "in"},
        )
        self.assertEqual(labels, {"rainofhourly": "Intensity (in/hr)"})
        self.assertEqual(units, {"rainofhourly": "inch / hour"})

    def test_missing_rain_unit_stays_unknown(self) -> None:
        sensors = {"rainofhourly": "Hourly Rain Rate"}
        self.assertEqual(units_for_columns(sensors, {}), (sensors, {}))

    def test_unknown_unit_is_not_assumed_to_be_inches(self) -> None:
        _labels, units = units_for_columns(
            {"rainofhourly": "Hourly Rain Rate"},
            {Units.RAINFALL: "unknown"},
        )
        self.assertEqual(units, {"rainofhourly": "unknown"})
