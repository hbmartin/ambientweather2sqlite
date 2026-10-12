from unittest import TestCase

from ambientweather2sqlite.units_mapping import Units, is_rain_rate, units_for_columns


class TestRainUnits(TestCase):
    def test_explicit_rate_labels(self) -> None:
        for label in (
            "Hourly Rain Rate",
            "RAIN RATE",
            "Rainfall Rate",
            "Rain-Rate",
            "Rain_Rate",
            "RainRate",
            "RainfallRate",
            "Hourly Rain Rate (in/hr)",
            "Rainfall Rate (mm/hr)",
        ):
            with self.subTest(label=label):
                self.assertTrue(is_rain_rate("other", label))

    def test_rate_substrings_do_not_classify_accumulations(self) -> None:
        for label in (
            "Calibrated Rainfall",
            "Rainfall calibrated",
            "Drain flow rate",
            "Hourly Rain",
            "Rainfall moderate",
            "Rain Rateometer",
            "Brain Rate",
        ):
            with self.subTest(label=label):
                self.assertFalse(is_rain_rate("other", label))
                self.assertTrue(is_rain_rate("rainofhourly", label))

    def test_calibrated_accumulation_keeps_length_units(self) -> None:
        for raw, length in (("in", "inch"), ("mm", "millimeter")):
            with self.subTest(unit=raw):
                labels, units = units_for_columns(
                    {"eventrain": "Calibrated Rainfall"},
                    {Units.RAINFALL: raw},
                )
                self.assertEqual(labels, {"eventrain": f"Calibrated Rainfall ({raw})"})
                self.assertEqual(units, {"eventrain": length})

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
