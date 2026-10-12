from unittest import TestCase

from hypothesis import given
from hypothesis import strategies as st

from ambientweather2sqlite.units_mapping import (
    AW_UNIT_TO_PINT_UNIT,
    PINT_UNIT_TO_AW_UNIT,
    Units,
    is_rain_rate,
    units_for_columns,
)


class TestStationUnitCasing(TestCase):
    def test_supported_options_use_canonical_spellings_and_pint_units(self) -> None:
        original_map = AW_UNIT_TO_PINT_UNIT.copy()
        original_inverse = PINT_UNIT_TO_AW_UNIT.copy()
        for canonical, pint_unit in AW_UNIT_TO_PINT_UNIT.items():
            mixed_case = "".join(
                char.upper() if index % 2 else char.lower()
                for index, char in enumerate(canonical)
            )
            for raw in (canonical, canonical.lower(), canonical.upper(), mixed_case):
                with self.subTest(unit=raw):
                    labels = {"solarrad": "Solar Radiation"}
                    station_units = {Units.SOLAR_RADIATION: raw}
                    self.assertEqual(
                        units_for_columns(labels, station_units),
                        (
                            {"solarrad": f"Solar Radiation ({canonical})"},
                            {"solarrad": pint_unit},
                        ),
                    )
                    self.assertEqual(labels, {"solarrad": "Solar Radiation"})
                    self.assertEqual(station_units, {Units.SOLAR_RADIATION: raw})
        self.assertEqual(AW_UNIT_TO_PINT_UNIT, original_map)
        self.assertEqual(PINT_UNIT_TO_AW_UNIT, original_inverse)

    @given(
        canonical=st.sampled_from(tuple(AW_UNIT_TO_PINT_UNIT)),
        uppercase=st.integers(min_value=0),
    )
    def test_capitalization_preserves_metadata(
        self, canonical: str, uppercase: int
    ) -> None:
        raw = "".join(
            char.upper() if uppercase & (1 << index) else char.lower()
            for index, char in enumerate(canonical)
        )
        labels = {"sensor": "Solar Radiation"}
        self.assertEqual(
            units_for_columns(labels, {Units.SOLAR_RADIATION: raw}),
            units_for_columns(labels, {Units.SOLAR_RADIATION: canonical}),
        )

    def test_unknown_options_preserve_the_original_string(self) -> None:
        for raw in ("UnknownUnit", "W/m3", "DEGK", "WATT / METER ** 2"):
            with self.subTest(unit=raw):
                self.assertEqual(
                    units_for_columns(
                        {"solarrad": "Solar Radiation"},
                        {Units.SOLAR_RADIATION: raw},
                    ),
                    (
                        {"solarrad": f"Solar Radiation ({raw})"},
                        {"solarrad": raw},
                    ),
                )


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

    def test_capitalized_rain_options_normalize_before_rate_mapping(self) -> None:
        for raw, rate_raw, pint_rate, total_raw, pint_total in (
            ("IN", "in/hr", "inch / hour", "in", "inch"),
            ("Mm", "mm/hr", "millimeter / hour", "mm", "millimeter"),
            ("IN/HR", "in/hr", "inch / hour", "in/hr", "inch / hour"),
            ("mM/hR", "mm/hr", "millimeter / hour", "mm/hr", "millimeter / hour"),
        ):
            with self.subTest(unit=raw):
                labels, units = units_for_columns(
                    {"rainofhourly": "Intensity", "eventrain": "Calibrated Rainfall"},
                    {Units.RAINFALL: raw},
                )
                self.assertEqual(labels["rainofhourly"], f"Intensity ({rate_raw})")
                self.assertEqual(units["rainofhourly"], pint_rate)
                self.assertEqual(
                    labels["eventrain"], f"Calibrated Rainfall ({total_raw})"
                )
                self.assertEqual(units["eventrain"], pint_total)

    def test_missing_rain_unit_stays_unknown(self) -> None:
        sensors = {"rainofhourly": "Hourly Rain Rate"}
        self.assertEqual(units_for_columns(sensors, {}), (sensors, {}))

    def test_unknown_unit_is_not_assumed_to_be_inches(self) -> None:
        _labels, units = units_for_columns(
            {"rainofhourly": "Hourly Rain Rate"},
            {Units.RAINFALL: "unknown"},
        )
        self.assertEqual(units, {"rainofhourly": "unknown"})
