import tempfile
from datetime import datetime, tzinfo
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from ambientweather2sqlite.database import (
    create_database_if_not_exists,
    insert_observation,
    query_range_aggregated_data,
)
from ambientweather2sqlite.exceptions import (
    InvalidDateError,
    InvalidDateRangeError,
    InvalidFormatError,
    InvalidTimezoneError,
)


class TestQueryRangeAggregatedData(TestCase):
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        Path(self.db_path).unlink(missing_ok=True)
        create_database_if_not_exists(self.db_path)

        # Stored timestamps are UTC.
        insert_observation(
            self.db_path,
            {"ts": "2026-01-01 10:00:00", "outTemp": 70.0},
        )
        insert_observation(
            self.db_path,
            {"ts": "2026-01-01 11:00:00", "outTemp": 74.0},
        )
        insert_observation(
            self.db_path,
            {"ts": "2026-01-02 10:00:00", "outTemp": 80.0},
        )

    def tearDown(self):
        Path(self.db_path).unlink(missing_ok=True)

    def _fixed_now(self) -> type[datetime]:
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz: tzinfo | None = None) -> datetime:
                return datetime(2026, 1, 3, tzinfo=tz)

        return FixedDateTime

    def test_aggregates_over_window(self):
        result = query_range_aggregated_data(
            self.db_path,
            ["avg_outTemp", "max_outTemp"],
            start="2026-01-01 00:00:00",
            end="2026-01-02 00:00:00",
            tz="UTC",
        )

        self.assertEqual(result["count"], 2)
        self.assertEqual(result["avg_outTemp"], 72.0)
        self.assertEqual(result["max_outTemp"], 74.0)
        self.assertEqual(result["start"], "2026-01-01 00:00:00")
        self.assertEqual(result["end"], "2026-01-02 00:00:00")

    def test_date_only_bounds(self):
        result = query_range_aggregated_data(
            self.db_path,
            ["min_outTemp"],
            start="2026-01-02",
            tz="UTC",
        )

        self.assertEqual(result["count"], 1)
        self.assertEqual(result["min_outTemp"], 80.0)

    def test_start_is_interpreted_in_query_timezone(self):
        # 2026-01-01 05:00 in UTC-6 is 11:00 UTC, so only the second
        # observation of that day falls inside the window.
        result = query_range_aggregated_data(
            self.db_path,
            ["avg_outTemp"],
            start="2026-01-01 05:00:00",
            end="2026-01-01 18:00:00",
            tz="-06:00",
        )

        self.assertEqual(result["count"], 1)
        self.assertEqual(result["avg_outTemp"], 74.0)

    def test_end_defaults_to_now(self):
        with patch(
            "ambientweather2sqlite.database.aggregation.datetime",
            self._fixed_now(),
        ):
            result = query_range_aggregated_data(
                self.db_path,
                ["avg_outTemp"],
                start="2026-01-01",
                tz="UTC",
            )

        self.assertEqual(result["count"], 3)

    def test_empty_window_returns_zero_count_and_nulls(self):
        result = query_range_aggregated_data(
            self.db_path,
            ["avg_outTemp"],
            start="2030-01-01",
            end="2030-01-02",
            tz="UTC",
        )

        self.assertEqual(result["count"], 0)
        self.assertIsNone(result["avg_outTemp"])

    def test_invalid_start_raises(self):
        with self.assertRaises(InvalidDateError):
            query_range_aggregated_data(
                self.db_path,
                ["avg_outTemp"],
                start="not-a-date",
                tz="UTC",
            )

    def test_end_before_start_raises(self):
        with self.assertRaises(InvalidDateRangeError):
            query_range_aggregated_data(
                self.db_path,
                ["avg_outTemp"],
                start="2026-01-02",
                end="2026-01-01",
                tz="UTC",
            )

    def test_invalid_aggregation_field_raises(self):
        with self.assertRaises(InvalidFormatError):
            query_range_aggregated_data(
                self.db_path,
                ["explode_outTemp"],
                start="2026-01-01",
                tz="UTC",
            )

    def test_extreme_numeric_timezone_raises_invalid_timezone(self):
        with self.assertRaises(InvalidTimezoneError):
            query_range_aggregated_data(
                self.db_path,
                ["avg_outTemp"],
                start="2026-01-01",
                tz="2400",
            )
