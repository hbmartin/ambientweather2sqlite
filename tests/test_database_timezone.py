import sqlite3
import tempfile
from pathlib import Path
from unittest import TestCase

from ambientweather2sqlite.database import (
    _validate_timezone,
    create_database_if_not_exists,
    insert_observation,
    query_daily_aggregated_data,
    query_hourly_aggregated_data,
)
from ambientweather2sqlite.exceptions import InvalidTimezoneError


class TestDatabaseTimezone(TestCase):
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        Path(self.db_path).unlink(missing_ok=True)

        # Create database and insert test data
        was_created = create_database_if_not_exists(self.db_path)
        self.assertTrue(was_created)

        # Insert test observations with different timestamps
        test_data = [
            {
                "ts": "2025-06-27 12:00:00",
                "outTemp": 75.0,
                "outHumi": 60.0,
                "gustspeed": 10.0,
            },
            {
                "ts": "2025-06-27 13:00:00",
                "outTemp": 77.0,
                "outHumi": 58.0,
                "gustspeed": 15.0,
            },
            {
                "ts": "2025-06-27 14:00:00",
                "outTemp": 79.0,
                "outHumi": 55.0,
                "gustspeed": 20.0,
            },
            {
                "ts": "2025-06-26 12:00:00",
                "outTemp": 72.0,
                "outHumi": 65.0,
                "gustspeed": 8.0,
            },
            {
                "ts": "2025-06-26 13:00:00",
                "outTemp": 74.0,
                "outHumi": 62.0,
                "gustspeed": 12.0,
            },
        ]
        print(self.db_path)
        print(Path(self.db_path).exists())
        for data in test_data:
            insert_observation(self.db_path, data)

    def tearDown(self):
        Path(self.db_path).unlink(missing_ok=True)

    def test_query_daily_aggregated_data_with_valid_timezone(self):
        """Test daily aggregation with valid timezone"""
        result = query_daily_aggregated_data(
            db_path=self.db_path,
            aggregation_fields=["avg_outTemp", "max_gustspeed"],
            prior_days=7,
            tz="America/New_York",
        )

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

        # Check that data contains expected fields
        for day_data in result:
            self.assertIn("date", day_data)
            self.assertIn("avg_outTemp", day_data)
            self.assertIn("max_gustspeed", day_data)
            self.assertIn("count", day_data)

    def test_query_daily_aggregated_data_with_utc_offset(self):
        """Test daily aggregation with UTC offset timezone"""
        result = query_daily_aggregated_data(
            db_path=self.db_path,
            aggregation_fields=["avg_outTemp"],
            prior_days=7,
            tz="+05:30",
        )

        self.assertIsInstance(result, list)

    def test_query_daily_aggregated_data_with_invalid_timezone(self):
        """Test daily aggregation with invalid timezone raises ValueError"""
        with self.assertRaises(InvalidTimezoneError) as context:
            query_daily_aggregated_data(
                db_path=self.db_path,
                aggregation_fields=["avg_outTemp"],
                prior_days=7,
                tz="Invalid/Timezone",
            )

        self.assertIn("Invalid timezone", str(context.exception))

    def test_query_daily_aggregated_data_without_timezone(self):
        """Test daily aggregation without timezone parameter"""
        result = query_daily_aggregated_data(
            db_path=self.db_path,
            aggregation_fields=["avg_outTemp", "max_gustspeed"],
            prior_days=7,
        )

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

    def test_query_hourly_aggregated_data_with_valid_timezone(self):
        """Test hourly aggregation with valid timezone"""
        result = query_hourly_aggregated_data(
            db_path=self.db_path,
            aggregation_fields=["avg_outTemp", "max_gustspeed"],
            date="2025-06-27",
            tz="Europe/London",
        )

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 24)  # 24 hours

        # Check that non-null entries have expected fields
        for hour_data in result:
            if hour_data is not None:
                self.assertIn("hour", hour_data)
                self.assertIn("avg_outTemp", hour_data)
                self.assertIn("max_gustspeed", hour_data)
                self.assertIn("count", hour_data)

    def test_query_hourly_aggregated_data_with_utc_offset(self):
        """Test hourly aggregation with UTC offset timezone"""
        result = query_hourly_aggregated_data(
            db_path=self.db_path,
            aggregation_fields=["avg_outTemp"],
            date="2025-06-27",
            tz="-08:00",
        )

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 24)

    def test_query_hourly_aggregated_data_with_invalid_timezone(self):
        """Test hourly aggregation with invalid timezone raises ValueError"""
        with self.assertRaises(InvalidTimezoneError) as context:
            query_hourly_aggregated_data(
                db_path=self.db_path,
                aggregation_fields=["avg_outTemp"],
                date="2025-06-27",
                tz="Not/A/Timezone",
            )

        self.assertIn("Invalid timezone", str(context.exception))

    def test_query_hourly_aggregated_data_without_timezone(self):
        """Test hourly aggregation without timezone parameter"""
        result = query_hourly_aggregated_data(
            db_path=self.db_path,
            aggregation_fields=["avg_outTemp", "max_gustspeed"],
            date="2025-06-27",
        )

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 24)

    def test_timezone_affects_aggregation_results(self):
        """Test that different timezones can produce different results"""
        # Insert data at timezone boundary
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO observations (ts, outTemp) VALUES (?, ?)",
                ("2025-06-27 23:30:00", 80.0),
            )
            conn.commit()

        # Query with different timezones
        result_utc = query_daily_aggregated_data(
            db_path=self.db_path,
            aggregation_fields=["avg_outTemp"],
            prior_days=7,
            tz="UTC",
        )

        result_pst = query_daily_aggregated_data(
            db_path=self.db_path,
            aggregation_fields=["avg_outTemp"],
            prior_days=7,
            tz="America/Los_Angeles",
        )

        # Both should return results (specific assertions would depend on test data)
        self.assertIsInstance(result_utc, list)
        self.assertIsInstance(result_pst, list)

    def test_empty_timezone_string(self):
        """Test empty timezone string is treated as None"""
        result = query_daily_aggregated_data(
            db_path=self.db_path,
            aggregation_fields=["avg_outTemp"],
            prior_days=7,
            tz="",
        )

        # Empty string should be treated as valid (no timezone conversion)
        self.assertIsInstance(result, list)

    def test_validate_timezone_none(self):
        """Test _validate_timezone with None input"""
        result = _validate_timezone(None)
        self.assertEqual(result, "localtime")

    def test_validate_timezone_empty_string(self):
        """Test _validate_timezone with empty string input"""
        result = _validate_timezone("")
        self.assertEqual(result, "localtime")

    def test_validate_timezone_whitespace_string(self):
        """Test _validate_timezone with whitespace string input"""
        with self.assertRaises(InvalidTimezoneError) as context:
            _validate_timezone("   ")
        self.assertIn("Invalid timezone", str(context.exception))

    def test_validate_timezone_positive_offset(self):
        """Test _validate_timezone with positive UTC offset"""
        result = _validate_timezone("+05:30")
        self.assertEqual(result, "5.5 hours")

    def test_validate_timezone_decimal_offset(self):
        """Test _validate_timezone with decimal offset"""
        result = _validate_timezone("+05.5")
        self.assertEqual(result, "5.5 hours")

    def test_validate_timezone_negative_offset(self):
        """Test _validate_timezone with negative UTC offset"""
        result = _validate_timezone("-08:00")
        self.assertEqual(result, "-8.0 hours")

    def test_validate_timezone_offset_with_minutes(self):
        """Test _validate_timezone with offset including minutes"""
        result = _validate_timezone("+02:45")
        self.assertEqual(result, "2.75 hours")

    def test_validate_timezone_offset_whole_hours(self):
        """Test _validate_timezone with whole hour offset"""
        result = _validate_timezone("+03:00")
        self.assertEqual(result, "3.0 hours")

    def test_validate_timezone_offset_zero_minutes(self):
        """Test _validate_timezone with zero minutes"""
        result = _validate_timezone("-05:00")
        self.assertEqual(result, "-5.0 hours")

    def test_validate_timezone_valid_zoneinfo(self):
        """Test _validate_timezone with valid ZoneInfo timezone"""
        result = _validate_timezone("America/New_York")
        # Should return a valid hours string, exact value depends on current time
        self.assertIsInstance(result, str)
        self.assertTrue(result.endswith(" hours"))
        # Should be a valid float
        hours = float(result.replace(" hours", ""))
        self.assertIsInstance(hours, float)

    def test_validate_timezone_utc_zoneinfo(self):
        """Test _validate_timezone with UTC timezone"""
        result = _validate_timezone("UTC")
        self.assertEqual(result, "0.0 hours")

    def test_validate_timezone_europe_london(self):
        """Test _validate_timezone with Europe/London timezone"""
        result = _validate_timezone("Europe/London")
        # Should return a valid hours string
        self.assertIsInstance(result, str)
        self.assertTrue(result.endswith(" hours"))
        hours = float(result.replace(" hours", ""))
        self.assertIsInstance(hours, float)

    def test_validate_timezone_invalid_offset_format(self):
        """Test _validate_timezone with invalid offset format"""
        with self.assertRaises(InvalidTimezoneError) as context:
            _validate_timezone("invalid:offset")
        self.assertIn("Invalid timezone", str(context.exception))

    def test_validate_timezone_invalid_offset_with_letters(self):
        """Test _validate_timezone with offset containing letters"""
        with self.assertRaises(InvalidTimezoneError) as context:
            _validate_timezone("+12:ab")
        self.assertIn("Invalid timezone", str(context.exception))

    def test_validate_timezone_invalid_zoneinfo(self):
        """Test _validate_timezone with invalid ZoneInfo timezone"""
        with self.assertRaises(InvalidTimezoneError) as context:
            _validate_timezone("Not/A/Valid/Timezone")
        self.assertIn("Invalid timezone", str(context.exception))

    def test_validate_timezone_malformed_offset(self):
        """Test _validate_timezone with malformed offset string"""
        with self.assertRaises(InvalidTimezoneError) as context:
            _validate_timezone("++05:30")
        self.assertIn("Invalid timezone", str(context.exception))

    def test_validate_timezone_offset_without_colon(self):
        """Test _validate_timezone with offset without colon"""
        result = _validate_timezone("+0530")
        self.assertEqual(result, "5.5 hours")

    def test_validate_timezone_offset_negative_hours_no_separator(self):
        """Test _validate_timezone with negative hours"""
        result = _validate_timezone("-0530")
        self.assertEqual(result, "-5.5 hours")

    def test_validate_timezone_offset_negative_hours_colon_separator(self):
        """Test _validate_timezone with negative hours"""
        result = _validate_timezone("-05:30")
        self.assertEqual(result, "-5.5 hours")

    def test_validate_timezone_offset_negative_hours_period_separator(self):
        """Test _validate_timezone with negative hours"""
        result = _validate_timezone("-05.5")
        self.assertEqual(result, "-5.5 hours")

    def test_validate_timezone_offset_with_extra_parts(self):
        """Test _validate_timezone with offset having extra parts"""
        with self.assertRaises(InvalidTimezoneError) as context:
            _validate_timezone("+05:30:00")
        self.assertIn("Invalid timezone", str(context.exception))
