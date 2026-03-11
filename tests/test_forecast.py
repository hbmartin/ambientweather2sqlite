import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from ambientweather2sqlite.database import ensure_forecast_tables
from ambientweather2sqlite.exceptions import ForecastCooldownError
from ambientweather2sqlite.forecast import (
    can_fetch,
    fetch_and_store_forecast,
    get_last_fetch_time,
    store_forecast,
)

_SAMPLE_FORECAST = {
    "location": {
        "name": "Test City",
        "lat": 40.7,
        "lon": -74.0,
        "country": "US",
        "timezone": "America/New_York",
    },
    "hourly": {
        "provider_a": [
            {
                "time": "2025-01-01T00:00",
                "timestamp": 1735689600000,
                "temperature": 32.0,
                "feelsLike": 28.0,
                "humidity": 80.0,
                "windSpeed": 5.0,
                "precipitation": 0.0,
                "precipitationProbability": 10.0,
                "cloudCover": 50.0,
                "condition": "Cloudy",
                "icon": "cloudy",
                "pressure": 1013.0,
            },
            {
                "time": "2025-01-01T01:00",
                "timestamp": 1735693200000,
                "temperature": 31.0,
                "feelsLike": 27.0,
                "humidity": 82.0,
                "windSpeed": 6.0,
                "precipitation": 0.1,
                "precipitationProbability": 20.0,
                "cloudCover": 60.0,
                "condition": "Light Rain",
                "icon": "rain",
                "pressure": 1012.0,
            },
        ],
        "provider_b": [
            {
                "time": "2025-01-01T00:00",
                "temperature": 33.0,
                "humidity": 78.0,
                "windSpeed": 4.5,
                "condition": "Overcast",
                "icon": "overcast",
            },
        ],
    },
    "daily": {
        "provider_a": [
            {
                "date": "2025-01-01",
                "high": 35.0,
                "low": 28.0,
                "humidity": 75.0,
                "windSpeed": 8.0,
                "precipitation": 0.5,
                "precipitationProbability": 40.0,
                "condition": "Rain",
                "icon": "rain",
                "sunrise": "07:15",
                "sunset": "16:45",
            },
        ],
    },
}


class TestForecastTables(unittest.TestCase):
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()

    def tearDown(self):
        Path(self.db_path).unlink(missing_ok=True)

    def test_ensure_forecast_tables_creates_tables(self):
        ensure_forecast_tables(self.db_path)
        with sqlite3.connect(self.db_path) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'",
                ).fetchall()
            }
        self.assertIn("forecast_batches", tables)
        self.assertIn("forecast_hourly", tables)
        self.assertIn("forecast_daily", tables)

    def test_ensure_forecast_tables_idempotent(self):
        ensure_forecast_tables(self.db_path)
        ensure_forecast_tables(self.db_path)  # should not raise


class TestCooldown(unittest.TestCase):
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        ensure_forecast_tables(self.db_path)

    def tearDown(self):
        Path(self.db_path).unlink(missing_ok=True)

    def test_empty_table_returns_none(self):
        self.assertIsNone(get_last_fetch_time(self.db_path))

    def test_can_fetch_empty_table(self):
        self.assertTrue(can_fetch(self.db_path))

    def test_can_fetch_recent_batch(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO forecast_batches (fetched_at) VALUES (?)",
                (datetime.now(UTC).isoformat(),),
            )
        self.assertFalse(can_fetch(self.db_path))

    def test_can_fetch_old_batch(self):
        old_time = datetime.now(UTC) - timedelta(hours=24)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO forecast_batches (fetched_at) VALUES (?)",
                (old_time.isoformat(),),
            )
        self.assertTrue(can_fetch(self.db_path))


class TestStoreForcast(unittest.TestCase):
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        ensure_forecast_tables(self.db_path)

    def tearDown(self):
        Path(self.db_path).unlink(missing_ok=True)

    def test_store_creates_batch(self):
        batch_id = store_forecast(self.db_path, _SAMPLE_FORECAST)
        self.assertEqual(batch_id, 1)
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT location_name, lat, lon FROM forecast_batches WHERE id = ?",
                (batch_id,),
            ).fetchone()
        self.assertEqual(row[0], "Test City")
        self.assertAlmostEqual(row[1], 40.7)
        self.assertAlmostEqual(row[2], -74.0)

    def test_store_hourly_row_counts(self):
        batch_id = store_forecast(self.db_path, _SAMPLE_FORECAST)
        with sqlite3.connect(self.db_path) as conn:
            count_a = conn.execute(
                "SELECT COUNT(*) FROM forecast_hourly WHERE batch_id = ? AND provider = ?",
                (batch_id, "provider_a"),
            ).fetchone()[0]
            count_b = conn.execute(
                "SELECT COUNT(*) FROM forecast_hourly WHERE batch_id = ? AND provider = ?",
                (batch_id, "provider_b"),
            ).fetchone()[0]
        self.assertEqual(count_a, 2)
        self.assertEqual(count_b, 1)

    def test_store_daily_row_counts(self):
        batch_id = store_forecast(self.db_path, _SAMPLE_FORECAST)
        with sqlite3.connect(self.db_path) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM forecast_daily WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_store_hourly_field_mapping(self):
        batch_id = store_forecast(self.db_path, _SAMPLE_FORECAST)
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM forecast_hourly WHERE batch_id = ? AND provider = ? ORDER BY forecast_time LIMIT 1",
                (batch_id, "provider_a"),
            ).fetchone()
        self.assertEqual(row["forecast_time"], "2025-01-01T00:00")
        self.assertEqual(row["timestamp_ms"], 1735689600000)
        self.assertEqual(row["feels_like"], 28.0)
        self.assertEqual(row["wind_speed"], 5.0)
        self.assertEqual(row["precipitation_probability"], 10.0)
        self.assertEqual(row["cloud_cover"], 50.0)
        self.assertEqual(row["pressure"], 1013.0)

    def test_store_missing_optional_fields(self):
        """provider_b has no pressure, timestamp, etc. — should store NULLs."""
        batch_id = store_forecast(self.db_path, _SAMPLE_FORECAST)
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM forecast_hourly WHERE batch_id = ? AND provider = ?",
                (batch_id, "provider_b"),
            ).fetchone()
        self.assertIsNone(row["pressure"])
        self.assertIsNone(row["timestamp_ms"])

    def test_successive_fetches_separate_batch_ids(self):
        batch_id_1 = store_forecast(self.db_path, _SAMPLE_FORECAST)
        batch_id_2 = store_forecast(self.db_path, _SAMPLE_FORECAST)
        self.assertNotEqual(batch_id_1, batch_id_2)
        with sqlite3.connect(self.db_path) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM forecast_batches",
            ).fetchone()[0]
        self.assertEqual(count, 2)


class TestFetchAndStore(unittest.TestCase):
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        ensure_forecast_tables(self.db_path)

    def tearDown(self):
        Path(self.db_path).unlink(missing_ok=True)

    @patch("ambientweather2sqlite.forecast.fetch_forecast")
    def test_fetch_and_store_success(self, mock_fetch):
        mock_fetch.return_value = _SAMPLE_FORECAST
        batch_id = fetch_and_store_forecast("http://example.com/api", self.db_path)
        self.assertEqual(batch_id, 1)
        mock_fetch.assert_called_once_with("http://example.com/api")

    @patch("ambientweather2sqlite.forecast.fetch_forecast")
    def test_cooldown_enforcement(self, mock_fetch):
        mock_fetch.return_value = _SAMPLE_FORECAST
        fetch_and_store_forecast("http://example.com/api", self.db_path)
        with self.assertRaises(ForecastCooldownError):
            fetch_and_store_forecast("http://example.com/api", self.db_path)

    @patch("ambientweather2sqlite.forecast.fetch_forecast")
    def test_force_bypass(self, mock_fetch):
        mock_fetch.return_value = _SAMPLE_FORECAST
        fetch_and_store_forecast("http://example.com/api", self.db_path)
        batch_id = fetch_and_store_forecast(
            "http://example.com/api",
            self.db_path,
            force=True,
        )
        self.assertEqual(batch_id, 2)


if __name__ == "__main__":
    unittest.main()
