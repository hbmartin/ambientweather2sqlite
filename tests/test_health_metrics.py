"""Tests for /health and /metrics endpoints, plus DB metrics and validation."""

import json
import logging
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest import TestCase
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from ambientweather2sqlite.database import (
    create_database_if_not_exists,
    insert_observation,
    query_db_metrics,
    query_latest_timestamp,
    query_observation_gaps,
)
from ambientweather2sqlite.database.writes import _validate_observation
from ambientweather2sqlite.server import Server


class TestQueryDbMetrics(TestCase):
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        Path(self.db_path).unlink(missing_ok=True)
        create_database_if_not_exists(self.db_path)

    def tearDown(self):
        Path(self.db_path).unlink(missing_ok=True)

    def test_metrics_empty_database(self):
        metrics = query_db_metrics(self.db_path)

        self.assertEqual(metrics["row_count"], 0)
        self.assertIsNone(metrics["earliest_ts"])
        self.assertIsNone(metrics["latest_ts"])
        self.assertGreater(metrics["column_count"], 0)
        self.assertGreater(metrics["db_file_size_bytes"], 0)

    def test_metrics_with_data(self):
        today = datetime.now().date()
        insert_observation(
            self.db_path,
            {"ts": f"{today} 12:00:00", "outTemp": 72.0},
        )
        insert_observation(
            self.db_path,
            {"ts": f"{today} 13:00:00", "outTemp": 75.0},
        )

        metrics = query_db_metrics(self.db_path)

        self.assertEqual(metrics["row_count"], 2)
        self.assertIsNotNone(metrics["earliest_ts"])
        self.assertIsNotNone(metrics["latest_ts"])
        self.assertGreaterEqual(metrics["column_count"], 2)


class TestQueryLatestTimestamp(TestCase):
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        Path(self.db_path).unlink(missing_ok=True)
        create_database_if_not_exists(self.db_path)

    def tearDown(self):
        Path(self.db_path).unlink(missing_ok=True)

    def test_returns_none_for_empty_db(self):
        result = query_latest_timestamp(self.db_path)
        self.assertIsNone(result)

    def test_returns_latest_ts(self):
        insert_observation(
            self.db_path,
            {"ts": "2026-01-01 10:00:00", "outTemp": 70.0},
        )
        insert_observation(
            self.db_path,
            {"ts": "2026-01-01 11:00:00", "outTemp": 72.0},
        )

        result = query_latest_timestamp(self.db_path)
        self.assertIn("2026-01-01 11:00:00", result)


class TestDeduplication(TestCase):
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        Path(self.db_path).unlink(missing_ok=True)
        create_database_if_not_exists(self.db_path)

    def tearDown(self):
        Path(self.db_path).unlink(missing_ok=True)

    def test_duplicate_timestamp_is_ignored(self):
        data = {"ts": "2026-01-01 12:00:00", "outTemp": 72.0}
        insert_observation(self.db_path, data)
        insert_observation(self.db_path, data)

        metrics = query_db_metrics(self.db_path)
        self.assertEqual(metrics["row_count"], 1)

    def test_different_timestamps_both_inserted(self):
        insert_observation(
            self.db_path,
            {"ts": "2026-01-01 12:00:00", "outTemp": 72.0},
        )
        insert_observation(
            self.db_path,
            {"ts": "2026-01-01 12:01:00", "outTemp": 73.0},
        )

        metrics = query_db_metrics(self.db_path)
        self.assertEqual(metrics["row_count"], 2)


class TestQueryObservationGaps(TestCase):
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        Path(self.db_path).unlink(missing_ok=True)
        create_database_if_not_exists(self.db_path)
        self.now = datetime(2026, 1, 2, 12, 0, 0, tzinfo=UTC)

    def tearDown(self):
        Path(self.db_path).unlink(missing_ok=True)

    def _insert_at(self, ts: str) -> None:
        insert_observation(self.db_path, {"ts": ts, "outTemp": 70.0})

    def test_no_gaps_for_empty_db(self):
        self.assertEqual(query_observation_gaps(self.db_path, now=self.now), [])

    def test_no_gaps_for_continuous_observations(self):
        self._insert_at("2026-01-02 11:58:00")
        self._insert_at("2026-01-02 11:59:00")
        self._insert_at("2026-01-02 12:00:00")

        self.assertEqual(query_observation_gaps(self.db_path, now=self.now), [])

    def test_detects_gap_between_observations(self):
        self._insert_at("2026-01-02 10:00:00")
        self._insert_at("2026-01-02 11:58:00")
        self._insert_at("2026-01-02 12:00:00")

        gaps = query_observation_gaps(self.db_path, now=self.now)

        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0]["start"], "2026-01-02 10:00:00")
        self.assertEqual(gaps[0]["end"], "2026-01-02 11:58:00")
        self.assertEqual(gaps[0]["duration_seconds"], 7080.0)

    def test_detects_ongoing_trailing_gap(self):
        self._insert_at("2026-01-02 10:00:00")

        gaps = query_observation_gaps(self.db_path, now=self.now)

        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0]["start"], "2026-01-02 10:00:00")
        self.assertIsNone(gaps[0]["end"])
        self.assertEqual(gaps[0]["duration_seconds"], 7200.0)

    def test_detects_ongoing_gap_that_started_before_window(self):
        self._insert_at("2026-01-01 11:00:00")

        gaps = query_observation_gaps(self.db_path, hours=24, now=self.now)

        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0]["start"], "2026-01-01 11:00:00")
        self.assertIsNone(gaps[0]["end"])
        self.assertEqual(gaps[0]["duration_seconds"], 90000.0)

    def test_normalizes_naive_now_to_utc(self):
        self._insert_at("2026-01-02 10:00:00")

        gaps = query_observation_gaps(
            self.db_path,
            now=datetime.fromisoformat("2026-01-02T12:00:00"),
        )

        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0]["duration_seconds"], 7200.0)

    def test_respects_custom_threshold(self):
        self._insert_at("2026-01-02 11:58:00")
        self._insert_at("2026-01-02 12:00:00")

        gaps = query_observation_gaps(
            self.db_path,
            threshold_seconds=60.0,
            now=self.now,
        )

        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0]["duration_seconds"], 120.0)


class TestDataValidation(TestCase):
    def test_warns_on_implausible_temperature(self):
        with self.assertLogs("ambientweather2sqlite.database", level="WARNING") as cm:
            _validate_observation({"outTemp": 999.0})

        self.assertTrue(
            any("Implausible value for outTemp" in msg for msg in cm.output),
        )

    def test_warns_on_negative_humidity(self):
        with self.assertLogs("ambientweather2sqlite.database", level="WARNING") as cm:
            _validate_observation({"outHumi": -50.0})

        self.assertTrue(
            any("Implausible value for outHumi" in msg for msg in cm.output),
        )

    def test_no_warning_for_normal_values(self):
        logger = logging.getLogger("ambientweather2sqlite.database")
        with self.assertNoLogs(logger, level="WARNING"):
            _validate_observation({"outTemp": 72.0, "outHumi": 55.0})

    def test_no_warning_for_non_numeric_values(self):
        logger = logging.getLogger("ambientweather2sqlite.database")
        with self.assertNoLogs(logger, level="WARNING"):
            _validate_observation({"ts": "2026-01-01 12:00:00"})

    def test_warns_on_extreme_wind(self):
        with self.assertLogs("ambientweather2sqlite.database", level="WARNING") as cm:
            _validate_observation({"gustspeed": 600.0})

        self.assertTrue(
            any("Implausible value for gustspeed" in msg for msg in cm.output),
        )


def _wait_for_health_ok(test: TestCase, port: int, timeout: float = 2.0) -> None:
    """Poll /health until it reports "ok" or fail the test on timeout.

    An empty database reports "ok" (fresh-deploy grace), so this works
    before any observation exists.
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            url = f"http://127.0.0.1:{port}/health"
            with urlopen(url, timeout=1) as response:
                if json.load(response)["status"] == "ok":
                    return
        except (OSError, URLError, json.JSONDecodeError, KeyError):
            if time.monotonic() >= deadline:
                test.fail("Server not ready")
            time.sleep(0.01)


class TestServerHealthMetrics(TestCase):
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        Path(self.db_path).unlink(missing_ok=True)
        create_database_if_not_exists(self.db_path)

        # A fresh timestamp keeps /health "ok"; stale data returns 503.
        insert_observation(
            self.db_path,
            {
                "ts": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f"),
                "outTemp": 72.0,
            },
        )

        self.server = Server("http://127.0.0.1:9", self.db_path, 0, "127.0.0.1")
        self.server.start()
        self.port = self.server.httpd.server_address[1]
        _wait_for_health_ok(self, self.port)

    def tearDown(self):
        self.server.shutdown()
        Path(self.db_path).unlink(missing_ok=True)

    def _get_json(self, path: str) -> dict:
        with urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=1) as response:
            return json.load(response)

    def test_health_endpoint(self):
        data = self._get_json("/health")

        self.assertEqual(data["status"], "ok")
        self.assertIsNotNone(data["last_observation_ts"])
        self.assertEqual(data["row_count"], 1)
        self.assertIsInstance(data["age_seconds"], float)
        self.assertLess(data["age_seconds"], 180.0)

    def test_metrics_endpoint(self):
        data = self._get_json("/metrics")

        self.assertEqual(data["row_count"], 1)
        self.assertIn("db_file_size_bytes", data)
        self.assertIn("earliest_ts", data)
        self.assertIn("latest_ts", data)
        self.assertIn("column_count", data)


class TestServerHealthStaleness(TestCase):
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        Path(self.db_path).unlink(missing_ok=True)
        create_database_if_not_exists(self.db_path)

        self.server = Server("http://127.0.0.1:9", self.db_path, 0, "127.0.0.1")
        self.server.start()
        self.port = self.server.httpd.server_address[1]
        _wait_for_health_ok(self, self.port)

    def tearDown(self):
        self.server.shutdown()
        Path(self.db_path).unlink(missing_ok=True)

    def _get_json(self, path: str) -> dict:
        with urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=1) as response:
            return json.load(response)

    def test_empty_database_reports_ok(self):
        data = self._get_json("/health")

        self.assertEqual(data["status"], "ok")
        self.assertIsNone(data["last_observation_ts"])
        self.assertIsNone(data["age_seconds"])
        self.assertEqual(data["row_count"], 0)

    def test_stale_observation_returns_503(self):
        insert_observation(
            self.db_path,
            {"ts": "2026-01-01 12:00:00", "outTemp": 72.0},
        )

        with self.assertRaises(HTTPError) as ctx:
            self._get_json("/health")

        self.assertEqual(ctx.exception.code, 503)
        data = json.load(ctx.exception)
        self.assertEqual(data["status"], "stale")
        self.assertGreater(data["age_seconds"], 180.0)
        self.assertEqual(data["row_count"], 1)
