from datetime import UTC, datetime
from unittest import TestCase

from ambientweather2sqlite.prometheus import build_prometheus_metrics

_METRICS = {
    "row_count": 42,
    "db_file_size_bytes": 4096,
    "earliest_ts": "2026-01-01 00:00:00",
    "latest_ts": "2026-01-01 12:00:00",
    "column_count": 5,
}


class TestBuildPrometheusMetrics(TestCase):
    def test_includes_db_metrics(self):
        payload = build_prometheus_metrics(_METRICS, None)

        self.assertIn("aw2sqlite_observations_total 42", payload)
        self.assertIn("aw2sqlite_db_file_size_bytes 4096", payload)
        self.assertIn("# TYPE aw2sqlite_observations_total counter", payload)

    def test_includes_sensor_gauges_from_latest_observation(self):
        latest = {"ts": "2026-01-01 12:00:00", "outTemp": 72.5, "outHumi": 55.0}
        now = datetime(2026, 1, 1, 12, 1, 0, tzinfo=UTC)

        payload = build_prometheus_metrics(_METRICS, latest, now=now)

        self.assertIn('aw2sqlite_sensor_value{sensor="outTemp"} 72.5', payload)
        self.assertIn('aw2sqlite_sensor_value{sensor="outHumi"} 55.0', payload)
        self.assertIn("aw2sqlite_last_observation_age_seconds 60.0", payload)

    def test_skips_null_sensor_values(self):
        latest = {"ts": "2026-01-01 12:00:00", "outTemp": None}

        payload = build_prometheus_metrics(_METRICS, latest)

        self.assertNotIn('sensor="outTemp"', payload)

    def test_no_sensor_section_without_latest_observation(self):
        payload = build_prometheus_metrics(_METRICS, None)

        self.assertNotIn("aw2sqlite_sensor_value", payload)
        self.assertNotIn("aw2sqlite_last_observation_age_seconds", payload)

    def test_escapes_label_values(self):
        latest = {"ts": "2026-01-01 12:00:00", 'weird"name': 1.0}

        payload = build_prometheus_metrics(_METRICS, latest)

        self.assertIn('aw2sqlite_sensor_value{sensor="weird\\"name"} 1.0', payload)

    def test_ends_with_newline(self):
        payload = build_prometheus_metrics(_METRICS, None)

        self.assertTrue(payload.endswith("\n"))
