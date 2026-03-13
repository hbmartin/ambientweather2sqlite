import json
import socket
import tempfile
import time
from datetime import datetime
from http.client import HTTPResponse
from pathlib import Path
from typing import Any, cast
from unittest import TestCase
from urllib.error import HTTPError
from urllib.request import urlopen

from ambientweather2sqlite.database import (
    create_database_if_not_exists,
    insert_observation,
)
from ambientweather2sqlite.server import Server, _tz_from_query, create_request_handler


class TestServer(TestCase):
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        Path(self.db_path).unlink(missing_ok=True)
        was_created = create_database_if_not_exists(self.db_path)
        self.assertTrue(was_created)

        self.today = datetime.now().date()
        insert_observation(
            self.db_path,
            {
                "ts": f"{self.today} 12:00:00",
                "outTemp": 75.0,
                "outHumi": 50.0,
            },
        )

        self.server = Server("http://127.0.0.1:9", self.db_path, 0, "127.0.0.1")
        self.server.start()
        self.port = self.server.httpd.server_address[1]
        self._wait_for_server_ready()

    def tearDown(self):
        self.server.shutdown()
        Path(self.db_path).unlink(missing_ok=True)

    def _get_json(self, path: str) -> dict[str, dict[str, object]]:
        with urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=1) as response:
            return json.load(response)

    def _get_response(self, path: str) -> HTTPResponse:
        return urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=1)

    def _get_error_json(self, path: str) -> tuple[int, dict[str, object]]:
        try:
            with urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=1) as response:
                return response.status, json.load(response)
        except HTTPError as e:
            body = json.loads(e.read().decode("utf-8"))
            return e.code, body

    def _wait_for_server_ready(self, timeout: float = 2.0) -> None:
        deadline = time.monotonic() + timeout
        while True:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.1):
                    return
            except OSError:
                if time.monotonic() >= deadline:
                    self.fail(f"Server did not become ready within {timeout} seconds")
                time.sleep(0.01)

    def test_server_integration_hourly_data_supports_legacy_date_query(self):
        payload = self._get_json(
            f"/hourly?date={self.today}&tz=UTC&q=avg_outTemp",
        )

        self.assertIn("data", payload)
        self.assertIn(str(self.today), payload["data"])

    def test_hourly_with_start_date_and_end_date(self):
        payload = self._get_json(
            f"/hourly?start_date={self.today}&end_date={self.today}&tz=UTC&q=avg_outTemp",
        )

        self.assertIn("data", payload)
        self.assertIn(str(self.today), payload["data"])
        day_data = payload["data"][str(self.today)]
        self.assertEqual(len(day_data), 24)

    def test_hourly_missing_start_date_returns_400(self):
        status, body = self._get_error_json("/hourly?tz=UTC&q=avg_outTemp")
        self.assertEqual(status, 400)
        self.assertIn("error", body)
        self.assertIn("start_date", body["error"])

    def test_hourly_missing_tz_returns_400(self):
        status, body = self._get_error_json(
            f"/hourly?start_date={self.today}&q=avg_outTemp",
        )
        self.assertEqual(status, 400)
        self.assertIn("error", body)

    def test_hourly_invalid_date_returns_400(self):
        status, body = self._get_error_json(
            "/hourly?start_date=not-a-date&tz=UTC&q=avg_outTemp",
        )
        self.assertEqual(status, 400)
        self.assertIn("error", body)

    def test_hourly_missing_aggregation_fields_returns_400(self):
        status, body = self._get_error_json(
            f"/hourly?start_date={self.today}&tz=UTC",
        )
        self.assertEqual(status, 400)
        self.assertIn("error", body)

    def test_daily_returns_aggregated_data(self):
        payload = self._get_json("/daily?tz=UTC&q=avg_outTemp&days=7")

        self.assertIn("data", payload)
        self.assertIsInstance(payload["data"], list)

    def test_daily_with_multiple_aggregation_fields(self):
        payload = self._get_json(
            "/daily?tz=UTC&q=avg_outTemp&q=max_outTemp&q=min_outHumi&days=7",
        )

        self.assertIn("data", payload)
        if payload["data"]:
            row = payload["data"][0]
            self.assertIn("avg_outTemp", row)
            self.assertIn("max_outTemp", row)
            self.assertIn("min_outHumi", row)

    def test_daily_defaults_to_7_days(self):
        payload = self._get_json("/daily?tz=UTC&q=avg_outTemp")
        self.assertIn("data", payload)

    def test_daily_invalid_days_returns_400(self):
        status, body = self._get_error_json("/daily?tz=UTC&q=avg_outTemp&days=abc")
        self.assertEqual(status, 400)
        self.assertIn("error", body)
        self.assertIn("days must be int", body["error"])

    def test_daily_missing_tz_returns_400(self):
        status, body = self._get_error_json("/daily?q=avg_outTemp")
        self.assertEqual(status, 400)
        self.assertIn("error", body)

    def test_daily_missing_aggregation_fields_returns_400(self):
        status, body = self._get_error_json("/daily?tz=UTC")
        self.assertEqual(status, 400)
        self.assertIn("error", body)

    def test_daily_invalid_aggregation_field_returns_400(self):
        status, body = self._get_error_json("/daily?tz=UTC&q=badfield")
        self.assertEqual(status, 400)
        self.assertIn("error", body)

    def test_health_returns_status_and_metrics(self):
        payload = self._get_json("/health")

        self.assertEqual(payload["status"], "ok")
        self.assertIn("last_observation_ts", payload)
        self.assertIn("row_count", payload)
        self.assertEqual(payload["row_count"], 1)

    def test_metrics_returns_db_metrics(self):
        payload = self._get_json("/metrics")

        self.assertIn("row_count", payload)
        self.assertIn("db_file_size_bytes", payload)
        self.assertIn("earliest_ts", payload)
        self.assertIn("latest_ts", payload)
        self.assertIn("column_count", payload)
        self.assertEqual(payload["row_count"], 1)

    def test_unknown_path_returns_404(self):
        status, body = self._get_error_json("/nonexistent")
        self.assertEqual(status, 404)
        self.assertIn("error", body)
        self.assertEqual(body["error"], "Not found")

    def test_response_has_cors_header(self):
        response = self._get_response("/health")
        self.assertEqual(response.headers["Access-Control-Allow-Origin"], "*")
        response.close()

    def test_response_content_type_is_json(self):
        response = self._get_response("/health")
        self.assertEqual(response.headers["Content-type"], "application/json")
        response.close()

    def test_live_data_endpoint_returns_error_for_unreachable_url(self):
        status, body = self._get_error_json("/")
        self.assertEqual(status, 500)
        self.assertIn("error", body)

    def test_daily_with_named_timezone(self):
        payload = self._get_json(
            "/daily?tz=America%2FNew_York&q=avg_outTemp&days=7",
        )
        self.assertIn("data", payload)

    def test_hourly_with_named_timezone(self):
        payload = self._get_json(
            f"/hourly?start_date={self.today}&tz=America%2FNew_York&q=avg_outTemp",
        )
        self.assertIn("data", payload)

    def test_daily_invalid_timezone_returns_400(self):
        status, body = self._get_error_json(
            "/daily?tz=Not%2FReal%2FTimezone&q=avg_outTemp",
        )
        self.assertEqual(status, 400)
        self.assertIn("error", body)

    def test_create_request_handler_reuses_a_single_file_handler_per_instance(self):
        handler_class = create_request_handler(
            "http://127.0.0.1:9",
            self.db_path,
        )
        duplicate_handler_class = create_request_handler(
            "http://127.0.0.1:9",
            self.db_path,
        )
        typed_handler_class = cast("Any", handler_class)
        typed_duplicate_handler_class = cast("Any", duplicate_handler_class)

        self.assertEqual(typed_handler_class.log_handler_count(), 1)
        self.assertEqual(typed_duplicate_handler_class.log_handler_count(), 1)

        typed_handler_class.teardown_logger()
        typed_duplicate_handler_class.teardown_logger()


class TestTzFromQuery(TestCase):
    def test_returns_timezone_from_query(self):
        result = _tz_from_query({"tz": ["UTC"]})
        self.assertEqual(result, "UTC")

    def test_decodes_url_encoded_timezone(self):
        result = _tz_from_query({"tz": ["America%2FNew_York"]})
        self.assertEqual(result, "America/New_York")

    def test_raises_on_missing_tz(self):
        from ambientweather2sqlite.exceptions import InvalidTimezoneError

        with self.assertRaises(InvalidTimezoneError):
            _tz_from_query({})

    def test_raises_on_empty_tz_list(self):
        from ambientweather2sqlite.exceptions import InvalidTimezoneError

        with self.assertRaises(InvalidTimezoneError):
            _tz_from_query({"tz": []})
