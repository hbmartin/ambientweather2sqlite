"""Integration test for the full daemon -> parse -> insert -> query cycle."""

import json
import sqlite3
import tempfile
import threading
import time
from contextlib import closing
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from ambientweather2sqlite.daemon import fetch_once, start_daemon
from ambientweather2sqlite.database import (
    create_database_if_not_exists,
    query_daily_aggregated_data,
)

_SAMPLE_LIVEDATA_HTML = """
<html><body>
<table>
<tr>
  <td>Outdoor Temperature</td>
  <td><input name="outTemp" value="72.5" disabled></td>
</tr>
<tr>
  <td>Outdoor Humidity</td>
  <td><input name="outHumi" value="55.0" disabled></td>
</tr>
<tr>
  <td>Wind Speed</td>
  <td><input name="avgwind" value="5.3" disabled></td>
</tr>
</table>
</body></html>
"""

_SAMPLE_STATION_HTML = """
<html><body>
<div class="item_1">Temperature</div>
<select><option selected>degF</option></select>
<div class="item_1">Wind</div>
<select><option selected>mph</option></select>
</body></html>
"""


def _create_mock_server() -> tuple[HTTPServer, int]:
    """Create an HTTP server serving sample weather station HTML."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            if "station.htm" in self.path:
                self.wfile.write(_SAMPLE_STATION_HTML.encode())
            else:
                self.wfile.write(_SAMPLE_LIVEDATA_HTML.encode())

        def log_message(self, format: str, *args: object) -> None:
            pass  # Suppress server logs during tests

    server = HTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    return server, port


class TestDaemonIntegration(TestCase):
    def setUp(self):
        self.mock_server, self.mock_port = _create_mock_server()
        self.server_thread = threading.Thread(
            target=self.mock_server.serve_forever,
            daemon=True,
        )
        self.server_thread.start()
        self.live_data_url = f"http://127.0.0.1:{self.mock_port}/livedata.htm"

        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        Path(self.db_path).unlink(missing_ok=True)
        create_database_if_not_exists(self.db_path)

    def tearDown(self):
        self.mock_server.shutdown()
        self.server_thread.join(timeout=2)
        Path(self.db_path).unlink(missing_ok=True)
        # Clean up metadata and log files
        db_stem = Path(self.db_path).stem
        db_dir = Path(self.db_path).parent
        for suffix in ["_metadata.json", "_daemon.log", "_server.log"]:
            (db_dir / f"{db_stem}{suffix}").unlink(missing_ok=True)

    def _wait_for_row_count(self, minimum: int = 1, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with closing(sqlite3.connect(self.db_path)) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM observations")
                if cursor.fetchone()[0] >= minimum:
                    return True
            time.sleep(0.2)
        return False

    def _run_single_cycle_daemon(self) -> None:
        try:
            start_daemon(
                self.live_data_url,
                self.db_path,
                period_seconds=1,
            )
        except SystemExit as exc:
            self.assertEqual(exc.code, 0)

    def test_daemon_single_cycle_inserts_observation(self):
        """Run the daemon for a single cycle and verify data is inserted."""
        with patch(
            "ambientweather2sqlite.daemon.wait_for_next_update",
            side_effect=KeyboardInterrupt,
        ):
            daemon_thread = threading.Thread(
                target=self._run_single_cycle_daemon,
            )
            daemon_thread.start()
            row_observed = self._wait_for_row_count()
            daemon_thread.join(timeout=2)

        self.assertTrue(row_observed)
        self.assertFalse(daemon_thread.is_alive())

        # Verify the inserted data has expected columns
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM observations ORDER BY ts DESC LIMIT 1")
            row = cursor.fetchone()
            self.assertIsNotNone(row)
            self.assertAlmostEqual(row["outTemp"], 72.5)
            self.assertAlmostEqual(row["outHumi"], 55.0)
            self.assertAlmostEqual(row["avgwind"], 5.3)

    def test_daemon_data_is_queryable_via_aggregation(self):
        """Verify the full pipeline: daemon inserts -> aggregation query works."""
        with patch(
            "ambientweather2sqlite.daemon.wait_for_next_update",
            side_effect=KeyboardInterrupt,
        ):
            daemon_thread = threading.Thread(
                target=self._run_single_cycle_daemon,
            )
            daemon_thread.start()
            row_observed = self._wait_for_row_count()
            daemon_thread.join(timeout=2)

        self.assertTrue(row_observed)
        self.assertFalse(daemon_thread.is_alive())

        result = query_daily_aggregated_data(
            db_path=self.db_path,
            aggregation_fields=["avg_outTemp"],
            prior_days=1,
        )

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        self.assertIn("avg_outTemp", result[0])
        self.assertAlmostEqual(result[0]["avg_outTemp"], 72.5)


class TestFetchOnce(TestCase):
    def setUp(self):
        self.mock_server, self.mock_port = _create_mock_server()
        self.server_thread = threading.Thread(
            target=self.mock_server.serve_forever,
            daemon=True,
        )
        self.server_thread.start()
        self.live_data_url = f"http://127.0.0.1:{self.mock_port}/livedata.htm"

    def tearDown(self):
        self.mock_server.shutdown()
        self.server_thread.join(timeout=2)

    def test_fetch_once_prints_json(self):
        """Test that fetch_once outputs valid JSON with sensor data."""
        import io
        import sys

        captured = io.StringIO()
        old_stdout = sys.stdout
        sys.stdout = captured

        try:
            fetch_once(self.live_data_url)
        finally:
            sys.stdout = old_stdout

        output = captured.getvalue()
        data = json.loads(output)

        self.assertIn("outTemp", data)
        self.assertAlmostEqual(data["outTemp"], 72.5)
        self.assertAlmostEqual(data["outHumi"], 55.0)
