import json
import socket
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, cast
from unittest import TestCase
from urllib.request import urlopen

from ambientweather2sqlite.database import (
    create_database_if_not_exists,
    insert_observation,
)
from ambientweather2sqlite.server import Server, create_request_handler


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
