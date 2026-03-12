import json
import tempfile
import time
from datetime import datetime
from pathlib import Path
from unittest import TestCase
from urllib.request import urlopen

from ambientweather2sqlite.database import (
    create_database_if_not_exists,
    insert_observation,
)
from ambientweather2sqlite.server import Server


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
        time.sleep(0.05)

    def tearDown(self):
        self.server.shutdown()
        Path(self.db_path).unlink(missing_ok=True)

    def _get_json(self, path) -> dict[str, object]:
        with urlopen(f"http://127.0.0.1:{self.port}{path}") as response:
            return json.load(response)

    def test_server_integration_hourly_data_supports_legacy_date_query(self):
        payload = self._get_json(
            f"/hourly?date={self.today}&tz=UTC&q=avg_outTemp",
        )

        self.assertIn("data", payload)
        self.assertIn(str(self.today), payload["data"])
