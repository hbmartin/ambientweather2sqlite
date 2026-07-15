import sqlite3
import unittest
from contextlib import closing

from ambientweather2sqlite.database.identifiers import _resolve_sensor_names


class TestSensorNameResolution(unittest.TestCase):
    def test_loads_sensor_mappings_once_per_batch(self) -> None:
        statements: list[str] = []
        with closing(sqlite3.connect(":memory:")) as conn:
            conn.set_trace_callback(statements.append)

            resolved = _resolve_sensor_names(
                conn,
                ["outTemp", "sensor-name", "sensor.name"],
            )

        mapping_selects = [
            statement
            for statement in statements
            if statement.startswith("SELECT raw_name, column_name")
        ]
        self.assertEqual(len(mapping_selects), 1)
        self.assertEqual(
            resolved,
            {
                "outTemp": "outTemp",
                "sensor-name": "sensor_name",
                "sensor.name": "aw2_73656e736f722e6e616d65",
            },
        )
