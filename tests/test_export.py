import csv
import io
import json
import tempfile
from pathlib import Path
from unittest import TestCase

from ambientweather2sqlite.database import (
    create_database_if_not_exists,
    export_observations,
    insert_observation,
)


class TestExportObservations(TestCase):
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        Path(self.db_path).unlink(missing_ok=True)
        create_database_if_not_exists(self.db_path)

        insert_observation(
            self.db_path,
            {"ts": "2026-01-01 10:00:00", "outTemp": 70.0, "outHumi": 50.0},
        )
        insert_observation(
            self.db_path,
            {"ts": "2026-01-02 10:00:00", "outTemp": 72.0, "outHumi": 55.0},
        )
        insert_observation(
            self.db_path,
            {"ts": "2026-01-03 10:00:00", "outTemp": 74.0, "outHumi": 60.0},
        )

    def tearDown(self):
        Path(self.db_path).unlink(missing_ok=True)

    def test_csv_export_includes_header_and_all_rows(self):
        output = io.StringIO()

        row_count = export_observations(self.db_path, output, fmt="csv")

        self.assertEqual(row_count, 3)
        rows = list(csv.reader(io.StringIO(output.getvalue())))
        self.assertEqual(len(rows), 4)
        self.assertIn("ts", rows[0])
        self.assertIn("outTemp", rows[0])
        ts_index = rows[0].index("ts")
        self.assertEqual(rows[1][ts_index], "2026-01-01 10:00:00")
        self.assertEqual(rows[3][ts_index], "2026-01-03 10:00:00")

    def test_csv_export_uses_lf_line_terminator(self):
        output = io.StringIO()

        export_observations(self.db_path, output, fmt="csv")

        self.assertNotIn("\r", output.getvalue())

    def test_json_export_is_valid_and_ordered(self):
        output = io.StringIO()

        row_count = export_observations(self.db_path, output, fmt="json")

        self.assertEqual(row_count, 3)
        data = json.loads(output.getvalue())
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]["ts"], "2026-01-01 10:00:00")
        self.assertEqual(data[0]["outTemp"], 70.0)
        self.assertEqual(data[2]["ts"], "2026-01-03 10:00:00")

    def test_json_export_of_empty_range_is_empty_array(self):
        output = io.StringIO()

        row_count = export_observations(
            self.db_path,
            output,
            fmt="json",
            start="2030-01-01",
        )

        self.assertEqual(row_count, 0)
        self.assertEqual(json.loads(output.getvalue()), [])

    def test_start_is_inclusive_and_end_is_exclusive(self):
        output = io.StringIO()

        row_count = export_observations(
            self.db_path,
            output,
            fmt="json",
            start="2026-01-02 10:00:00",
            end="2026-01-03 10:00:00",
        )

        self.assertEqual(row_count, 1)
        data = json.loads(output.getvalue())
        self.assertEqual(data[0]["ts"], "2026-01-02 10:00:00")

    def test_date_only_bounds_behave_as_midnight(self):
        output = io.StringIO()

        row_count = export_observations(
            self.db_path,
            output,
            fmt="json",
            start="2026-01-02",
            end="2026-01-03",
        )

        self.assertEqual(row_count, 1)
        data = json.loads(output.getvalue())
        self.assertEqual(data[0]["ts"], "2026-01-02 10:00:00")
