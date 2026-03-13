import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from ambientweather2sqlite.database import (
    _column_name,
    _insert_dict_row,
    create_database_if_not_exists,
    insert_observation,
)


class TestDatabaseUtilityFunctions(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures with temporary database."""
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        Path(self.db_path).unlink(missing_ok=True)

    def tearDown(self):
        """Clean up after tests."""
        Path(self.db_path).unlink(missing_ok=True)

    def test_column_name_function(self):
        """Test _column_name function."""
        test_cases = [
            ("simple", "simple"),
            ("with spaces", "with_spaces"),
            ("with-dashes", "with_dashes"),
            ("with.dots", "with_dots"),
            ("with@symbols", "with_symbols"),
            ("MixedCase123", "MixedCase123"),
            ("", ""),
            ("123numbers", "123numbers"),
        ]

        for input_str, expected in test_cases:
            with self.subTest(input_str=input_str):
                result = _column_name(input_str)
                self.assertEqual(result, expected)

    def test_create_database_if_not_exists_new_db(self):
        """Test creating a new database."""
        was_created = create_database_if_not_exists(self.db_path)
        self.assertTrue(was_created)
        self.assertTrue(Path(self.db_path).exists())

        # Verify observations table was created
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='observations'",
            )
            result = cursor.fetchone()
            self.assertIsNotNone(result)
            self.assertEqual(result[0], "observations")

    def test_create_database_if_not_exists_existing_db(self):
        """Test with existing database."""
        # Create database first
        create_database_if_not_exists(self.db_path)

        # Try to create again
        was_created = create_database_if_not_exists(self.db_path)
        self.assertFalse(was_created)

    def test_observations_table_structure(self):
        """Test that observations table has correct structure."""
        create_database_if_not_exists(self.db_path)

        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(observations)")
            columns = cursor.fetchall()

            # Should have at least the ts column
            column_names = {col[1] for col in columns}
            self.assertIn("ts", column_names)

    def test_insert_observation_basic(self):
        """Test basic observation insertion."""
        create_database_if_not_exists(self.db_path)

        test_data = {
            "outTemp": 75.5,
            "outHumi": 60.0,
            "windSpeed": 10.2,
        }

        insert_observation(self.db_path, test_data)

        # Verify data was inserted
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM observations")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 1)

    def test_insert_observation_creates_columns(self):
        """Test that insert_observation creates columns as needed."""
        create_database_if_not_exists(self.db_path)

        test_data = {
            "newColumn": 42.0,
            "anotherColumn": 33.3,
        }

        insert_observation(self.db_path, test_data)

        # Verify columns were created
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(observations)")
            columns = cursor.fetchall()

            column_names = {col[1] for col in columns}
            self.assertIn("newColumn", column_names)
            self.assertIn("anotherColumn", column_names)

    def test_insert_multiple_observations(self):
        """Test inserting multiple observations with distinct timestamps."""
        create_database_if_not_exists(self.db_path)

        test_data_list = [
            {"ts": "2026-01-01 12:00:00", "outTemp": 75.0, "outHumi": 60.0},
            {"ts": "2026-01-01 12:01:00", "outTemp": 77.0, "outHumi": 58.0},
            {"ts": "2026-01-01 12:02:00", "outTemp": 79.0, "outHumi": 55.0},
        ]

        for data in test_data_list:
            insert_observation(self.db_path, data)

        # Verify all data was inserted
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM observations")
            count = cursor.fetchone()[0]
            self.assertEqual(count, len(test_data_list))

    def test_insert_observation_without_ts_uses_unique_precise_timestamps(self):
        create_database_if_not_exists(self.db_path)

        with patch(
            "ambientweather2sqlite.database._current_observation_timestamp",
            side_effect=[
                "2026-01-01 12:00:00.000001",
                "2026-01-01 12:00:00.000002",
            ],
        ):
            insert_observation(self.db_path, {"outTemp": 75.0})
            insert_observation(self.db_path, {"outTemp": 76.0})

        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ts FROM observations ORDER BY ts")
            timestamps = [row[0] for row in cursor.fetchall()]

        self.assertEqual(
            timestamps,
            [
                "2026-01-01 12:00:00.000001",
                "2026-01-01 12:00:00.000002",
            ],
        )

    def test_insert_observation_normalizes_invalid_ts_values(self):
        create_database_if_not_exists(self.db_path)

        with patch(
            "ambientweather2sqlite.database._current_observation_timestamp",
            side_effect=[
                "2026-01-01 12:00:00.000010",
                "2026-01-01 12:00:00.000011",
            ],
        ):
            insert_observation(self.db_path, {"ts": None, "outTemp": 75.0})
            insert_observation(self.db_path, {"ts": "   ", "outTemp": 76.0})

        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ts FROM observations ORDER BY ts")
            timestamps = [row[0] for row in cursor.fetchall()]

        self.assertEqual(
            timestamps,
            [
                "2026-01-01 12:00:00.000010",
                "2026-01-01 12:00:00.000011",
            ],
        )

    def test_create_database_migrates_duplicate_timestamps(self):
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE observations (ts TIMESTAMP, outTemp REAL)")
            cursor.executemany(
                "INSERT INTO observations (ts, outTemp) VALUES (?, ?)",
                [
                    ("2026-01-01 12:00:00", 70.0),
                    ("2026-01-01 12:00:00", 72.0),
                ],
            )
            conn.commit()

        was_created = create_database_if_not_exists(self.db_path)

        self.assertFalse(was_created)
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM observations")
            count = cursor.fetchone()[0]
            cursor.execute("SELECT outTemp FROM observations")
            remaining_temp = cursor.fetchone()[0]
            cursor.execute("PRAGMA index_list(observations)")
            indexes = cursor.fetchall()

        self.assertEqual(count, 1)
        self.assertEqual(remaining_temp, 72.0)
        self.assertTrue(any(index[2] for index in indexes))

    def test_insert_dict_row_returns_none_when_duplicate_is_ignored(self):
        create_database_if_not_exists(self.db_path)
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute("ALTER TABLE observations ADD COLUMN outTemp REAL")
            first_rowid = _insert_dict_row(
                conn,
                "observations",
                {"ts": "2026-01-01 12:00:00", "outTemp": 70.0},
            )
            second_rowid = _insert_dict_row(
                conn,
                "observations",
                {"ts": "2026-01-01 12:00:00", "outTemp": 72.0},
            )

        self.assertIsNotNone(first_rowid)
        self.assertIsNone(second_rowid)

    def test_insert_observation_with_special_column_names(self):
        """Test insertion with column names requiring sanitization."""
        create_database_if_not_exists(self.db_path)

        test_data = {
            "column with spaces": 1.0,
            "column-with-dashes": 2.0,
            "column.with.dots": 3.0,
        }

        insert_observation(self.db_path, test_data)

        # Verify data was inserted and columns were sanitized
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(observations)")
            columns = cursor.fetchall()

            column_names = {col[1] for col in columns}
            self.assertIn("column_with_spaces", column_names)
            self.assertIn("column_with_dashes", column_names)
            self.assertIn("column_with_dots", column_names)


if __name__ == "__main__":
    unittest.main()
