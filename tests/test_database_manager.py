import sqlite3
import tempfile
import unittest
from pathlib import Path

from ambientweather2sqlite.database import (
    _column_name,
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
        with sqlite3.connect(self.db_path) as conn:
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

        with sqlite3.connect(self.db_path) as conn:
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
        with sqlite3.connect(self.db_path) as conn:
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
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(observations)")
            columns = cursor.fetchall()

            column_names = {col[1] for col in columns}
            self.assertIn("newColumn", column_names)
            self.assertIn("anotherColumn", column_names)

    def test_insert_multiple_observations(self):
        """Test inserting multiple observations."""
        create_database_if_not_exists(self.db_path)

        test_data_list = [
            {"outTemp": 75.0, "outHumi": 60.0},
            {"outTemp": 77.0, "outHumi": 58.0},
            {"outTemp": 79.0, "outHumi": 55.0},
        ]

        for data in test_data_list:
            insert_observation(self.db_path, data)

        # Verify all data was inserted
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM observations")
            count = cursor.fetchone()[0]
            self.assertEqual(count, len(test_data_list))

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
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(observations)")
            columns = cursor.fetchall()

            column_names = {col[1] for col in columns}
            self.assertIn("column_with_spaces", column_names)
            self.assertIn("column_with_dashes", column_names)
            self.assertIn("column_with_dots", column_names)


if __name__ == "__main__":
    unittest.main()
