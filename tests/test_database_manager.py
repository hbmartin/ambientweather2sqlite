import sqlite3
import unittest

from ambientweather2sqlite.database import (
    DatabaseManager,
    _column_name,
    get_db_manager,
)
from ambientweather2sqlite.exceptions import DatabaseNotInitializedError


class TestDatabaseManager(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures with in-memory database."""
        self.db_path = ":memory:"
        self.db_manager = DatabaseManager(self.db_path)

    def tearDown(self):
        """Clean up after tests."""
        if self.db_manager:
            self.db_manager.close()
        # Reset global db_manager
        import ambientweather2sqlite.database as db_module

        db_module.db_manager = None

    def test_database_manager_initialization(self):
        """Test DatabaseManager initialization."""
        self.assertIsNotNone(self.db_manager.conn)
        self.assertEqual(self.db_manager.conn.row_factory, sqlite3.Row)

    def test_logs_table_creation(self):
        """Test that logs table is created on initialization."""
        cursor = self.db_manager.conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='logs'",
        )
        result = cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], "logs")

    def test_logs_table_structure(self):
        """Test that logs table has correct structure."""
        cursor = self.db_manager.conn.cursor()
        cursor.execute("PRAGMA table_info(logs)")
        columns = cursor.fetchall()

        expected_columns = {"ts", "error", "message"}
        actual_columns = {col[1] for col in columns}

        self.assertTrue(expected_columns.issubset(actual_columns))

    def test_log_error_functionality(self):
        """Test error logging functionality."""
        error_name = "TestError"
        error_message = "This is a test error message"

        self.db_manager.log_error(error_name, error_message)

        # Verify error was logged
        cursor = self.db_manager.conn.cursor()
        cursor.execute("SELECT error, message FROM logs WHERE error = ?", (error_name,))
        result = cursor.fetchone()

        self.assertIsNotNone(result)
        self.assertEqual(result[0], error_name)
        self.assertEqual(result[1], error_message)

    def test_log_error_with_timestamp(self):
        """Test that error logging includes timestamp."""
        error_name = "TimestampError"
        error_message = "Error with timestamp"

        self.db_manager.log_error(error_name, error_message)

        cursor = self.db_manager.conn.cursor()
        cursor.execute("SELECT ts FROM logs WHERE error = ?", (error_name,))
        result = cursor.fetchone()

        self.assertIsNotNone(result)
        self.assertIsNotNone(result[0])  # Timestamp should not be None

    def test_multiple_error_logs(self):
        """Test logging multiple errors."""
        errors = [
            ("Error1", "First error"),
            ("Error2", "Second error"),
            ("Error3", "Third error"),
        ]

        for error_name, error_message in errors:
            self.db_manager.log_error(error_name, error_message)

        cursor = self.db_manager.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM logs")
        count = cursor.fetchone()[0]

        self.assertEqual(count, len(errors))

    def test_close_already_closed_connection(self):
        """Test closing already closed connection doesn't raise error."""
        self.db_manager.close()
        self.db_manager.close()  # Should not raise an error

    def test_observations_table_creation(self):
        """Test that observations table is created properly."""
        # Create observations table (normally done by create_database_if_not_exists)
        cursor = self.db_manager.conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS observations (
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        )
        self.db_manager.conn.commit()

        # Verify table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='observations'",
        )
        result = cursor.fetchone()
        self.assertIsNotNone(result)


class TestGlobalDatabaseManager(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        # Reset global db_manager
        import ambientweather2sqlite.database as db_module

        db_module.db_manager = None

    def tearDown(self):
        """Clean up after tests."""
        # Reset global db_manager
        import ambientweather2sqlite.database as db_module

        if db_module.db_manager:
            db_module.db_manager.close()
        db_module.db_manager = None

    def test_initialize_database(self):
        """Test initialize_database function."""
        db_path = ":memory:"
        manager = get_db_manager(db_path)

        self.assertIsInstance(manager, DatabaseManager)

        # Test that global manager is set
        global_manager = get_db_manager()
        self.assertIs(manager, global_manager)

    def test_get_db_manager_uninitialized(self):
        """Test get_db_manager raises error when not initialized."""
        with self.assertRaises(DatabaseNotInitializedError) as context:
            get_db_manager()

        self.assertIn("Database manager not initialized", str(context.exception))


class TestDatabaseUtilityFunctions(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures with in-memory database."""
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute(
            """
            CREATE TABLE observations (
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        )
        self.conn.commit()

    def tearDown(self):
        """Clean up after tests."""
        self.conn.close()

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


class TestErrorLoggingIntegration(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.db_path = ":memory:"
        self.db_manager = get_db_manager(self.db_path)

    def tearDown(self):
        """Clean up after tests."""
        import ambientweather2sqlite.database as db_module

        if db_module.db_manager:
            db_module.db_manager.close()
        db_module.db_manager = None

    def test_error_logging_through_global_manager(self):
        """Test error logging through global database manager."""
        error_name = "IntegrationError"
        error_message = "Error logged through global manager"

        self.db_manager.log_error(error_name, error_message)

        # Verify error was logged
        cursor = self.db_manager.conn.cursor()
        cursor.execute("SELECT error, message FROM logs WHERE error = ?", (error_name,))
        result = cursor.fetchone()

        self.assertIsNotNone(result)
        self.assertEqual(result[0], error_name)
        self.assertEqual(result[1], error_message)

    def test_database_manager_survives_multiple_operations(self):
        """Test that database manager can handle multiple operations."""
        manager = get_db_manager()

        # Log multiple errors
        for i in range(10):
            manager.log_error(f"Error{i}", f"Message {i}")

        # Verify all errors were logged
        cursor = manager.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM logs")
        count = cursor.fetchone()[0]

        self.assertEqual(count, 10)


if __name__ == "__main__":
    unittest.main()
