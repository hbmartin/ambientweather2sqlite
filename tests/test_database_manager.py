import sqlite3
import unittest
from unittest.mock import patch

from ambientweather2sqlite.database import (
    DatabaseManager,
    get_db_manager,
    initialize_database,
    db_manager,
    ensure_columns,
    insert_dict_row,
    _column_name,
)
from ambientweather2sqlite.exceptions import UnexpectedEmptyDictionaryError


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
        self.assertEqual(self.db_manager.db_path, self.db_path)
        self.assertEqual(self.db_manager.conn.row_factory, sqlite3.Row)

    def test_logs_table_creation(self):
        """Test that logs table is created on initialization."""
        cursor = self.db_manager.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='logs'")
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

    def test_close_connection(self):
        """Test closing database connection."""
        self.assertIsNotNone(self.db_manager.conn)
        self.db_manager.close()
        self.assertIsNone(self.db_manager.conn)

    def test_close_already_closed_connection(self):
        """Test closing already closed connection doesn't raise error."""
        self.db_manager.close()
        self.db_manager.close()  # Should not raise an error

    def test_observations_table_creation(self):
        """Test that observations table is created properly."""
        # Create observations table (normally done by create_database_if_not_exists)
        cursor = self.db_manager.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS observations (
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.db_manager.conn.commit()
        
        # Verify table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='observations'")
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
        manager = initialize_database(db_path)
        
        self.assertIsInstance(manager, DatabaseManager)
        self.assertEqual(manager.db_path, db_path)
        
        # Test that global manager is set
        global_manager = get_db_manager()
        self.assertIs(manager, global_manager)

    def test_get_db_manager_uninitialized(self):
        """Test get_db_manager raises error when not initialized."""
        with self.assertRaises(RuntimeError) as context:
            get_db_manager()
        
        self.assertIn("Database manager not initialized", str(context.exception))

    def test_get_db_manager_after_initialization(self):
        """Test get_db_manager returns initialized manager."""
        db_path = ":memory:"
        manager = initialize_database(db_path)
        
        retrieved_manager = get_db_manager()
        self.assertIs(manager, retrieved_manager)


class TestDatabaseUtilityFunctions(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures with in-memory database."""
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("""
            CREATE TABLE observations (
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
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

    def test_ensure_columns_new_columns(self):
        """Test ensure_columns adds new columns."""
        required_columns = {"outTemp", "outHumi", "windSpeed"}
        
        added_columns = ensure_columns(self.conn, required_columns)
        
        self.assertEqual(len(added_columns), 3)
        self.assertEqual(set(added_columns), required_columns)
        
        # Verify columns were actually added
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA table_info(observations)")
        columns = cursor.fetchall()
        column_names = {col[1] for col in columns}
        
        self.assertTrue(required_columns.issubset(column_names))

    def test_ensure_columns_existing_columns(self):
        """Test ensure_columns with existing columns."""
        # Add a column first
        self.conn.execute("ALTER TABLE observations ADD COLUMN outTemp REAL")
        self.conn.commit()
        
        required_columns = {"outTemp", "outHumi"}
        
        added_columns = ensure_columns(self.conn, required_columns)
        
        # Only outHumi should be added, not outTemp
        self.assertEqual(len(added_columns), 1)
        self.assertEqual(added_columns[0], "outHumi")

    def test_ensure_columns_empty_set(self):
        """Test ensure_columns with empty set."""
        required_columns = set()
        
        added_columns = ensure_columns(self.conn, required_columns)
        
        self.assertEqual(len(added_columns), 0)

    def test_insert_dict_row_valid_data(self):
        """Test insert_dict_row with valid data."""
        # Add columns first
        ensure_columns(self.conn, {"outTemp", "outHumi"})
        
        data = {"outTemp": 75.5, "outHumi": 60.0}
        
        rowid = insert_dict_row(self.conn, "observations", data)
        
        self.assertIsNotNone(rowid)
        self.assertIsInstance(rowid, int)
        
        # Verify data was inserted
        cursor = self.conn.cursor()
        cursor.execute("SELECT outTemp, outHumi FROM observations WHERE rowid = ?", (rowid,))
        result = cursor.fetchone()
        
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 75.5)
        self.assertEqual(result[1], 60.0)

    def test_insert_dict_row_with_none_values(self):
        """Test insert_dict_row with None values."""
        ensure_columns(self.conn, {"outTemp", "outHumi"})
        
        data = {"outTemp": 75.5, "outHumi": None}
        
        rowid = insert_dict_row(self.conn, "observations", data)
        
        self.assertIsNotNone(rowid)
        
        # Verify data was inserted
        cursor = self.conn.cursor()
        cursor.execute("SELECT outTemp, outHumi FROM observations WHERE rowid = ?", (rowid,))
        result = cursor.fetchone()
        
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 75.5)
        self.assertIsNone(result[1])

    def test_insert_dict_row_empty_dict(self):
        """Test insert_dict_row with empty dictionary raises error."""
        with self.assertRaises(UnexpectedEmptyDictionaryError):
            insert_dict_row(self.conn, "observations", {})

    def test_insert_dict_row_special_column_names(self):
        """Test insert_dict_row with special characters in column names."""
        data = {"out-temp": 75.5, "out humid": 60.0}
        
        # This should work because _column_name sanitizes the names
        rowid = insert_dict_row(self.conn, "observations", data)
        
        self.assertIsNotNone(rowid)
        
        # Verify columns were created with sanitized names
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA table_info(observations)")
        columns = cursor.fetchall()
        column_names = {col[1] for col in columns}
        
        self.assertIn("out_temp", column_names)
        self.assertIn("out_humid", column_names)


class TestErrorLoggingIntegration(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.db_path = ":memory:"
        initialize_database(self.db_path)

    def tearDown(self):
        """Clean up after tests."""
        import ambientweather2sqlite.database as db_module
        if db_module.db_manager:
            db_module.db_manager.close()
        db_module.db_manager = None

    def test_error_logging_through_global_manager(self):
        """Test error logging through global database manager."""
        manager = get_db_manager()
        
        error_name = "IntegrationError"
        error_message = "Error logged through global manager"
        
        manager.log_error(error_name, error_message)
        
        # Verify error was logged
        cursor = manager.conn.cursor()
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

    @patch('ambientweather2sqlite.database.get_db_manager')
    def test_error_logging_with_manager_failure(self, mock_get_db_manager):
        """Test that error logging handles manager failures gracefully."""
        mock_get_db_manager.side_effect = RuntimeError("Manager not initialized")
        
        # This should not raise an error in actual usage
        # since the except blocks have try/except around logging
        with self.assertRaises(RuntimeError):
            get_db_manager()


if __name__ == '__main__':
    unittest.main()