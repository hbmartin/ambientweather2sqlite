import json
import sqlite3
import tempfile
import threading
import time
import unittest
from http.client import HTTPConnection
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from ambientweather2sqlite.database import (
    create_database_if_not_exists,
    insert_observation,
)
from ambientweather2sqlite.server import (
    Server,
    _tz_from_query,
    create_request_handler,
    get_int_argument,
    get_str_argument,
)


class TestServerUtilityFunctions(unittest.TestCase):
    def test_tz_from_query_with_valid_tz(self):
        """Test _tz_from_query with valid timezone in query parameters."""
        query = {"tz": ["America/New_York"]}
        result = _tz_from_query(query)
        self.assertEqual(result, "America/New_York")

    def test_tz_from_query_with_url_encoded_tz(self):
        """Test _tz_from_query with URL-encoded timezone."""
        query = {"tz": ["America%2FNew_York"]}
        result = _tz_from_query(query)
        self.assertEqual(result, "America/New_York")

    def test_tz_from_query_without_tz(self):
        """Test _tz_from_query raises error when tz is missing."""
        from ambientweather2sqlite.exceptions import InvalidTimezoneError

        query = {"other": ["value"]}
        with self.assertRaises(InvalidTimezoneError):
            _tz_from_query(query)

    def test_tz_from_query_with_empty_tz_list(self):
        """Test _tz_from_query raises error when tz list is empty."""
        from ambientweather2sqlite.exceptions import InvalidTimezoneError

        query = {"tz": []}
        with self.assertRaises(InvalidTimezoneError):
            _tz_from_query(query)

    def test_get_int_argument_with_integer(self):
        """Test get_int_argument returns first integer found."""
        args = ["hello", "123", "456"]
        result = get_int_argument(args)
        self.assertEqual(result, 123)

    def test_get_int_argument_with_no_integers(self):
        """Test get_int_argument returns None when no integers found."""
        args = ["hello", "world", "test"]
        result = get_int_argument(args)
        self.assertIsNone(result)

    def test_get_int_argument_with_empty_list(self):
        """Test get_int_argument returns None for empty list."""
        args = []
        result = get_int_argument(args)
        self.assertIsNone(result)

    def test_get_str_argument_with_non_numeric_string(self):
        """Test get_str_argument returns first non-numeric string."""
        args = ["123", "hello", "456"]
        result = get_str_argument(args)
        self.assertEqual(result, "hello")

    def test_get_str_argument_with_only_numbers(self):
        """Test get_str_argument returns None when all arguments are numeric."""
        args = ["123", "456", "789"]
        result = get_str_argument(args)
        self.assertIsNone(result)

    def test_get_str_argument_with_empty_list(self):
        """Test get_str_argument returns None for empty list."""
        args = []
        result = get_str_argument(args)
        self.assertIsNone(result)


class TestRequestHandler(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures with temporary database."""
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        Path(self.db_path).unlink(missing_ok=True)

        # Create database and insert test data
        create_database_if_not_exists(self.db_path)
        test_data = [
            {
                "ts": "2025-06-27 12:00:00",
                "outTemp": 75.0,
                "outHumi": 60.0,
                "gustspeed": 10.0,
            },
            {
                "ts": "2025-06-27 13:00:00",
                "outTemp": 77.0,
                "outHumi": 58.0,
                "gustspeed": 15.0,
            },
        ]
        for data in test_data:
            insert_observation(self.db_path, data)

        self.live_data_url = "http://test.example.com/live"
        self.handler_class = create_request_handler(self.live_data_url, self.db_path)

    def tearDown(self):
        """Clean up after tests."""
        Path(self.db_path).unlink(missing_ok=True)

    def test_create_request_handler_returns_class(self):
        """Test create_request_handler returns a class."""
        handler_class = create_request_handler("http://test.com", "/tmp/test.db")
        self.assertTrue(callable(handler_class))
        self.assertEqual(handler_class.LIVE_DATA_URL, "http://test.com")
        self.assertEqual(handler_class.DB_PATH, "/tmp/test.db")

    @patch("ambientweather2sqlite.server.mureq.get")
    def test_send_live_data_success(self, mock_get):
        """Test _send_live_data with successful response."""
        # Mock response from live data URL
        mock_get.return_value = """
        <td>Temperature (°F)</td><td>75.5</td>
        <td>Humidity (%)</td><td>60</td>
        """

        # Create handler instance without triggering HTTP processing
        handler = self.handler_class.__new__(self.handler_class)
        handler.LIVE_DATA_URL = self.live_data_url
        handler.DB_PATH = self.db_path
        handler.wfile = Mock()
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.log_message = Mock()
        handler._setup_logger()

        handler._send_live_data()

        # Verify response was sent
        mock_get.assert_called_once_with(self.live_data_url, auto_retry=True)
        handler.send_response.assert_called_with(200)
        handler.wfile.write.assert_called_once()

    @patch("ambientweather2sqlite.server.mureq.get")
    def test_send_live_data_network_error(self, mock_get):
        """Test _send_live_data with network error."""
        mock_get.side_effect = Exception("Network error")

        # Create handler instance without triggering HTTP processing
        handler = self.handler_class.__new__(self.handler_class)
        handler.LIVE_DATA_URL = self.live_data_url
        handler.DB_PATH = self.db_path
        handler.wfile = Mock()
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.log_message = Mock()
        handler._setup_logger()

        handler._send_live_data()

        # Verify error response was sent
        handler.send_response.assert_called_with(500)
        handler.log_message.assert_called()

    def test_send_daily_aggregated_data_success(self):
        """Test _send_daily_aggregated_data with valid parameters."""
        # Create handler instance without triggering HTTP processing
        handler = self.handler_class.__new__(self.handler_class)
        handler.LIVE_DATA_URL = self.live_data_url
        handler.DB_PATH = self.db_path
        handler.path = "/daily?q=avg_outTemp&days=7&tz=UTC"
        handler.wfile = Mock()
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.log_message = Mock()
        handler._setup_logger()

        handler._send_daily_aggregated_data()

        # Verify successful response
        handler.send_response.assert_called_with(200)
        handler.wfile.write.assert_called_once()

        # Check that response contains JSON data
        written_data = handler.wfile.write.call_args[0][0]
        response_data = json.loads(written_data.decode("utf-8"))
        self.assertIn("data", response_data)

    def test_send_daily_aggregated_data_invalid_days(self):
        """Test _send_daily_aggregated_data with invalid days parameter."""
        # Create handler instance without triggering HTTP processing
        handler = self.handler_class.__new__(self.handler_class)
        handler.LIVE_DATA_URL = self.live_data_url
        handler.DB_PATH = self.db_path
        handler.path = "/daily?q=avg_outTemp&days=invalid&tz=UTC"
        handler.wfile = Mock()
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.log_message = Mock()
        handler._setup_logger()

        handler._send_daily_aggregated_data()

        # Verify error response
        handler.send_response.assert_called_with(400)
        written_data = handler.wfile.write.call_args[0][0]
        response_data = json.loads(written_data.decode("utf-8"))
        self.assertIn("error", response_data)

    def test_send_daily_aggregated_data_missing_timezone(self):
        """Test _send_daily_aggregated_data with missing timezone."""
        # Create handler instance without triggering HTTP processing
        handler = self.handler_class.__new__(self.handler_class)
        handler.LIVE_DATA_URL = self.live_data_url
        handler.DB_PATH = self.db_path
        handler.path = "/daily?q=avg_outTemp&days=7"
        handler.wfile = Mock()
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.log_message = Mock()
        handler._setup_logger()

        handler._send_daily_aggregated_data()

        # Verify error response for missing timezone
        handler.send_response.assert_called_with(400)

    def test_send_hourly_aggregated_data_success(self):
        """Test _send_hourly_aggregated_data with valid parameters."""
        # Create handler instance without triggering HTTP processing
        handler = self.handler_class.__new__(self.handler_class)
        handler.LIVE_DATA_URL = self.live_data_url
        handler.DB_PATH = self.db_path
        handler.path = "/hourly?q=avg_outTemp&start_date=2025-06-27&tz=UTC"
        handler.wfile = Mock()
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.log_message = Mock()
        handler._setup_logger()

        handler._send_hourly_aggregated_data()

        # Verify successful response
        handler.send_response.assert_called_with(200)
        handler.wfile.write.assert_called_once()

        # Check that response contains JSON data
        written_data = handler.wfile.write.call_args[0][0]
        response_data = json.loads(written_data.decode("utf-8"))
        self.assertIn("data", response_data)

    def test_send_hourly_aggregated_data_missing_start_date(self):
        """Test _send_hourly_aggregated_data with missing start_date."""
        # Create handler instance without triggering HTTP processing
        handler = self.handler_class.__new__(self.handler_class)
        handler.LIVE_DATA_URL = self.live_data_url
        handler.DB_PATH = self.db_path
        handler.path = "/hourly?q=avg_outTemp&tz=UTC"
        handler.wfile = Mock()
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.log_message = Mock()
        handler._setup_logger()

        handler._send_hourly_aggregated_data()

        # Verify error response for missing start_date
        handler.send_response.assert_called_with(400)
        written_data = handler.wfile.write.call_args[0][0]
        response_data = json.loads(written_data.decode("utf-8"))
        self.assertIn("start_date is required", response_data["error"])

    def test_do_get_root_path(self):
        """Test do_GET method for root path."""
        # Create handler instance without triggering HTTP processing
        handler = self.handler_class.__new__(self.handler_class)
        handler.LIVE_DATA_URL = self.live_data_url
        handler.DB_PATH = self.db_path
        handler.path = "/"
        handler._send_live_data = Mock()
        handler._setup_logger()

        handler.do_GET()

        handler._send_live_data.assert_called_once()

    def test_do_get_daily_path(self):
        """Test do_GET method for daily path."""
        # Create handler instance without triggering HTTP processing
        handler = self.handler_class.__new__(self.handler_class)
        handler.LIVE_DATA_URL = self.live_data_url
        handler.DB_PATH = self.db_path
        handler.path = "/daily?tz=UTC"
        handler._send_daily_aggregated_data = Mock()
        handler._setup_logger()

        handler.do_GET()

        handler._send_daily_aggregated_data.assert_called_once()

    def test_do_get_hourly_path(self):
        """Test do_GET method for hourly path."""
        # Create handler instance without triggering HTTP processing
        handler = self.handler_class.__new__(self.handler_class)
        handler.LIVE_DATA_URL = self.live_data_url
        handler.DB_PATH = self.db_path
        handler.path = "/hourly?start_date=2025-06-27&tz=UTC"
        handler._send_hourly_aggregated_data = Mock()
        handler._setup_logger()

        handler.do_GET()

        handler._send_hourly_aggregated_data.assert_called_once()

    def test_do_get_unknown_path(self):
        """Test do_GET method for unknown path returns 404."""
        # Create handler instance without triggering HTTP processing
        handler = self.handler_class.__new__(self.handler_class)
        handler.LIVE_DATA_URL = self.live_data_url
        handler.DB_PATH = self.db_path
        handler.path = "/unknown"
        handler.wfile = Mock()
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler._setup_logger()

        handler.do_GET()

        # Verify 404 response
        handler.send_response.assert_called_with(404)
        written_data = handler.wfile.write.call_args[0][0]
        response_data = json.loads(written_data.decode("utf-8"))
        self.assertEqual(response_data["error"], "Not found")

    def test_send_json_with_broken_pipe(self):
        """Test _send_json handles BrokenPipeError gracefully."""
        # Create handler instance without triggering HTTP processing
        handler = self.handler_class.__new__(self.handler_class)
        handler.LIVE_DATA_URL = self.live_data_url
        handler.DB_PATH = self.db_path
        handler.wfile = Mock()
        handler.wfile.write = Mock(side_effect=BrokenPipeError())
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.log_message = Mock()
        handler._setup_logger()

        handler._send_json({"test": "data"})

        # Verify BrokenPipeError was logged
        handler.log_message.assert_called_with("%s", "BrokenPipeError")


class TestServer(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        Path(self.db_path).unlink(missing_ok=True)

        create_database_if_not_exists(self.db_path)
        self.live_data_url = "http://test.example.com/live"
        self.port = 8081  # Use different port to avoid conflicts
        self.host = "localhost"

    def tearDown(self):
        """Clean up after tests."""
        Path(self.db_path).unlink(missing_ok=True)

    def test_server_initialization(self):
        """Test Server class initialization."""
        server = Server(self.live_data_url, self.db_path, self.port, self.host)

        self.assertIsNotNone(server.httpd)
        self.assertIsNotNone(server.server_thread)
        self.assertTrue(server.server_thread.daemon)

    def test_server_start_stop(self):
        """Test Server start and shutdown methods."""
        server = Server(self.live_data_url, self.db_path, self.port, self.host)

        # Start server
        server.start()
        self.assertTrue(server.server_thread.is_alive())

        # Give server a moment to start
        time.sleep(0.1)

        # Shutdown server
        server.shutdown()
        time.sleep(0.1)  # Give shutdown time to complete
        self.assertFalse(server.server_thread.is_alive())

    @patch("ambientweather2sqlite.server.mureq.get")
    def test_server_integration_live_data(self, mock_get):
        """Integration test for server serving live data."""
        mock_get.return_value = """
        <td>Temperature (°F)</td><td>72.3</td>
        <td>Humidity (%)</td><td>65</td>
        """

        server = Server(self.live_data_url, self.db_path, self.port, self.host)
        server.start()

        try:
            # Give server time to start
            time.sleep(0.1)

            # Make HTTP request to server
            conn = HTTPConnection(self.host, self.port)
            conn.request("GET", "/")
            response = conn.getresponse()
            data = response.read().decode("utf-8")
            conn.close()

            # Verify response
            self.assertEqual(response.status, 200)
            self.assertEqual(response.getheader("Content-Type"), "application/json")

            response_data = json.loads(data)
            self.assertIn("data", response_data)
            self.assertIn("metadata", response_data)

        finally:
            server.shutdown()

    def test_server_integration_daily_data(self):
        """Integration test for server serving daily aggregated data."""
        # Insert test data
        test_data = [
            {
                "ts": "2025-06-27 12:00:00",
                "outTemp": 75.0,
                "outHumi": 60.0,
            },
        ]
        for data in test_data:
            insert_observation(self.db_path, data)

        server = Server(self.live_data_url, self.db_path, self.port, self.host)
        server.start()

        try:
            # Give server time to start
            time.sleep(0.1)

            # Make HTTP request to server
            conn = HTTPConnection(self.host, self.port)
            conn.request("GET", "/daily?q=avg_outTemp&days=7&tz=UTC")
            response = conn.getresponse()
            data = response.read().decode("utf-8")
            conn.close()

            # Verify response
            self.assertEqual(response.status, 200)
            response_data = json.loads(data)
            self.assertIn("data", response_data)

        finally:
            server.shutdown()

    def test_server_integration_hourly_data(self):
        """Integration test for server serving hourly aggregated data."""
        # Insert test data with current date
        from datetime import datetime

        current_date = datetime.now().strftime("%Y-%m-%d")
        test_data = [
            {
                "ts": f"{current_date} 12:00:00",
                "outTemp": 75.0,
                "outHumi": 60.0,
            },
        ]
        for data in test_data:
            insert_observation(self.db_path, data)

        server = Server(self.live_data_url, self.db_path, self.port, self.host)
        server.start()

        try:
            # Give server time to start
            time.sleep(0.1)

            # Make HTTP request to server using current date
            conn = HTTPConnection(self.host, self.port)
            conn.request(
                "GET", f"/hourly?q=avg_outTemp&start_date={current_date}&tz=UTC"
            )
            response = conn.getresponse()
            data = response.read().decode("utf-8")
            conn.close()

            # Verify response
            if response.status != 200:
                print(f"Response status: {response.status}")
                print(f"Response data: {data}")
            self.assertEqual(response.status, 200)
            response_data = json.loads(data)
            self.assertIn("data", response_data)

        finally:
            server.shutdown()


if __name__ == "__main__":
    unittest.main()
