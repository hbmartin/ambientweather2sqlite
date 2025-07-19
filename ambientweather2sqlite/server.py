import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from ambientweather2sqlite.exceptions import Aw2SqliteError, InvalidTimezoneError

from . import mureq
from .awparser import extract_labels, extract_values
from .database import query_daily_aggregated_data, query_hourly_aggregated_data


def _tz_from_query(query: dict) -> str:
    if tz_query := query.get("tz", []):
        return unquote(tz_query[0])
    raise InvalidTimezoneError("tz is required")


def create_request_handler(  # noqa: C901
    live_data_url: str,
    db_path: str,
) -> type[BaseHTTPRequestHandler]:
    class JSONHandler(BaseHTTPRequestHandler):
        LIVE_DATA_URL = live_data_url
        DB_PATH = db_path
        LOG_PATH = Path(db_path).parent / f"{Path(db_path).stem}_server.log"
        _logger: logging.Logger = logging.getLogger(f"{__name__}.JSONHandler")

        @classmethod
        def _setup_logger(cls) -> None:
            # Prevent propagation to root logger to avoid console output
            cls._logger.propagate = False
            handler = logging.FileHandler(cls.LOG_PATH)
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                ),
            )
            cls._logger.addHandler(handler)
            cls._logger.setLevel(logging.INFO)

        def __init__(self, *args, **kwargs):
            self._setup_logger()
            super().__init__(*args, **kwargs)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            message = format % args
            self._logger.info(
                f"{self.address_string()} - - "
                f"[{self.log_date_time_string()}] "
                f"{message}",
            )

        def _set_headers(self, status: int = 200) -> None:
            """Set common headers for JSON responses."""
            self.send_response(status)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")  # Enable CORS
            self.end_headers()

        def _send_json(self, data: dict, status: int = 200) -> None:
            """Helper method to send JSON response."""
            try:
                self._set_headers(status)
                json_string = json.dumps(data, indent=2)
                self.wfile.write(json_string.encode("utf-8"))
            except BrokenPipeError:
                self.log_message("%s", "BrokenPipeError")

        def _send_live_data(self) -> None:
            try:
                body = mureq.get(self.LIVE_DATA_URL, auto_retry=True)
            except Exception as e:  # noqa: BLE001
                self.log_message("%s\n%s", type(e).__name__, str(e))
                self._send_json({"error": str(e)}, 500)
                return
            values = extract_values(body)
            labels = extract_labels(body)

            response_data = {
                "data": values,
                "metadata": {
                    "labels": labels,
                },
            }
            self._send_json(response_data)

        def _send_daily_aggregated_data(self) -> None:
            try:
                query = parse_qs(urlparse(self.path).query)
                aggregation_fields = query.get("q", [])

                prior_days = 7
                prior_days_query = query.get("days", [])
                if len(prior_days_query) != 0:
                    try:
                        prior_days = int(prior_days_query[0])
                    except (ValueError, TypeError) as e:
                        self.log_message(
                            "%s\n%s",
                            type(e).__name__,
                            f"days must be int, got {prior_days_query[0]}",
                        )
                        self._send_json(
                            {
                                "error": f"days must be int, got {prior_days_query[0]}",
                            },
                            400,
                        )
                        return

                data = query_daily_aggregated_data(
                    db_path=self.DB_PATH,
                    aggregation_fields=aggregation_fields,
                    prior_days=prior_days,
                    tz=_tz_from_query(query),
                )
                self._send_json({"data": data})
            except Aw2SqliteError as e:
                self.log_message("%s\n%s", type(e).__name__, str(e))
                self._send_json({"error": str(e)}, 400)
            except Exception as e:  # noqa: BLE001
                self.log_message("%s\n%s", type(e).__name__, str(e))
                self._send_json({"error": str(e)}, 500)

        def _send_hourly_aggregated_data(self) -> None:
            try:
                query = parse_qs(urlparse(self.path).query)
                aggregation_fields = query.get("q", [])
                start_date = query.get("start_date", [])
                end_date = query.get("end_date", [])

                if not start_date:
                    self._send_json(
                        {
                            "error": "start_date required e.g. /hourly?start_date=2025-06-22",  # noqa: E501
                        },
                        400,
                    )
                    return

                data = query_hourly_aggregated_data(
                    db_path=self.DB_PATH,
                    aggregation_fields=aggregation_fields,
                    start_date=start_date[0],
                    end_date=end_date[0] if end_date else None,
                    tz=_tz_from_query(query),
                )
                self._send_json({"data": data})
            except Aw2SqliteError as e:
                self.log_message("%s\n%s", type(e).__name__, str(e))
                self._send_json({"error": str(e)}, 400)
            except Exception as e:  # noqa: BLE001
                self.log_message("%s\n%s", type(e).__name__, str(e))
                self._send_json({"error": str(e)}, 500)

        def do_GET(self):
            # Only serve data for the root path
            if self.path == "/":
                self._send_live_data()
            elif self.path.startswith("/daily"):
                self._send_daily_aggregated_data()
            elif self.path.startswith("/hourly"):
                self._send_hourly_aggregated_data()
            else:
                self._send_json({"error": "Not found"}, 404)
                return

    return JSONHandler


class Server:
    def __init__(self, live_data_url: str, db_path: str, port: int, host: str):
        self.httpd = HTTPServer(
            (host, port),
            create_request_handler(live_data_url, db_path),
        )
        self.server_thread = threading.Thread(
            target=self.httpd.serve_forever,
            daemon=True,
        )

    def start(self):
        self.server_thread.start()

    def shutdown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.server_thread.join()
