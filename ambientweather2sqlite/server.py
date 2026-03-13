import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, cast, override
from urllib.parse import parse_qs, unquote, urlparse

from ambientweather2sqlite.exceptions import Aw2SqliteError, InvalidTimezoneError

from . import mureq
from .awparser import extract_labels, extract_values
from .database import (
    query_daily_aggregated_data,
    query_db_metrics,
    query_hourly_aggregated_data,
    query_latest_timestamp,
)
from .models import (
    QueryParams,
    build_daily_aggregated_payload,
    build_error_payload,
    build_hourly_aggregated_payload,
    build_live_data_payload,
)


def _tz_from_query(query: QueryParams) -> str:
    if tz_query := query.get("tz", []):
        return unquote(tz_query[0])
    raise InvalidTimezoneError("tz is required")


def create_request_handler(  # noqa: C901
    live_data_url: str,
    db_path: str,
) -> type[BaseHTTPRequestHandler]:
    log_path = Path(db_path).parent / f"{Path(db_path).stem}_server.log"

    class JSONHandler(BaseHTTPRequestHandler):
        LIVE_DATA_URL = live_data_url
        DB_PATH = db_path
        LOG_PATH = log_path
        _logger: logging.Logger = logging.getLogger(
            f"{__name__}.JSONHandler.{LOG_PATH}",
        )

        @classmethod
        def setup_logger(cls) -> None:
            if cls._logger.handlers:
                return
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

        @classmethod
        def teardown_logger(cls) -> None:
            for handler in list(cls._logger.handlers):
                cls._logger.removeHandler(handler)
                handler.close()

        @classmethod
        def log_handler_count(cls) -> int:
            return len(cls._logger.handlers)

        @override
        def log_message(self, format: str, *args: object) -> None:
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

        def _send_json(self, data: object, status: int = 200) -> None:
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
                self._send_json(build_error_payload(str(e)), 500)
                return
            values = extract_values(body)
            labels = extract_labels(body)
            self._send_json(build_live_data_payload(values, labels))

        def _send_daily_aggregated_data(self) -> None:
            try:
                query: QueryParams = parse_qs(urlparse(self.path).query)
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
                            build_error_payload(
                                f"days must be int, got {prior_days_query[0]}",
                            ),
                            400,
                        )
                        return

                data = query_daily_aggregated_data(
                    db_path=self.DB_PATH,
                    aggregation_fields=aggregation_fields,
                    prior_days=prior_days,
                    tz=_tz_from_query(query),
                )
                self._send_json(build_daily_aggregated_payload(data))
            except Aw2SqliteError as e:
                self.log_message("%s\n%s", type(e).__name__, str(e))
                self._send_json(build_error_payload(str(e)), 400)
            except Exception as e:  # noqa: BLE001
                self.log_message("%s\n%s", type(e).__name__, str(e))
                self._send_json(build_error_payload(str(e)), 500)

        def _send_hourly_aggregated_data(self) -> None:
            try:
                query: QueryParams = parse_qs(urlparse(self.path).query)
                aggregation_fields = query.get("q", [])
                start_date = query.get("start_date", []) or query.get("date", [])
                end_date = query.get("end_date", [])

                if not start_date:
                    self._send_json(
                        build_error_payload(
                            "start_date or date required e.g. /hourly?start_date=2025-06-22&tz=UTC",  # noqa: E501
                        ),
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
                self._send_json(build_hourly_aggregated_payload(data))
            except Aw2SqliteError as e:
                self.log_message("%s\n%s", type(e).__name__, str(e))
                self._send_json(build_error_payload(str(e)), 400)
            except Exception as e:  # noqa: BLE001
                self.log_message("%s\n%s", type(e).__name__, str(e))
                self._send_json(build_error_payload(str(e)), 500)

        def _send_health(self) -> None:
            try:
                latest_ts = query_latest_timestamp(self.DB_PATH)
                metrics = query_db_metrics(self.DB_PATH)
                self._send_json(
                    {
                        "status": "ok",
                        "last_observation_ts": latest_ts,
                        "row_count": metrics["row_count"],
                    },
                )
            except Exception as e:  # noqa: BLE001
                self.log_message("%s\n%s", type(e).__name__, str(e))
                self._send_json(build_error_payload(str(e)), 500)

        def _send_metrics(self) -> None:
            try:
                self._send_json(query_db_metrics(self.DB_PATH))
            except Exception as e:  # noqa: BLE001
                self.log_message("%s\n%s", type(e).__name__, str(e))
                self._send_json(build_error_payload(str(e)), 500)

        def do_GET(self) -> None:
            match self.path.split("?")[0]:
                case "/":
                    self._send_live_data()
                case "/daily":
                    self._send_daily_aggregated_data()
                case "/hourly":
                    self._send_hourly_aggregated_data()
                case "/health":
                    self._send_health()
                case "/metrics":
                    self._send_metrics()
                case _:
                    self._send_json(build_error_payload("Not found"), 404)

    JSONHandler.setup_logger()
    return JSONHandler


class Server:
    def __init__(self, live_data_url: str, db_path: str, port: int, host: str):
        handler_class = create_request_handler(live_data_url, db_path)
        self._teardown_logger = cast("Any", handler_class).teardown_logger
        self.httpd = HTTPServer(
            (host, port),
            handler_class,
        )
        self.server_thread = threading.Thread(
            target=self.httpd.serve_forever,
            daemon=True,
        )

    def start(self) -> None:
        self.server_thread.start()

    def shutdown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.server_thread.join()
        self._teardown_logger()
