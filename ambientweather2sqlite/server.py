import hmac
import json
import logging
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, cast, override
from urllib.parse import parse_qs, unquote, urlparse

from ambientweather2sqlite.exceptions import Aw2SqliteError, MissingTimezoneError

from . import mureq
from .awparser import extract_labels, extract_values
from .database import (
    query_daily_aggregated_data,
    query_db_metrics,
    query_hourly_aggregated_data,
    query_latest_timestamp,
    query_range_aggregated_data,
)
from .database.metrics import query_latest_observation
from .models import (
    HealthPayload,
    JsonResponse,
    QueryParams,
    build_daily_aggregated_payload,
    build_error_payload,
    build_hourly_aggregated_payload,
    build_live_data_payload,
    build_range_aggregated_payload,
)
from .prometheus import build_prometheus_metrics

_PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def _tz_from_query(query: QueryParams) -> str:
    if tz_query := query.get("tz", []):
        return unquote(tz_query[0])
    raise MissingTimezoneError


def create_request_handler(  # noqa: C901
    live_data_url: str,
    db_path: str,
    auth_token: str | None = None,
) -> type[BaseHTTPRequestHandler]:
    log_path = Path(db_path).parent / f"{Path(db_path).stem}_server.log"

    class JSONHandler(BaseHTTPRequestHandler):
        LIVE_DATA_URL = live_data_url
        DB_PATH = db_path
        LOG_PATH = log_path
        AUTH_TOKEN = auth_token
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
                "%s - - [%s] %s",
                self.address_string(),
                self.log_date_time_string(),
                message,
            )

        def _set_headers(
            self,
            status: int = 200,
            content_type: str = "application/json",
        ) -> None:
            """Set common headers for responses."""
            self.send_response(status)
            self.send_header("Content-type", content_type)
            self.send_header("Access-Control-Allow-Origin", "*")  # Enable CORS
            self.end_headers()

        def _send_json(self, data: object, status: int = 200) -> None:
            """Send data as an indented JSON response."""
            try:
                self._set_headers(status)
                json_string = json.dumps(data, indent=2)
                self.wfile.write(json_string.encode("utf-8"))
            except ConnectionError as e:
                self.log_message("%s", type(e).__name__)

        def _is_authorized(self) -> bool:
            if self.AUTH_TOKEN is None:
                return True
            authorization = self.headers.get("Authorization", "")
            return hmac.compare_digest(
                authorization,
                f"Bearer {self.AUTH_TOKEN}",
            )

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

        def _send_query_response(self, build: Callable[[], JsonResponse]) -> None:
            """Send the built payload, mapping validation errors to 400."""
            try:
                self._send_json(build())
            except Aw2SqliteError as e:
                self.log_message("%s\n%s", type(e).__name__, str(e))
                self._send_json(build_error_payload(str(e)), 400)
            except Exception as e:  # noqa: BLE001
                self.log_message("%s\n%s", type(e).__name__, str(e))
                self._send_json(build_error_payload(str(e)), 500)

        def _send_daily_aggregated_data(self) -> None:
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

            self._send_query_response(
                lambda: build_daily_aggregated_payload(
                    query_daily_aggregated_data(
                        db_path=self.DB_PATH,
                        aggregation_fields=aggregation_fields,
                        prior_days=prior_days,
                        tz=_tz_from_query(query),
                    ),
                ),
            )

        def _send_hourly_aggregated_data(self) -> None:
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

            self._send_query_response(
                lambda: build_hourly_aggregated_payload(
                    query_hourly_aggregated_data(
                        db_path=self.DB_PATH,
                        aggregation_fields=aggregation_fields,
                        start_date=start_date[0],
                        end_date=end_date[0] if end_date else None,
                        tz=_tz_from_query(query),
                    ),
                ),
            )

        def _send_range_aggregated_data(self) -> None:
            query: QueryParams = parse_qs(urlparse(self.path).query)
            aggregation_fields = query.get("q", [])
            start = query.get("start", [])
            end = query.get("end", [])

            if not start:
                self._send_json(
                    build_error_payload(
                        "start required e.g. /range?start=2026-06-22&tz=UTC&q=avg_outTemp",  # noqa: E501
                    ),
                    400,
                )
                return

            self._send_query_response(
                lambda: build_range_aggregated_payload(
                    query_range_aggregated_data(
                        db_path=self.DB_PATH,
                        aggregation_fields=aggregation_fields,
                        start=unquote(start[0]),
                        end=unquote(end[0]) if end else None,
                        tz=_tz_from_query(query),
                    ),
                ),
            )

        def _build_health_payload(self) -> HealthPayload:
            latest_ts = query_latest_timestamp(self.DB_PATH)
            metrics = query_db_metrics(self.DB_PATH)
            return {
                "status": "ok",
                "last_observation_ts": latest_ts,
                "row_count": metrics["row_count"],
            }

        def _send_health(self) -> None:
            self._send_query_response(self._build_health_payload)

        def _wants_prometheus(self, query: QueryParams) -> bool:
            if fmt := query.get("format", []):
                return fmt[0] == "prometheus"
            accept = self.headers.get("Accept", "")
            return "openmetrics" in accept or "text/plain" in accept

        def _send_prometheus_metrics(self) -> None:
            # Build the full payload before sending headers so a failure can
            # still produce a clean 500; once headers are committed, only
            # connection errors are possible and no second response is sent.
            try:
                metrics = query_db_metrics(self.DB_PATH)
                latest_observation = query_latest_observation(self.DB_PATH)
                payload = build_prometheus_metrics(metrics, latest_observation)
            except Exception as e:  # noqa: BLE001
                self.log_message("%s\n%s", type(e).__name__, str(e))
                self._send_json(build_error_payload(str(e)), 500)
                return
            try:
                self._set_headers(content_type=_PROMETHEUS_CONTENT_TYPE)
                self.wfile.write(payload.encode("utf-8"))
            except ConnectionError as e:
                self.log_message("%s", type(e).__name__)

        def _send_metrics(self) -> None:
            query: QueryParams = parse_qs(urlparse(self.path).query)
            if self._wants_prometheus(query):
                self._send_prometheus_metrics()
                return
            self._send_query_response(lambda: query_db_metrics(self.DB_PATH))

        def do_OPTIONS(self) -> None:
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Authorization")
            self.end_headers()

        def do_GET(self) -> None:
            if not self._is_authorized():
                self._send_json(build_error_payload("Unauthorized"), 401)
                return
            match self.path.split("?")[0]:
                case "/":
                    self._send_live_data()
                case "/daily":
                    self._send_daily_aggregated_data()
                case "/hourly":
                    self._send_hourly_aggregated_data()
                case "/range":
                    self._send_range_aggregated_data()
                case "/health":
                    self._send_health()
                case "/metrics":
                    self._send_metrics()
                case _:
                    self._send_json(build_error_payload("Not found"), 404)

    JSONHandler.setup_logger()
    return JSONHandler


class Server:
    def __init__(
        self,
        live_data_url: str,
        db_path: str,
        port: int,
        host: str,
        auth_token: str | None = None,
    ):
        handler_class = create_request_handler(live_data_url, db_path, auth_token)
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
        """Serve requests on a daemon background thread."""
        self.server_thread.start()

    def shutdown(self) -> None:
        """Stop serving, close the socket, and release the log handler."""
        self.httpd.shutdown()
        self.httpd.server_close()
        self.server_thread.join()
        self._teardown_logger()
