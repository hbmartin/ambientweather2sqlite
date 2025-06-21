import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from . import mureq
from .awparser import extract_labels, extract_values


def create_request_handler(live_data_url: str) -> type[BaseHTTPRequestHandler]:
    class JSONHandler(BaseHTTPRequestHandler):
        LIVE_DATA_URL = live_data_url

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            # Override to disable all logging
            pass

        def _set_headers(self, status: int = 200) -> None:
            """Set common headers for JSON responses."""
            self.send_response(status)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")  # Enable CORS
            self.end_headers()

        def _send_json(self, data: dict, status: int = 200) -> None:
            """Helper method to send JSON response."""
            self._set_headers(status)
            json_string = json.dumps(data, indent=2)
            self.wfile.write(json_string.encode("utf-8"))

        def do_GET(self):
            try:
                body = mureq.get(self.LIVE_DATA_URL, auto_retry=True)
            except Exception as e:  # noqa: BLE001
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

    return JSONHandler


class Server:
    def __init__(self, live_data_url: str, port: int, host: str):
        self.httpd = HTTPServer((host, port), create_request_handler(live_data_url))
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
