import json
import logging
import math
import signal
import sqlite3
import sys
import threading
import time
from datetime import datetime
from http.client import HTTPException
from logging.handlers import RotatingFileHandler
from pathlib import Path
from types import FrameType

from ambientweather2sqlite import mureq
from ambientweather2sqlite.awparser import extract_values
from ambientweather2sqlite.server import Server

from .alerts import AlertNotifier
from .database import insert_observation
from .ha_discovery import build_discovery_messages
from .metadata import create_metadata
from .models import AlertConfig, ColumnUnitMap, LabelMap, MqttConfig
from .mqtt import MqttError, publish, publish_message

# Cap the rotating daemon log so it cannot grow without bound on a
# long-running deployment: up to 4 files of ~5 MB each (~20 MB total).
_LOG_MAX_BYTES = 5 * 1024 * 1024
_LOG_BACKUP_COUNT = 3


class _JsonFormatter(logging.Formatter):
    """Format log records as single-line JSON (JSONL)."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, object] = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def _configure_logging(
    log_path: Path,
    *,
    log_format: str = "text",
) -> logging.Logger:
    """Set up rotating file logging with the requested format.

    A size-capped rotating handler keeps the daemon log bounded no matter how
    long the collector runs.
    """
    logger = logging.getLogger("ambientweather2sqlite")
    logger.setLevel(logging.INFO)

    handler = RotatingFileHandler(
        str(log_path),
        maxBytes=_LOG_MAX_BYTES,
        backupCount=_LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    if log_format == "json":
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            ),
        )
    logger.addHandler(handler)
    return logger


def clear_lines(n: int) -> None:
    for _ in range(n):
        print("\033[A\033[K", end="")


def wait_for_next_update(deadline: float, *, show_countdown: bool = True) -> None:
    """Sleep until the monotonic deadline.

    On an interactive terminal a live countdown is redrawn on a single line
    with a carriage return. When stdout is captured to a file or the journal,
    those escapes accumulate as noise instead of overwriting the line, so
    non-interactive callers disable the countdown and simply sleep.
    """
    if not show_countdown:
        if (remaining := deadline - time.monotonic()) > 0:
            time.sleep(remaining)
        return
    while (remaining := deadline - time.monotonic()) > 0:
        print(f"\033[KNext update in {math.ceil(remaining)} seconds", end="\r")
        time.sleep(min(1.0, remaining))


def next_deadline(epoch: float, period_seconds: int, now: float) -> float:
    """Return the next fetch deadline aligned to epoch + n * period.

    Aligning deadlines to a fixed epoch keeps samples evenly spaced no matter
    how long each fetch takes, and skips any slots that were missed entirely.
    """
    intervals = math.floor((now - epoch) / period_seconds) + 1
    return epoch + intervals * period_seconds


def _install_sigterm_handler() -> None:
    """Translate SIGTERM into KeyboardInterrupt for a graceful shutdown.

    Signal handlers can only be installed in the main thread; when the daemon
    runs elsewhere (e.g. under test), termination handling is left unchanged.
    """
    if threading.current_thread() is not threading.main_thread():
        return

    def _raise_keyboard_interrupt(_signum: int, _frame: FrameType | None) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)


def fetch_once(live_data_url: str) -> None:
    """Fetch a single observation and print it without writing to the DB."""
    try:
        body = mureq.get(live_data_url)
        live_data = extract_values(body)
    except TimeoutError:
        print("Error: Weather station timed out.", file=sys.stderr)
        sys.exit(1)
    except HTTPException as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    if not live_data:
        print(
            "Warning: parsed 0 sensors - the station page format may have changed",
            file=sys.stderr,
        )
    print(json.dumps(live_data, indent=4))


def _print_observation(labels: dict[str, str], live_data: dict) -> int:
    """Print the labeled observation and return the line count to clear next."""
    print(f"Updated at: {datetime.now()}")
    labeled_data = {labels.get(key, key): value for key, value in live_data.items()}
    pretty_data = json.dumps(labeled_data, indent=4)
    print(pretty_data)
    # + 2 for the "Updated at" line and the newline generated by print()
    return pretty_data.count("\n") + 2


def _status(message: str, *, interactive: bool) -> int:
    """Print a one-line status update, returning lines for the next redraw.

    Interactive callers clear the single status line before the next frame;
    captured streams never rewrite lines, so nothing needs clearing.
    """
    print(message)
    return 1 if interactive else 0


def _emit_observation(
    labels: dict[str, str],
    live_data: dict,
    *,
    interactive: bool,
) -> int:
    """Show an observation, returning the line count for the next redraw.

    Interactive terminals get the full labeled JSON redrawn in place each
    cycle. Captured streams get one concise line so the log stays bounded.
    """
    if interactive:
        return _print_observation(labels, live_data)
    print(f"Recorded {len(live_data)} sensors at {datetime.now()}")
    return 0


def _publish_observation(
    logger: logging.Logger,
    mqtt_config: MqttConfig | None,
    live_data: dict,
) -> None:
    if mqtt_config is None:
        return
    try:
        publish(mqtt_config, json.dumps(live_data))
    except (MqttError, OSError) as e:
        logger.warning("MQTT publish failed: %s", e)


def _publish_discovery(
    logger: logging.Logger,
    mqtt_config: MqttConfig | None,
    labels: LabelMap,
    column_to_unit: ColumnUnitMap,
) -> None:
    """Publish retained Home Assistant discovery configs, once at startup."""
    if mqtt_config is None or not mqtt_config.discovery:
        return
    if not labels:
        logger.warning("Skipping MQTT discovery: no sensor metadata available")
        return
    try:
        for topic, payload in build_discovery_messages(
            labels,
            column_to_unit,
            mqtt_config,
        ):
            publish_message(mqtt_config, topic, payload, retain=True)
    except (MqttError, OSError) as e:
        logger.warning("MQTT discovery publish failed: %s", e)


def _record_failure_and_wait(
    notifier: AlertNotifier | None,
    epoch: float,
    period_seconds: int,
    *,
    interactive: bool = True,
) -> None:
    """Count a failed collection attempt and sleep until the next slot."""
    if notifier is not None:
        notifier.record_failure()
    wait_for_next_update(
        next_deadline(epoch, period_seconds, time.monotonic()),
        show_countdown=interactive,
    )


def _collect_forever(  # noqa: PLR0913
    live_data_url: str,
    database_path: str,
    labels: LabelMap,
    logger: logging.Logger,
    mqtt_config: MqttConfig | None,
    period_seconds: int,
    notifier: AlertNotifier | None = None,
    *,
    interactive: bool = True,
) -> None:
    """Fetch, print, store, and publish observations until interrupted."""
    remove_newlines = 0
    epoch = time.monotonic()
    while True:
        if interactive:
            clear_lines(remove_newlines)
        try:
            body = mureq.get(live_data_url)
            live_data = extract_values(body)
        except TimeoutError:
            logger.info("TimeoutError")
            remove_newlines = _status(
                "Warming up weather station's server...",
                interactive=interactive,
            )
            if notifier is not None:
                notifier.record_failure()
            continue
        except HTTPException as e:
            logger.info("%s\n%s", type(e).__name__, e)
            remove_newlines = _status(
                f"Error fetching live data: {e}",
                interactive=interactive,
            )
            _record_failure_and_wait(
                notifier,
                epoch,
                period_seconds,
                interactive=interactive,
            )
            continue
        if not live_data:
            # An unparseable page means no data is flowing, so it counts
            # toward the offline alert just like a failed fetch.
            logger.warning(
                "Parsed 0 sensors from %s - "
                "the station page format may have changed",
                live_data_url,
            )
            remove_newlines = _status(
                "Warning: parsed 0 sensors from station page",
                interactive=interactive,
            )
            _record_failure_and_wait(
                notifier,
                epoch,
                period_seconds,
                interactive=interactive,
            )
            continue
        if notifier is not None:
            notifier.record_success()
        remove_newlines = _emit_observation(
            labels,
            live_data,
            interactive=interactive,
        )
        try:
            insert_observation(database_path, live_data)
        except sqlite3.Error as e:
            logger.warning("Failed to insert observation: %s", e)
        _publish_observation(logger, mqtt_config, live_data)
        wait_for_next_update(
            next_deadline(epoch, period_seconds, time.monotonic()),
            show_countdown=interactive,
        )


def start_daemon(  # noqa: PLR0913
    live_data_url: str,
    database_path: str,
    *,
    port: int | None = None,
    period_seconds: int = 60,
    log_format: str = "text",
    host: str = "localhost",
    auth_token: str | None = None,
    mqtt_config: MqttConfig | None = None,
    alert_config: AlertConfig | None = None,
    interactive: bool | None = None,
) -> None:
    # A non-tty stdout (Docker, systemd, launchd, a redirect) can't overwrite
    # lines, so the live countdown/observation redraw would otherwise pile up
    # forever. Fall back to concise, append-only output there.
    if interactive is None:
        interactive = sys.stdout.isatty()
    print(f"Observing {live_data_url}")
    print("Press Ctrl+C to stop")

    log_path = Path(database_path).parent / f"{Path(database_path).stem}_daemon.log"
    logger = _configure_logging(log_path, log_format=log_format)

    labels: dict[str, str] = {}
    column_to_unit: ColumnUnitMap = {}
    try:
        labels, column_to_unit = create_metadata(database_path, live_data_url)
    except (HTTPException, TimeoutError) as e:
        logger.info("%s\n%s", type(e).__name__, e)
        print(f"Error fetching metadata: {e}")

    _publish_discovery(logger, mqtt_config, labels, column_to_unit)

    server = None

    if port is not None:
        print(f"Starting JSON server on http://{host}:{port}")
        server = Server(live_data_url, database_path, port, host, auth_token)
        server.start()

    notifier = (
        AlertNotifier(alert_config, live_data_url, logger)
        if alert_config is not None
        else None
    )

    _install_sigterm_handler()
    try:
        _collect_forever(
            live_data_url,
            database_path,
            labels,
            logger,
            mqtt_config,
            period_seconds,
            notifier,
            interactive=interactive,
        )
    except KeyboardInterrupt:
        print(f"\nStopping... results saved to {database_path}")
        if server is not None:
            server.shutdown()
        sys.exit(0)
