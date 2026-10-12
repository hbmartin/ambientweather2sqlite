"""Regressions for host identity, pending outages, and snapshot clocks."""

# Internal state verifies that transitions preserve and then clear outage history.
# ruff: noqa: SLF001

import signal
import socket
import sqlite3
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from http.client import HTTPException
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from hypothesis import given
from hypothesis import strategies as st

from ambientweather2sqlite.alerts import AlertNotifier
from ambientweather2sqlite.daemon import (
    _collect_forever,
    _fetch_receiver_page,
    _sigterm_handler,
    start_daemon,
)
from ambientweather2sqlite.database import (
    create_database_if_not_exists,
    insert_observation,
    metrics,
    query_db_metrics,
)
from ambientweather2sqlite.models import AlertConfig, Observation, RecoveryConfig
from ambientweather2sqlite.recovery import CollectionMonitor
from ambientweather2sqlite.station_http import _receiver_lock, station_get

_PAGE = '<input name="outTemp" disabled value="72.5">'
_NOW = datetime(2026, 10, 12, 16, tzinfo=UTC)
_ALERTS = AlertConfig("http://hooks.invalid/", failure_threshold=3)


class TestHostLocks(TestCase):
    @given(
        host=st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=1, max_size=30),
        port=st.integers(min_value=1, max_value=65535),
    )
    def test_host_key_ignores_case_scheme_port_and_path(
        self, host: str, port: int
    ) -> None:
        first = _receiver_lock(f"http://{host.upper()}.example:{port}/livedata.htm")
        self.assertIs(first, _receiver_lock(f"https://{host}.example/station.htm"))

    def test_idna_and_ipv6_keys_are_canonical_without_resolving_aliases(self) -> None:
        with patch("socket.getaddrinfo", side_effect=socket.gaierror) as dns:
            first = _receiver_lock("http://BÜCHER.example/livedata.htm")
            self.assertIs(first, _receiver_lock("https://xn--bcher-kva.example/"))
            ipv6 = _receiver_lock("http://[2001:0db8:0000:0000:0000:0000:0000:0001]/")
            self.assertIs(ipv6, _receiver_lock("http://[2001:db8::1]:8080/station.htm"))
            self.assertIsNot(first, _receiver_lock("http://192.0.2.10/"))
            dns.assert_not_called()

    def test_host_lock_holds_through_retry_without_dns_selection(self) -> None:
        first_entered = threading.Event()
        second_started = threading.Event()
        allow_retry = threading.Event()
        retry_entered = threading.Event()
        release = threading.Event()
        second_entered = threading.Event()
        attempts = 0
        retry_attempt = 2
        first_url = "http://Observer.LOCAL/livedata.htm"
        second_url = "https://observer.local:8443/station.htm"

        def request(*_args: object, **_kwargs: object) -> Mock:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                first_entered.set()
                if not allow_retry.wait(timeout=2):
                    raise AssertionError("Retry was never released")
                raise TimeoutError
            if attempts == retry_attempt:
                retry_entered.set()
                if not release.wait(timeout=2):
                    raise AssertionError("Receiver was never released")
            else:
                second_entered.set()
            return Mock(body=_PAGE.encode())

        def second_request() -> str:
            second_started.set()
            return station_get(second_url)

        with (
            patch("socket.getaddrinfo", side_effect=socket.gaierror) as dns,
            patch("ambientweather2sqlite.mureq.request", side_effect=request) as get,
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            first = executor.submit(station_get, first_url, auto_retry=True)
            try:
                self.assertTrue(first_entered.wait(timeout=2))
                second = executor.submit(second_request)
                self.assertTrue(second_started.wait(timeout=2))
                allow_retry.set()
                self.assertTrue(retry_entered.wait(timeout=2))
                self.assertFalse(second_entered.wait(timeout=0.05))
            finally:
                allow_retry.set()
                release.set()
            self.assertEqual(first.result(timeout=2), _PAGE)
            self.assertEqual(second.result(timeout=2), _PAGE)
            self.assertEqual(
                [call.kwargs["url"] for call in get.call_args_list],
                [first_url, first_url, second_url],
            )
            dns.assert_not_called()

    def test_valid_request_arguments_and_transport_errors_are_preserved(self) -> None:
        url = "https://Observer.LOCAL:8443/livedata.htm?raw=1"
        with patch("ambientweather2sqlite.mureq.get", return_value=_PAGE) as get:
            self.assertEqual(station_get(url), _PAGE)
            get.assert_called_once_with(url, auto_retry=False)
            get.reset_mock()
            self.assertEqual(station_get(url, timeout=1.5, auto_retry=True), _PAGE)
            get.assert_called_once_with(url, timeout=1.5, auto_retry=True)
        for error in (
            TimeoutError("timeout"),
            OSError("network"),
            HTTPException("http failure"),
            ValueError("bad URL"),
        ):
            with self.subTest(error=type(error).__name__):
                with patch("ambientweather2sqlite.mureq.get", side_effect=error):
                    expected = (
                        HTTPException if isinstance(error, ValueError) else type(error)
                    )
                    with self.assertRaises(expected) as raised:
                        station_get(url)
                if isinstance(error, ValueError):
                    self.assertIs(raised.exception.__cause__, error)
                    self.assertTrue(
                        str(raised.exception).startswith("Receiver request failed:")
                    )
                else:
                    self.assertIs(raised.exception, error)

    def test_invalid_hostnames_and_url_parts_are_fetch_errors(self) -> None:
        for url in (
            "http://" + "a" * 64 + ".invalid/",
            "http://" + chr(0xD800) + ".invalid/",
            "http:///livedata.htm",
            "http://[::1/livedata.htm",
            "http://station.local:invalid/",
            "http://station.local:65536/",
        ):
            with self.subTest(url=repr(url)):
                logger = Mock()
                with patch("ambientweather2sqlite.mureq.get") as get:
                    with self.assertRaisesRegex(HTTPException, "Invalid receiver URL"):
                        station_get(url)
                    with patch("builtins.print"):
                        self.assertEqual(
                            _fetch_receiver_page(url, logger, interactive=False),
                            ("", {}, 0),
                        )
                    get.assert_not_called()
                self.assertEqual(logger.info.call_args.args[1], "HTTPException")

    def test_malformed_url_does_not_terminate_collection_retries(self) -> None:
        notifier = AlertNotifier(_ALERTS, "http:///livedata.htm", Mock())
        with (
            patch("builtins.print"),
            patch("ambientweather2sqlite.daemon.insert_observation") as insert,
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=[None, KeyboardInterrupt],
            ) as wait,
            self.assertRaises(KeyboardInterrupt),
        ):
            _collect_forever(
                notifier.station_url,
                "unused.db",
                {},
                {},
                notifier.logger,
                None,
                60,
                notifier,
                interactive=False,
            )
        self.assertEqual(wait.call_count, 2)
        self.assertEqual(notifier.consecutive_failures, 2)
        insert.assert_not_called()


class TestPendingOutageCounts(TestCase):
    def test_online_payload_keeps_peak_across_fetches_and_later_failures(self) -> None:
        notifier = AlertNotifier(_ALERTS, "http://station.local/", Mock())
        with patch.object(AlertNotifier, "_send") as send:
            for failures in (4, 2, 6, 1):
                for _ in range(failures):
                    notifier.record_failure()
                notifier.record_fetch_success()
                self.assertEqual(notifier.consecutive_failures, 0)
                self.assertTrue(notifier.alert_sent)
            notifier.record_success()
            self.assertEqual(
                [
                    (call.args[0]["event"], call.args[0]["consecutive_failures"])
                    for call in send.call_args_list
                ],
                [("station_offline", 3), ("station_online", 6)],
            )
            self.assertEqual(notifier._pending_failure_peak, 0)
            self.assertFalse(notifier.alert_sent)
            for _ in range(3):
                notifier.record_failure()
            notifier.record_success()
            self.assertEqual(send.call_args.args[0]["consecutive_failures"], 3)

    @given(
        actions=st.lists(
            st.sampled_from(("failure", "fetch", "commit")), min_size=1, max_size=60
        ),
        initial_failures=st.integers(min_value=0, max_value=20),
        initial_pending=st.booleans(),
    )
    def test_pending_peak_and_current_counter_follow_independent_invariants(
        self, *, actions: list[str], initial_failures: int, initial_pending: bool
    ) -> None:
        notifier = AlertNotifier(
            _ALERTS,
            "http://station.local/",
            Mock(),
            consecutive_failures=initial_failures,
            alert_sent=initial_pending,
        )
        failures = initial_failures
        peak = 0
        pending = initial_pending
        expected: list[tuple[str, int]] = []
        with patch.object(AlertNotifier, "_send") as send:
            for action in actions:
                match action:
                    case "failure":
                        failures += 1
                        if not pending and failures >= _ALERTS.failure_threshold:
                            pending = True
                            expected.append(("station_offline", failures))
                        if pending:
                            peak = max(peak, failures)
                        notifier.record_failure()
                    case "fetch":
                        if pending:
                            peak = max(peak, failures)
                        failures = 0
                        notifier.record_fetch_success()
                    case "commit":
                        if pending:
                            peak = max(peak, failures)
                            expected.append(("station_online", peak))
                        failures = peak = 0
                        pending = False
                        notifier.record_success()
                self.assertEqual(notifier.consecutive_failures, failures)
                self.assertEqual(notifier._pending_failure_peak, peak)
                self.assertEqual(notifier.alert_sent, pending)
            self.assertEqual(
                [
                    (call.args[0]["event"], call.args[0]["consecutive_failures"])
                    for call in send.call_args_list
                ],
                expected,
            )

    def test_stale_pages_and_failed_insert_delay_online_without_losing_count(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "weather.db")
            create_database_if_not_exists(database)
            logger = Mock()
            notifier = AlertNotifier(_ALERTS, "http://127.0.0.1/", logger)
            changed = _PAGE.replace("72.5", "73.5")
            pages = [_PAGE, *[TimeoutError()] * 4, _PAGE, changed, changed]
            inserts = 0
            failed_insert = 2

            def store(database_path: str, observation: Observation) -> list[str]:
                nonlocal inserts
                inserts += 1
                if inserts == failed_insert:
                    self.assertEqual(notifier.consecutive_failures, 0)
                    self.assertTrue(notifier.alert_sent)
                    raise sqlite3.OperationalError("locked")
                return insert_observation(database_path, observation)

            with (
                patch("builtins.print"),
                patch("ambientweather2sqlite.mureq.get", side_effect=pages),
                patch(
                    "ambientweather2sqlite.daemon.insert_observation", side_effect=store
                ),
                patch("ambientweather2sqlite.daemon._refresh_metadata"),
                patch("ambientweather2sqlite.mureq.post") as post,
                patch(
                    "ambientweather2sqlite.daemon.wait_for_next_update",
                    side_effect=[None] * 7 + [KeyboardInterrupt],
                ),
                self.assertRaises(KeyboardInterrupt),
            ):
                _collect_forever(
                    notifier.station_url,
                    database,
                    {"outTemp": "Temperature"},
                    {},
                    logger,
                    None,
                    60,
                    notifier,
                    interactive=False,
                )
            self.assertEqual(inserts, 3)
            self.assertEqual(query_db_metrics(database)["row_count"], 2)
            self.assertEqual(
                [
                    (
                        call.kwargs["json"]["event"],
                        call.kwargs["json"]["consecutive_failures"],
                    )
                    for call in post.call_args_list
                ],
                [("station_offline", 3), ("station_online", 4)],
            )


class TestSnapshotClock(TestCase):
    def test_commit_before_select_is_not_rejected_by_an_earlier_entry_clock(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "weather.db")
            create_database_if_not_exists(database)
            logger = Mock()
            monitor = CollectionMonitor(
                database, RecoveryConfig(), logger, started_at=_NOW
            )
            latest = _NOW + timedelta(microseconds=1)
            checked_at = _NOW + timedelta(microseconds=2)
            phases: list[str] = []
            original_connect = metrics._connect_database

            def connect(db_path: str, *, read_only: bool = False) -> sqlite3.Connection:
                phases.append("connect")
                insert_observation(
                    database, {"ts": latest.isoformat(), "outTemp": 72.5}
                )
                connection = original_connect(db_path, read_only=read_only)
                connection.set_trace_callback(lambda _query: phases.append("select"))
                return connection

            def snapshot_time(_tz: object) -> datetime:
                phases.append("clock")
                return checked_at

            with (
                patch(
                    "ambientweather2sqlite.database.metrics._connect_database",
                    side_effect=connect,
                ),
                patch(
                    "ambientweather2sqlite.database.metrics.datetime", wraps=datetime
                ) as clock,
            ):
                clock.now.side_effect = snapshot_time
                monitor.check()
            self.assertEqual(phases, ["connect", "select", "clock"])
            self.assertEqual(monitor._last_verified_timestamp, latest)
            self.assertFalse(monitor._timestamp_error)
            self.assertFalse(monitor.stalled)
            logger.error.assert_not_called()
            self.assertEqual(query_db_metrics(database)["row_count"], 1)

    def test_explicit_clock_remains_fixed_and_rejects_genuine_future_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "weather.db")
            create_database_if_not_exists(database)
            insert_observation(
                database, {"ts": (_NOW + timedelta(microseconds=1)).isoformat()}
            )
            monitor = CollectionMonitor(
                database, RecoveryConfig(), Mock(), started_at=_NOW
            )
            with patch(
                "ambientweather2sqlite.database.metrics.datetime", wraps=datetime
            ) as clock:
                monitor.check(_NOW)
                clock.now.assert_not_called()
            self.assertTrue(monitor._timestamp_error)
            self.assertIsNone(monitor._last_verified_timestamp)
            self.assertEqual(query_db_metrics(database)["row_count"], 1)


class TestSignalContext(TestCase):
    def test_restorable_handlers_are_restored_after_body_errors(self) -> None:
        def previous(_signum: int, _frame: object) -> None:
            pass

        for handler in (signal.SIG_DFL, signal.SIG_IGN, previous):
            for error in (RuntimeError("startup failed"), KeyboardInterrupt()):
                with self.subTest(handler=handler, error=type(error).__name__):
                    with (
                        patch(
                            "ambientweather2sqlite.daemon.signal.getsignal",
                            return_value=handler,
                        ),
                        patch("ambientweather2sqlite.daemon.signal.signal") as install,
                        self.assertRaises(type(error)),
                        _sigterm_handler(),
                    ):
                        raise error
                    self.assertEqual(install.call_count, 2)
                    self.assertTrue(callable(install.call_args_list[0].args[1]))
                    self.assertEqual(
                        install.call_args_list[1].args, (signal.SIGTERM, handler)
                    )

    def test_opaque_native_handler_is_untouched_during_daemon_cleanup(self) -> None:
        server = Mock()
        logger = Mock()
        with (
            patch("builtins.print"),
            patch("ambientweather2sqlite.daemon.signal.getsignal", return_value=None),
            patch("ambientweather2sqlite.daemon.signal.signal") as install,
            patch(
                "ambientweather2sqlite.daemon._configure_logging", return_value=logger
            ),
            patch(
                "ambientweather2sqlite.daemon.create_metadata", return_value=({}, {})
            ),
            patch("ambientweather2sqlite.daemon.Server", return_value=server),
            patch(
                "ambientweather2sqlite.daemon._collect_forever",
                side_effect=KeyboardInterrupt,
            ),
            self.assertRaises(SystemExit) as exit_code,
        ):
            start_daemon("http://127.0.0.1/", "unused.db", port=0, interactive=False)
        self.assertEqual(exit_code.exception.code, 0)
        install.assert_not_called()
        server.shutdown.assert_called_once()
        logger.warning.assert_called_once()
        self.assertIn(
            "SIGTERM translator not installed", logger.warning.call_args.args[0]
        )

    def test_opaque_native_handler_without_logger_warns_and_preserves_body_error(
        self,
    ) -> None:
        with (
            patch("ambientweather2sqlite.daemon.signal.getsignal", return_value=None),
            patch("ambientweather2sqlite.daemon.signal.signal") as install,
            self.assertLogs("ambientweather2sqlite.daemon", level="WARNING") as logs,
            self.assertRaisesRegex(RuntimeError, "collection failed"),
            _sigterm_handler(),
        ):
            raise RuntimeError("collection failed")
        install.assert_not_called()
        self.assertEqual(len(logs.output), 1)
        self.assertIn("opaque native handler", logs.output[0])

    def test_worker_thread_never_reads_or_changes_signal_handlers(self) -> None:
        def run() -> None:
            with _sigterm_handler():
                pass

        with (
            patch("ambientweather2sqlite.daemon.signal.getsignal") as read,
            patch("ambientweather2sqlite.daemon.signal.signal") as install,
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            executor.submit(run).result(timeout=2)
        read.assert_not_called()
        install.assert_not_called()
