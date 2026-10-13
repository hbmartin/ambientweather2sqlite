"""Regressions for recovery state, cancellation, and receiver concurrency."""

# Private state coordinates cancellation and verifies worker completion.
# ruff: noqa: SLF001

import json
import logging
import signal
import sqlite3
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch
from urllib.request import urlopen

from hypothesis import given
from hypothesis import strategies as st

from ambientweather2sqlite.alerts import AlertNotifier
from ambientweather2sqlite.daemon import _collect_forever, start_daemon
from ambientweather2sqlite.database import (
    create_database_if_not_exists,
    insert_observation,
)
from ambientweather2sqlite.database.metrics import _query_monitor_timestamp
from ambientweather2sqlite.http_validation import normalized_http_host
from ambientweather2sqlite.models import AlertConfig, RecoveryConfig, RediscoveryConfig
from ambientweather2sqlite.recovery import CollectionMonitor, monitor_collection
from ambientweather2sqlite.rediscovery import StationRediscovery
from ambientweather2sqlite.server import Server
from ambientweather2sqlite.station_http import (
    _RECEIVER_LOCKS,
    _receiver_lock,
    station_get,
)

_STATION_URL = "http://192.0.2.10/livedata.htm"
_PAGE = '<input name="outTemp" disabled value="72.5">'
_NOW = datetime(2026, 10, 12, 16, tzinfo=UTC)


class TestMonitorRobustness(TestCase):
    def setUp(self) -> None:
        directory = self.enterContext(tempfile.TemporaryDirectory())
        self.database = str(Path(directory) / "weather.db")
        create_database_if_not_exists(self.database)
        self.logger = Mock()
        self.events = Mock()
        self.monitor = CollectionMonitor(
            self.database,
            RecoveryConfig(),
            self.logger,
            self.events,
            started_at=_NOW,
        )

    def _insert_raw(self, raw: object) -> None:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("INSERT INTO observations(ts) VALUES (?)", (raw,))

    def _rows(self) -> list[tuple[object, ...]]:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            return connection.execute(
                "SELECT ts FROM observations ORDER BY ts"
            ).fetchall()

    def test_rejected_rows_cannot_hide_an_existing_stall_or_modify_rows(self) -> None:
        insert_observation(
            self.database, {"ts": (_NOW - timedelta(seconds=600)).isoformat()}
        )
        for raw in (
            123,
            "invalid",
            "9999-12-31T23:59:59-01:00",
            (_NOW + timedelta(days=1)).isoformat(),
        ):
            self._insert_raw(raw)
        before = self._rows()
        self.monitor.check(_NOW)
        self.monitor.check(_NOW + timedelta(seconds=60))
        self.assertTrue(self.monitor.stalled)
        self.assertEqual(self.events.call_count, 1)
        self.assertEqual(self.events.call_args.args[0], "collection_stalled")
        self.assertEqual(self.events.call_args.args[2], 600)
        self.assertEqual(self.logger.error.call_count, 2)
        self.assertEqual(self._rows(), before)

    def test_rejected_only_rows_use_startup_clock_and_never_prove_recovery(
        self,
    ) -> None:
        for raw in (
            123,
            "invalid",
            "9999-12-31T23:59:59-01:00",
            (_NOW + timedelta(days=1)).isoformat(),
        ):
            self._insert_raw(raw)
        self.monitor.check(_NOW + timedelta(seconds=299))
        self.assertFalse(self.monitor.stalled)
        self.monitor.check(_NOW + timedelta(seconds=300))
        self.monitor.check(_NOW + timedelta(seconds=360))
        self.assertTrue(self.monitor.stalled)
        self.assertEqual(self.monitor.fresh_observations, 0)
        self.events.assert_called_once_with("collection_stalled", None, 300.0)
        self.assertEqual(self.logger.error.call_count, 2)

    def test_valid_rows_establish_recovery_with_future_row_still_present(self) -> None:
        insert_observation(
            self.database, {"ts": (_NOW - timedelta(seconds=600)).isoformat()}
        )
        future = (_NOW + timedelta(days=1)).isoformat()
        self._insert_raw(future)
        self.monitor.check(_NOW)
        for count, sample in enumerate((60, 120, 180), start=1):
            current = _NOW + timedelta(seconds=sample)
            insert_observation(
                self.database, {"ts": current.isoformat(), "outTemp": 72}
            )
            self.monitor.check(current)
            self.monitor.check(current)
            self.assertEqual(
                self.monitor.stalled, count < self.monitor.config.recovery_observations
            )
        self.assertEqual(
            [call.args[0] for call in self.events.call_args_list],
            ["collection_stalled", "collection_resumed"],
        )
        self.assertIn((future,), self._rows())

    def test_timestamp_diagnostic_rearms_after_usable_maximum_restored(self) -> None:
        self._insert_raw("invalid")
        self.monitor.check(_NOW)
        self.monitor.check(_NOW)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("DELETE FROM observations")
        self._insert_raw(_NOW.isoformat())
        self.monitor.check(_NOW)
        self._insert_raw("invalid again")
        self.monitor.check(_NOW)
        self.assertEqual(self.logger.error.call_count, 2)

    def test_last_verified_timestamp_ages_when_no_usable_row_remains(self) -> None:
        self._insert_raw((_NOW + timedelta(seconds=60)).isoformat())
        self.monitor.check(_NOW + timedelta(seconds=60))
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("DELETE FROM observations")
        self._insert_raw("invalid")
        self.monitor.check(_NOW + timedelta(seconds=359))
        self.assertFalse(self.monitor.stalled)
        self.monitor.check(_NOW + timedelta(seconds=360))
        self.events.assert_called_once_with("collection_stalled", None, 300.0)

    def test_callback_failures_preserve_transitions_and_recovery(self) -> None:
        self.events.side_effect = TypeError("callback failure")
        insert_observation(
            self.database, {"ts": (_NOW - timedelta(seconds=600)).isoformat()}
        )
        self.monitor.check(_NOW)
        for sample in (60, 120, 180):
            current = _NOW + timedelta(seconds=sample)
            self._insert_raw(current.isoformat())
            self.monitor.check(current)
        self.assertFalse(self.monitor.stalled)
        self.assertEqual(self.events.call_count, 2)
        self.assertEqual(self.logger.exception.call_count, 2)

    def test_unexpected_check_failure_does_not_terminate_thread(self) -> None:
        recovered = threading.Event()
        self.monitor.fresh_observations = 2

        calls = 0

        def check() -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise TypeError("unexpected check failure")
            recovered.set()
            self.monitor._stop.set()

        with patch.object(CollectionMonitor, "check", side_effect=check):
            self.monitor.config = RecoveryConfig(check_interval_seconds=1)
            self.monitor.start()
            try:
                self.assertTrue(recovered.wait(timeout=3))
            finally:
                self.monitor.stop()
        self.assertEqual(self.monitor.fresh_observations, 0)
        self.logger.exception.assert_called_once()
        self.assertFalse(self.monitor._thread.is_alive())

    def test_stop_warns_on_blocked_query_and_suppresses_late_transition(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        def blocked_query(*_args: object) -> tuple[str, datetime, bool, datetime]:
            entered.set()
            if not release.wait(timeout=4):
                raise TimeoutError
            latest = _NOW - timedelta(seconds=600)
            return latest.isoformat(), latest, False, _NOW

        with patch(
            "ambientweather2sqlite.recovery._query_monitor_timestamp",
            side_effect=blocked_query,
        ):
            self.monitor.start()
            try:
                self.assertTrue(entered.wait(timeout=2))
                self.monitor.stop()
                self.assertTrue(self.monitor._thread.is_alive())
                self.logger.warning.assert_called_once()
            finally:
                release.set()
                self.monitor._thread.join(timeout=2)
        self.assertFalse(self.monitor._thread.is_alive())
        self.assertFalse(self.monitor.stalled)
        self.events.assert_not_called()

    def test_stop_prevents_callback_even_if_requested_during_transition_log(
        self,
    ) -> None:
        insert_observation(
            self.database, {"ts": (_NOW - timedelta(seconds=600)).isoformat()}
        )
        self.logger.error.side_effect = lambda *_args: self.monitor._stop.set()
        self.monitor.check(_NOW)
        self.events.assert_not_called()

    def test_callback_can_finish_after_bounded_stop(self) -> None:
        self._insert_raw("2000-01-01 00:00:00")
        entered = threading.Event()
        release = threading.Event()

        def callback(*_args: object) -> None:
            entered.set()
            if not release.wait(timeout=4):
                raise TimeoutError

        self.monitor.on_event = callback
        self.monitor.start()
        try:
            self.assertTrue(entered.wait(timeout=2))
            self.monitor.stop()
            self.logger.warning.assert_called_once()
        finally:
            release.set()
            self.monitor._thread.join(timeout=2)
        self.assertFalse(self.monitor._thread.is_alive())

    def test_interrupted_monitor_start_closes_context_without_joining_unstarted_thread(
        self,
    ) -> None:
        with (
            patch(
                "ambientweather2sqlite.recovery.threading.Thread.start",
                side_effect=KeyboardInterrupt,
            ),
            self.assertRaises(KeyboardInterrupt),
        ):
            with monitor_collection(self.database, RecoveryConfig(), self.logger):
                self.fail("An interrupted monitor must not enter the collector")

    def test_callback_can_request_monitor_stop_without_joining_itself(self) -> None:
        self._insert_raw("2000-01-01 00:00:00")
        self.monitor.on_event = lambda *_args: self.monitor.stop()
        self.monitor.start()
        self.monitor._thread.join(timeout=2)
        self.assertFalse(self.monitor._thread.is_alive())
        self.logger.exception.assert_not_called()

    def test_monitor_query_accepts_offset_and_naive_utc_timestamps(self) -> None:
        for raw in ("2026-10-12 16:00:00", "2026-10-12T17:00:00+01:00"):
            with self.subTest(raw=raw):
                with closing(sqlite3.connect(self.database)) as connection, connection:
                    connection.execute("DELETE FROM observations")
                self._insert_raw(raw)
                self.assertEqual(
                    _query_monitor_timestamp(self.database, _NOW),
                    (raw, _NOW, False, _NOW),
                )


class TestReachabilityTransitions(TestCase):
    def setUp(self) -> None:
        directory = self.enterContext(tempfile.TemporaryDirectory())
        self.database = str(Path(directory) / "weather.db")
        create_database_if_not_exists(self.database)
        self.logger = Mock()
        self.notifier = AlertNotifier(
            AlertConfig(webhook_url="http://hooks.invalid/"), _STATION_URL, self.logger
        )
        self.rediscovery = StationRediscovery(
            RediscoveryConfig(failure_threshold=3),
            _STATION_URL,
            self.logger,
            mac_address="aa:bb:cc:dd:ee:ff",
        )

    def test_unchanged_pages_break_failure_streaks_without_extra_rows(self) -> None:
        pages = [
            _PAGE,
            TimeoutError(),
            _PAGE,
            TimeoutError(),
            _PAGE,
            TimeoutError(),
            _PAGE,
        ]
        with (
            patch("ambientweather2sqlite.station_http.mureq.get", side_effect=pages),
            patch("ambientweather2sqlite.daemon._refresh_metadata"),
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=[None] * 6 + [KeyboardInterrupt],
            ),
            patch.object(AlertNotifier, "_post") as post,
            patch.object(StationRediscovery, "_rescan") as rescan,
            patch("builtins.print"),
            self.assertRaises(KeyboardInterrupt),
        ):
            _collect_forever(
                _STATION_URL,
                self.database,
                {"outTemp": "Temperature"},
                {},
                self.logger,
                None,
                60,
                self.notifier,
                self.rediscovery,
                interactive=False,
            )
        self.assertEqual(self.notifier.consecutive_failures, 0)
        self.assertEqual(self.rediscovery.consecutive_failures, 0)
        post.assert_not_called()
        rescan.assert_not_called()
        with closing(sqlite3.connect(self.database)) as connection, connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0], 1
            )

    def test_failed_insert_resets_fetches_but_not_online_or_collection_recovery(
        self,
    ) -> None:
        self.notifier.consecutive_failures = 3
        self.notifier.alert_sent = True
        self.rediscovery.consecutive_failures = 3
        monitor = CollectionMonitor(
            self.database, RecoveryConfig(), self.logger, Mock(), started_at=_NOW
        )
        with (
            patch("ambientweather2sqlite.station_http.mureq.get", return_value=_PAGE),
            patch(
                "ambientweather2sqlite.daemon.insert_observation",
                side_effect=sqlite3.OperationalError("locked"),
            ),
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=KeyboardInterrupt,
            ),
            patch("ambientweather2sqlite.daemon._publish_observation") as publish,
            patch.object(AlertNotifier, "_post") as post,
            patch("builtins.print"),
            self.assertRaises(KeyboardInterrupt),
        ):
            _collect_forever(
                _STATION_URL,
                self.database,
                {},
                {},
                self.logger,
                None,
                60,
                self.notifier,
                self.rediscovery,
                interactive=False,
            )
        self.assertEqual(self.notifier.consecutive_failures, 0)
        self.assertTrue(self.notifier.alert_sent)
        self.assertEqual(self.rediscovery.consecutive_failures, 0)
        post.assert_not_called()
        publish.assert_not_called()
        monitor.check(_NOW + timedelta(seconds=300))
        self.assertTrue(monitor.stalled)
        self.assertEqual(monitor.fresh_observations, 0)

    def test_online_waits_for_stored_row_after_unchanged_page(self) -> None:
        def fail(*_args: object, **_kwargs: object) -> None:
            self.notifier.consecutive_failures = 3
            self.notifier.alert_sent = True

        with (
            patch(
                "ambientweather2sqlite.station_http.mureq.get",
                side_effect=[_PAGE, _PAGE, _PAGE.replace("72.5", "73.5")],
            ),
            patch("ambientweather2sqlite.daemon._refresh_metadata"),
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
            ) as wait,
            patch.object(AlertNotifier, "_post") as post,
            patch("builtins.print"),
        ):
            cycles = 0

            def after_cycle(*_args: object, **_kwargs: object) -> None:
                nonlocal cycles
                cycles += 1
                match cycles:
                    case 1:
                        fail()
                    case 2:
                        post.assert_not_called()
                        self.assertTrue(self.notifier.alert_sent)
                        self.assertEqual(self.notifier.consecutive_failures, 0)
                    case _:
                        raise KeyboardInterrupt

            wait.side_effect = after_cycle
            with self.assertRaises(KeyboardInterrupt):
                _collect_forever(
                    _STATION_URL,
                    self.database,
                    {"outTemp": "Temperature"},
                    {},
                    self.logger,
                    None,
                    60,
                    self.notifier,
                    self.rediscovery,
                    interactive=False,
                )
        post.assert_called_once_with("station_online", 3)
        self.assertFalse(self.notifier.alert_sent)

    def test_failed_store_does_not_clear_previous_terminal_frame_twice(self) -> None:
        with (
            patch("ambientweather2sqlite.station_http.mureq.get", return_value=_PAGE),
            patch(
                "ambientweather2sqlite.daemon._store_observation",
                side_effect=[True, False, False],
            ),
            patch(
                "ambientweather2sqlite.daemon._is_stale_observation", return_value=False
            ),
            patch("ambientweather2sqlite.daemon._emit_observation", return_value=8),
            patch("ambientweather2sqlite.daemon._publish_observation"),
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=[None, None, KeyboardInterrupt],
            ),
            patch("ambientweather2sqlite.daemon.clear_lines") as clear,
            self.assertRaises(KeyboardInterrupt),
        ):
            _collect_forever(_STATION_URL, self.database, {}, {}, self.logger, None, 60)
        self.assertEqual([call.args[0] for call in clear.call_args_list], [0, 8, 0])

    @given(st.lists(st.booleans(), min_size=1, max_size=30))
    def test_fetch_failure_count_tracks_only_consecutive_failures(
        self, outcomes: list[bool]
    ) -> None:
        notifier = AlertNotifier(
            AlertConfig(webhook_url="http://hooks.invalid/"), _STATION_URL, Mock()
        )
        streak = 0
        with patch.object(AlertNotifier, "_post"):
            for success in outcomes:
                pending = notifier.alert_sent
                if success:
                    notifier.record_fetch_success()
                    streak = 0
                    self.assertEqual(notifier.alert_sent, pending)
                else:
                    notifier.record_failure()
                    streak += 1
                self.assertEqual(notifier.consecutive_failures, streak)


class TestDaemonLifecycle(TestCase):
    def test_interrupts_and_errors_cleanup_and_restore_signals(self) -> None:
        previous = signal.getsignal(signal.SIGTERM)
        for phase in ("metadata", "server", "rediscovery", "collection"):
            for use_sigterm in (False, True):
                with self.subTest(phase=phase, sigterm=use_sigterm):
                    server = Mock()
                    monitor = Mock()

                    def interrupt(
                        *_args: object,
                        use_sigterm: bool = use_sigterm,
                        **_kwargs: object,
                    ) -> None:
                        if use_sigterm:
                            handler = signal.getsignal(signal.SIGTERM)
                            self.assertTrue(callable(handler))
                            handler(signal.SIGTERM, None)
                        raise KeyboardInterrupt

                    targets = {
                        "metadata": "ambientweather2sqlite.daemon.create_metadata",
                        "server": "ambientweather2sqlite.daemon.Server",
                        "rediscovery": "ambientweather2sqlite.daemon.StationRediscovery",
                        "collection": "ambientweather2sqlite.daemon._collect_forever",
                    }
                    with (
                        patch(
                            "ambientweather2sqlite.daemon._configure_logging",
                            return_value=Mock(),
                        ),
                        patch(
                            "ambientweather2sqlite.daemon.create_metadata",
                            return_value=({}, {}),
                        ),
                        patch(
                            "ambientweather2sqlite.daemon.Server", return_value=server
                        ),
                        patch(
                            "ambientweather2sqlite.recovery.CollectionMonitor",
                            return_value=monitor,
                        ),
                        patch("ambientweather2sqlite.daemon.StationRediscovery"),
                        patch("ambientweather2sqlite.daemon._collect_forever"),
                        patch("builtins.print"),
                    ):
                        if phase == "server":
                            server.start.side_effect = interrupt
                            context = nullcontext()
                        else:
                            context = patch(targets[phase], side_effect=interrupt)
                        with context, self.assertRaises(SystemExit) as caught:
                            start_daemon(
                                _STATION_URL,
                                "/tmp/weather.db",
                                port=0,
                                recovery_config=RecoveryConfig(),
                            )
                    self.assertEqual(caught.exception.code, 0)
                    monitor.start.assert_called_once()
                    monitor.stop.assert_called_once()
                    self.assertEqual(server.shutdown.call_count, phase != "metadata")
                    self.assertIs(signal.getsignal(signal.SIGTERM), previous)

    def test_unexpected_collection_error_propagates_after_cleanup(self) -> None:
        server = Mock()
        monitor = Mock()
        previous = signal.getsignal(signal.SIGTERM)
        with (
            patch(
                "ambientweather2sqlite.daemon._configure_logging", return_value=Mock()
            ),
            patch(
                "ambientweather2sqlite.daemon.create_metadata", return_value=({}, {})
            ),
            patch("ambientweather2sqlite.daemon.Server", return_value=server),
            patch(
                "ambientweather2sqlite.recovery.CollectionMonitor", return_value=monitor
            ),
            patch("ambientweather2sqlite.daemon.StationRediscovery"),
            patch(
                "ambientweather2sqlite.daemon._collect_forever",
                side_effect=RuntimeError("collection failed"),
            ),
            patch("builtins.print"),
            self.assertRaisesRegex(RuntimeError, "collection failed"),
        ):
            start_daemon(
                _STATION_URL,
                "/tmp/weather.db",
                port=0,
                recovery_config=RecoveryConfig(),
            )
        server.shutdown.assert_called_once()
        monitor.stop.assert_called_once()
        self.assertIs(signal.getsignal(signal.SIGTERM), previous)


class TestReceiverConcurrency(TestCase):
    def test_receiver_address_normalizes_hosts_without_dns_lookup(self) -> None:
        with patch("socket.getaddrinfo", side_effect=OSError("DNS unavailable")) as dns:
            first = _receiver_lock("http://Observer.LOCAL/livedata.htm")
            self.assertIs(first, _receiver_lock("http://observer.local/station.htm"))
            self.assertIsNot(first, _receiver_lock(_STATION_URL))
            dns.assert_not_called()
        self.assertEqual(normalized_http_host("http://[2001:0db8::1]/"), "2001:db8::1")

    def test_unused_receiver_locks_are_released(self) -> None:
        # Earlier request exceptions can retain their host's lock in tracebacks.
        host = "unused-receiver.example"
        receiver = _receiver_lock(f"http://{host}/")
        self.assertIn(host, _RECEIVER_LOCKS)
        del receiver
        self.assertNotIn(host, _RECEIVER_LOCKS)

    def test_unrelated_receiver_can_fetch_while_station_lock_is_held(self) -> None:
        entered = threading.Event()

        def get(*_args: object, **_kwargs: object) -> str:
            entered.set()
            return _PAGE

        with (
            patch("ambientweather2sqlite.station_http.mureq.get", side_effect=get),
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            with _receiver_lock(_STATION_URL):
                request = executor.submit(
                    station_get, "http://192.0.2.11/livedata.htm", timeout=3
                )
                self.assertTrue(entered.wait(timeout=2))
                self.assertEqual(request.result(timeout=2), _PAGE)

    def test_station_retry_keeps_receiver_lock_until_request_completes(self) -> None:
        entered = threading.Event()
        second_started = threading.Event()
        retry_entered = threading.Event()
        release = threading.Event()
        calls = 0

        def request(*_args: object, **_kwargs: object) -> Mock:
            nonlocal calls
            calls += 1
            match calls:
                case 1:
                    raise TimeoutError
                case 2:
                    retry_entered.set()
                    if not release.wait(timeout=3):
                        raise TimeoutError
                case _:
                    entered.set()
            return Mock(body=b"page")

        def second() -> str:
            second_started.set()
            return station_get(_STATION_URL)

        with (
            patch(
                "ambientweather2sqlite.station_http.mureq.request", side_effect=request
            ),
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            first = executor.submit(station_get, _STATION_URL, auto_retry=True)
            try:
                self.assertTrue(retry_entered.wait(timeout=2))
                later = executor.submit(second)
                self.assertTrue(second_started.wait(timeout=2))
                self.assertFalse(entered.wait(timeout=0.05))
            finally:
                release.set()
            self.assertEqual(first.result(timeout=2), "page")
            self.assertEqual(later.result(timeout=2), "page")
        self.assertEqual(calls, 3)

    def test_health_remains_responsive_while_live_request_waits(self) -> None:
        directory = self.enterContext(tempfile.TemporaryDirectory())
        database = str(Path(directory) / "weather.db")
        create_database_if_not_exists(database)
        insert_observation(database, {"outTemp": 72.5})
        server = Server(_STATION_URL, database, 0, "127.0.0.1", "secret")
        self.addCleanup(server.shutdown)
        server.start()
        base = f"http://127.0.0.1:{server.httpd.server_address[1]}"
        entered = threading.Event()

        def live_get(*args: object, **kwargs: object) -> str:
            entered.set()
            return station_get(*args, **kwargs)

        def fetch(path: str) -> tuple[int, dict[str, object]]:
            from urllib.request import Request

            with urlopen(
                Request(base + path, headers={"Authorization": "Bearer secret"}),
                timeout=2,
            ) as response:
                return response.status, json.load(response)

        with (
            patch("ambientweather2sqlite.server.station_get", side_effect=live_get),
            patch("ambientweather2sqlite.station_http.mureq.get", return_value=_PAGE),
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            for trial in range(3):
                with self.subTest(trial=trial):
                    entered.clear()
                    with _receiver_lock(_STATION_URL):
                        live = executor.submit(fetch, "/")
                        self.assertTrue(entered.wait(timeout=2))
                        status, payload = fetch("/health")
                        self.assertEqual((status, payload["status"]), (200, "ok"))
                        self.assertEqual(fetch("/metrics")[0], 200)
                    self.assertEqual(live.result(timeout=2)[0], 200)
        server.shutdown()
        server.shutdown()
        self.assertFalse(server.server_thread.is_alive())

    def test_server_can_shutdown_before_start_and_release_logging(self) -> None:
        directory = self.enterContext(tempfile.TemporaryDirectory())
        server = Server(
            _STATION_URL, str(Path(directory) / "weather.db"), 0, "127.0.0.1"
        )
        server.shutdown()
        server.shutdown()
        self.assertEqual(server._handler_class.log_handler_count(), 0)
        self.assertEqual(server.httpd.fileno(), -1)

    def test_interrupted_server_construction_releases_its_logger(self) -> None:
        directory = self.enterContext(tempfile.TemporaryDirectory())
        database = str(Path(directory) / "weather.db")
        with (
            patch(
                "ambientweather2sqlite.server.ThreadingHTTPServer",
                side_effect=KeyboardInterrupt,
            ),
            self.assertRaises(KeyboardInterrupt),
        ):
            Server(_STATION_URL, database, 0, "127.0.0.1")
        logger = logging.getLogger(
            f"ambientweather2sqlite.server.JSONHandler.{Path(directory) / 'weather_server.log'}"
        )
        self.assertEqual(logger.handlers, [])
