"""Exercise freshness detection against real committed observations."""

import sqlite3
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

from ambientweather2sqlite.daemon import (
    _collect_forever,
    _handle_failed_cycle,
    fetch_once,
)
from ambientweather2sqlite.database import (
    create_database_if_not_exists,
    insert_observation,
    query_db_metrics,
)
from ambientweather2sqlite.models import RecoveryConfig
from ambientweather2sqlite.recovery import CollectionMonitor, monitor_collection
from ambientweather2sqlite.scanner import probe_weather_station
from ambientweather2sqlite.station_http import STATION_REQUEST_LOCK


class TestCollectionMonitor(TestCase):
    def setUp(self) -> None:
        self.directory = self.enterContext(tempfile.TemporaryDirectory())
        self.database_path = str(Path(self.directory) / "weather.db")
        create_database_if_not_exists(self.database_path)
        self.now = datetime(2026, 10, 12, 16, tzinfo=UTC)
        self.logger = Mock()
        self.events = Mock()
        self.monitor = CollectionMonitor(
            self.database_path,
            RecoveryConfig(),
            self.logger,
            self.events,
            started_at=self.now,
        )

    def _insert(self, seconds: int) -> None:
        insert_observation(
            self.database_path,
            {
                "ts": (self.now + timedelta(seconds=seconds)).isoformat(),
                "outTemp": 72.5,
            },
        )

    def _check(self, seconds: int) -> None:
        self.monitor.check(self.now + timedelta(seconds=seconds))

    def test_empty_database_has_startup_grace_and_one_error_per_outage(self) -> None:
        self._check(299)
        self.logger.error.assert_not_called()
        self._check(300)
        self._check(600)
        self.logger.error.assert_called_once()
        self.events.assert_called_once_with("collection_stalled", None, 300.0)
        self.assertTrue(self.monitor.stalled)
        self.assertEqual(query_db_metrics(self.database_path)["row_count"], 0)

    def test_old_database_is_reported_immediately_without_receiver_requests(
        self,
    ) -> None:
        self._insert(-600)
        with patch("ambientweather2sqlite.mureq.get") as request:
            self._check(0)
            self._check(60)
        request.assert_not_called()
        self.logger.error.assert_called_once()
        self.assertEqual(self.events.call_args.args[0], "collection_stalled")

    def test_recovery_requires_three_advancing_committed_observations(self) -> None:
        self._insert(-600)
        self._check(0)
        for seconds in (60, 120):
            self._insert(seconds)
            self._check(seconds)
            self._check(seconds + 1)
            self.assertTrue(self.monitor.stalled)
        # An old imported row and repeated checks cannot count as recovery.
        self._insert(-300)
        self._check(130)
        self.assertEqual(self.monitor.fresh_observations, 2)
        self._insert(180)
        self._check(180)
        self.assertFalse(self.monitor.stalled)
        self.assertEqual(
            [call.args[0] for call in self.events.call_args_list],
            [
                "collection_stalled",
                "collection_resumed",
            ],
        )
        self._check(480)
        self.assertTrue(self.monitor.stalled)
        self.assertEqual(self.logger.error.call_count, 2)

    def test_another_stale_interval_resets_partial_recovery(self) -> None:
        self._insert(-600)
        self._check(0)
        self._insert(60)
        self._check(60)
        self.assertEqual(self.monitor.fresh_observations, 1)
        self._check(360)
        self.assertEqual(self.monitor.fresh_observations, 0)
        self.logger.error.assert_called_once()

    def test_timestamp_regression_cannot_recount_previously_seen_rows(self) -> None:
        self._check(300)
        for seconds in (360, 420, 360, 420):
            timestamp = (self.now + timedelta(seconds=seconds)).isoformat()
            with patch(
                "ambientweather2sqlite.recovery.query_latest_timestamp",
                return_value=timestamp,
            ):
                self._check(420)
        self.assertTrue(self.monitor.stalled)
        self.assertEqual(self.monitor.fresh_observations, 0)
        self.assertEqual(self.events.call_count, 1)

    def test_database_failures_do_not_clear_outage_or_partial_recovery(self) -> None:
        self._insert(-600)
        self._check(0)
        self._insert(60)
        self._check(60)
        with patch(
            "ambientweather2sqlite.recovery.query_latest_timestamp",
            side_effect=sqlite3.OperationalError("database is locked"),
        ):
            self._check(120)
            self._check(180)
        self.assertTrue(self.monitor.stalled)
        self.assertEqual(self.monitor.fresh_observations, 0)
        self.assertEqual(self.logger.error.call_count, 2)
        self._check(180)
        self.logger.info.assert_called_once_with(
            "Collection freshness database checks restored",
        )
        self.assertEqual(self.events.call_count, 1)

    def test_invalid_and_future_timestamps_cannot_prove_recovery(self) -> None:
        self._check(300)
        for raw in ("invalid", (self.now + timedelta(days=1)).isoformat()):
            with (
                self.subTest(raw=raw),
                patch(
                    "ambientweather2sqlite.recovery.query_latest_timestamp",
                    return_value=raw,
                ),
            ):
                self._check(300)
                self.assertTrue(self.monitor.stalled)
                self.assertEqual(self.monitor.fresh_observations, 0)
        self.assertEqual(self.events.call_count, 1)

    @given(required=st.integers(min_value=1, max_value=8))
    @settings(max_examples=8)
    def test_recovery_threshold_counts_distinct_timestamps(self, required: int) -> None:
        monitor = CollectionMonitor(
            self.database_path,
            RecoveryConfig(recovery_observations=required),
            Mock(),
            started_at=self.now,
        )
        monitor.check(self.now + timedelta(seconds=300))
        # Hypothesis reuses this TestCase database, so mock timestamps for
        # independent threshold trials rather than importing synthetic rows.
        for sample in range(1, required + 1):
            now = self.now + timedelta(seconds=300 + sample)
            with patch(
                "ambientweather2sqlite.recovery.query_latest_timestamp",
                return_value=now.isoformat(),
            ):
                monitor.check(now)
                monitor.check(now)
            self.assertEqual(monitor.stalled, sample < required)

    def test_monitor_reports_outage_while_receiver_request_lock_is_held(self) -> None:
        self._insert(-600)
        reported = threading.Event()
        logger = Mock()
        logger.error.side_effect = lambda *_args: reported.set()
        with (
            STATION_REQUEST_LOCK,
            monitor_collection(
                self.database_path,
                RecoveryConfig(),
                logger,
            ),
        ):
            self.assertTrue(reported.wait(timeout=2))
        self.assertFalse(
            any(
                thread.name == "aw2sqlite-collection-monitor" and thread.is_alive()
                for thread in threading.enumerate()
            )
        )

    def test_disabled_monitor_never_reads_database_or_starts_thread(self) -> None:
        for config in (None, RecoveryConfig(enabled=False)):
            with patch("ambientweather2sqlite.recovery.CollectionMonitor") as monitor:
                with monitor_collection(self.database_path, config, self.logger):
                    pass
                monitor.assert_not_called()

    def test_monitor_stops_when_collection_raises(self) -> None:
        with patch("ambientweather2sqlite.recovery.CollectionMonitor") as monitor:
            with (
                self.assertRaises(RuntimeError),
                monitor_collection(
                    self.database_path,
                    RecoveryConfig(),
                    self.logger,
                ),
            ):
                raise RuntimeError("collector exited")
        monitor.return_value.start.assert_called_once()
        monitor.return_value.stop.assert_called_once()

    def test_real_parse_insert_flow_cannot_recover_from_repeated_pages(self) -> None:
        self._insert(-600)
        self._check(0)
        cycle = 0

        def timestamp() -> str:
            return (self.now + timedelta(seconds=(cycle + 1) * 60)).isoformat()

        def wait(_deadline: float, *, show_countdown: bool) -> None:
            nonlocal cycle
            self.assertFalse(show_countdown)
            cycle += 1
            self._check(cycle * 60)
            if cycle == len(pages):
                raise KeyboardInterrupt

        pages = [
            '<html><body><input name="CurrTime" disabled value="'
            + minute
            + ' 10/12/2026"><input name="outTemp" disabled '
            'value="72.5"></body></html>'
            for minute in ("09:01", "09:01", "09:02", "09:03")
        ]
        with (
            patch("ambientweather2sqlite.daemon.mureq.get", side_effect=pages),
            patch("ambientweather2sqlite.daemon._refresh_metadata"),
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update", side_effect=wait
            ),
            patch(
                "ambientweather2sqlite.database.writes._current_observation_timestamp",
                side_effect=timestamp,
            ),
            patch("builtins.print"),
            self.assertRaises(KeyboardInterrupt),
        ):
            _collect_forever(
                "http://127.0.0.1/livedata.htm",
                self.database_path,
                {"outTemp": "Outdoor Temperature"},
                {},
                self.logger,
                None,
                60,
                interactive=False,
            )
        self.assertEqual(query_db_metrics(self.database_path)["row_count"], 4)
        self.assertFalse(self.monitor.stalled)
        self.assertEqual(self.events.call_count, 2)

    def test_failures_wait_full_interval_even_near_next_scheduled_slot(self) -> None:
        with (
            patch("ambientweather2sqlite.daemon.time.monotonic", return_value=59),
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
            ) as wait,
        ):
            _handle_failed_cycle(
                None, None, None, "http://station/", 0, 60, interactive=False
            )
        wait.assert_called_once_with(119, show_countdown=False)


class TestSerializedStationRequests(TestCase):
    def test_collection_and_discovery_never_overlap_receiver_requests(self) -> None:
        first_entered = threading.Event()
        second_started = threading.Event()
        second_entered = threading.Event()
        release = threading.Event()
        calls = 0

        def request(_url: str, **_kwargs: object) -> str:
            nonlocal calls
            calls += 1
            if calls == 1:
                first_entered.set()
                if not release.wait(timeout=2):
                    raise TimeoutError
            else:
                second_entered.set()
            return '<input name="outTemp" value="72.5" disabled>'

        def discover() -> str | None:
            second_started.set()
            return probe_weather_station("127.0.0.1")

        with (
            patch("ambientweather2sqlite.mureq.get", side_effect=request),
            patch(
                "builtins.print",
            ),
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            first = executor.submit(fetch_once, "http://127.0.0.1/livedata.htm")
            try:
                self.assertTrue(first_entered.wait(timeout=2))
                second = executor.submit(discover)
                self.assertTrue(second_started.wait(timeout=2))
                self.assertFalse(second_entered.wait(timeout=0.05))
            finally:
                release.set()
            first.result(timeout=2)
            self.assertEqual(second.result(timeout=2), "http://127.0.0.1/livedata.htm")
        self.assertEqual(calls, 2)
