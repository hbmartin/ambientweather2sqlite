"""Monitor clock normalization and explicit snapshot-read regressions."""

# Private query results and state establish that monitoring leaves rows intact.
# ruff: noqa: SLF001

import sqlite3
import tempfile
from contextlib import closing
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Literal, assert_never
from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

from ambientweather2sqlite.database import (
    create_database_if_not_exists,
    insert_observation,
    metrics,
)
from ambientweather2sqlite.models import RecoveryConfig
from ambientweather2sqlite.recovery import CollectionMonitor

_NOW = datetime(2026, 10, 12, 16, tzinfo=UTC)
type ClockRepresentation = Literal["naive", "utc", "offset"]


def _clock_representation(
    value: datetime, style: ClockRepresentation, offset_minutes: int
) -> datetime:
    match style:
        case "naive":
            return value.replace(tzinfo=None)
        case "utc":
            return value
        case "offset":
            return value.astimezone(timezone(timedelta(minutes=offset_minutes)))
        case _:
            assert_never(style)


class TestMonitorReview(TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.database = str(Path(directory.name) / "weather.db")
        create_database_if_not_exists(self.database)

    def test_supplied_clocks_normalize_to_utc_for_empty_and_populated_database(
        self,
    ) -> None:
        clocks = (
            _NOW.replace(tzinfo=None),
            _NOW,
            _NOW.astimezone(timezone(timedelta(hours=5, minutes=30))),
        )
        for populated in (False, True):
            if populated:
                insert_observation(
                    self.database, {"ts": (_NOW - timedelta(seconds=30)).isoformat()}
                )
            for supplied in clocks:
                with (
                    self.subTest(populated=populated, supplied=supplied),
                    patch(
                        "ambientweather2sqlite.database.metrics.datetime",
                        wraps=datetime,
                    ) as clock,
                ):
                    raw, latest, rejected, checked_at = (
                        metrics._query_monitor_timestamp(self.database, supplied)
                    )
                    logger = Mock()
                    monitor = CollectionMonitor(
                        self.database, RecoveryConfig(), logger, started_at=_NOW
                    )
                    monitor.check(supplied)
                    clock.now.assert_not_called()
                    self.assertEqual(checked_at, _NOW)
                    self.assertIs(checked_at.tzinfo, UTC)
                    self.assertFalse(rejected)
                    self.assertFalse(monitor.stalled)
                    logger.error.assert_not_called()
                    if populated:
                        self.assertEqual(raw, "2026-10-12 15:59:30")
                        self.assertEqual(latest, _NOW - timedelta(seconds=30))
                    else:
                        self.assertIsNone(raw)
                        self.assertIsNone(latest)

    def test_start_time_representations_have_identical_empty_database_ages(
        self,
    ) -> None:
        starts = (
            _NOW.replace(tzinfo=None),
            _NOW,
            _NOW.astimezone(timezone(timedelta(hours=-7))),
        )
        for start in starts:
            for supplied in starts:
                with self.subTest(start=start, supplied=supplied):
                    logger = Mock()
                    events = Mock()
                    monitor = CollectionMonitor(
                        self.database,
                        RecoveryConfig(),
                        logger,
                        events,
                        started_at=start,
                    )
                    with patch(
                        "ambientweather2sqlite.database.metrics.datetime",
                        wraps=datetime,
                    ) as clock:
                        monitor.check(supplied + timedelta(seconds=299))
                        self.assertFalse(monitor.stalled)
                        logger.error.assert_not_called()
                        monitor.check(supplied + timedelta(seconds=300))
                        clock.now.assert_not_called()
                    self.assertTrue(monitor.stalled)
                    events.assert_called_once_with("collection_stalled", None, 300.0)
                    self.assertEqual(monitor.started_at, start)
                    self.assertEqual(monitor.started_at.tzinfo, start.tzinfo)

    def test_naive_start_handles_future_only_then_valid_observation_recovery(
        self,
    ) -> None:
        future = _NOW + timedelta(days=1)
        insert_observation(self.database, {"ts": future.isoformat()})
        events = Mock()
        monitor = CollectionMonitor(
            self.database,
            RecoveryConfig(),
            Mock(),
            events,
            started_at=(_NOW - timedelta(seconds=300)).replace(tzinfo=None),
        )
        monitor.check(_NOW.replace(tzinfo=None))
        self.assertTrue(monitor.stalled)
        self.assertTrue(monitor._timestamp_error)
        self.assertIsNone(monitor._last_verified_timestamp)
        for step in range(1, 4):
            observed_at = _NOW + timedelta(seconds=step)
            insert_observation(self.database, {"ts": observed_at.isoformat()})
            monitor.check(observed_at)
        self.assertFalse(monitor.stalled)
        self.assertEqual(monitor._last_verified_timestamp, _NOW + timedelta(seconds=3))
        self.assertEqual(
            [call.args[0] for call in events.call_args_list],
            ["collection_stalled", "collection_resumed"],
        )
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0], 4
            )
            self.assertEqual(
                connection.execute("SELECT MAX(ts) FROM observations").fetchone()[0],
                "2026-10-13 16:00:00",
            )

    @settings(deadline=None)
    @given(
        offset_minutes=st.integers(min_value=-1439, max_value=1439),
        naive=st.booleans(),
    )
    def test_clock_representation_does_not_change_selected_timestamp(
        self, *, offset_minutes: int, naive: bool
    ) -> None:
        insert_observation(
            self.database, {"ts": (_NOW - timedelta(microseconds=1)).isoformat()}
        )
        supplied = (
            _NOW.replace(tzinfo=None)
            if naive
            else _NOW.astimezone(timezone(timedelta(minutes=offset_minutes)))
        )
        self.assertEqual(
            metrics._query_monitor_timestamp(self.database, supplied),
            (
                "2026-10-12 15:59:59.999999",
                _NOW - timedelta(microseconds=1),
                False,
                _NOW,
            ),
        )
        logger = Mock()
        monitor = CollectionMonitor(
            self.database, RecoveryConfig(), logger, started_at=_NOW
        )
        monitor.check(supplied)
        self.assertFalse(monitor.stalled)
        logger.error.assert_not_called()

    @settings(deadline=None)
    @given(
        start_style=st.sampled_from(("naive", "utc", "offset")),
        check_style=st.sampled_from(("naive", "utc", "offset")),
        start_offset=st.integers(min_value=-1439, max_value=1439),
        check_offset=st.integers(min_value=-1439, max_value=1439),
        age=st.one_of(st.sampled_from((0, 299, 300, 301, 600)), st.integers(0, 600)),
        future_only=st.booleans(),
    )
    def test_fallback_age_is_independent_of_start_and_check_representations(
        self,
        *,
        start_style: ClockRepresentation,
        check_style: ClockRepresentation,
        start_offset: int,
        check_offset: int,
        age: int,
        future_only: bool,
    ) -> None:
        start = _clock_representation(
            _NOW - timedelta(seconds=age), start_style, start_offset
        )
        supplied = _clock_representation(_NOW, check_style, check_offset)
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "weather.db")
            create_database_if_not_exists(database)
            if future_only:
                insert_observation(
                    database, {"ts": (_NOW + timedelta(days=1)).isoformat()}
                )
            with closing(sqlite3.connect(database)) as connection:
                before = connection.execute("SELECT ts FROM observations").fetchall()
            logger = Mock()
            events = Mock()
            monitor = CollectionMonitor(
                database, RecoveryConfig(), logger, events, started_at=start
            )
            with patch(
                "ambientweather2sqlite.database.metrics.datetime", wraps=datetime
            ) as clock:
                monitor.check(supplied)
                clock.now.assert_not_called()
            self.assertEqual(monitor.stalled, age >= monitor.config.stale_seconds)
            self.assertFalse(monitor._database_error)
            self.assertEqual(monitor._timestamp_error, future_only)
            self.assertIsNone(monitor._last_verified_timestamp)
            self.assertIs(monitor.started_at, start)
            if monitor.stalled:
                events.assert_called_once_with("collection_stalled", None, float(age))
            else:
                events.assert_not_called()
            self.assertEqual(
                logger.error.call_count, int(future_only) + int(monitor.stalled)
            )
            with closing(sqlite3.connect(database)) as connection:
                after = connection.execute("SELECT ts FROM observations").fetchall()
            self.assertEqual(after, before)

    def test_first_row_is_fetched_before_clock_and_older_rows_stay_unread(self) -> None:
        phases: list[str] = []
        cursor = MagicMock(spec=sqlite3.Cursor)

        def fetch() -> tuple[str]:
            phases.append("fetch")
            return ("2026-10-12 16:00:00",)

        def sample(_tz: object) -> datetime:
            phases.append("clock")
            return _NOW

        cursor.fetchone.side_effect = fetch
        cursor.__iter__.side_effect = AssertionError("Older rows should remain unread")
        connection = Mock(execute=Mock(return_value=cursor))
        with (
            patch(
                "ambientweather2sqlite.database.metrics._connect_database",
                return_value=connection,
            ),
            patch(
                "ambientweather2sqlite.database.metrics.datetime", wraps=datetime
            ) as clock,
        ):
            clock.now.side_effect = sample
            result = metrics._query_monitor_timestamp(self.database)
        self.assertEqual(result, ("2026-10-12 16:00:00", _NOW, False, _NOW))
        self.assertEqual(phases, ["fetch", "clock"])
        cursor.__iter__.assert_not_called()
        connection.close.assert_called_once()

    def test_empty_result_fetches_before_sampling_clock(self) -> None:
        phases: list[str] = []
        cursor = MagicMock(spec=sqlite3.Cursor)
        cursor.fetchone.side_effect = lambda: phases.append("fetch")
        cursor.__iter__.return_value = iter(())
        connection = Mock(execute=Mock(return_value=cursor))

        def sample(_tz: object) -> datetime:
            phases.append("clock")
            return _NOW

        with (
            patch(
                "ambientweather2sqlite.database.metrics._connect_database",
                return_value=connection,
            ),
            patch(
                "ambientweather2sqlite.database.metrics.datetime", wraps=datetime
            ) as clock,
        ):
            clock.now.side_effect = sample
            result = metrics._query_monitor_timestamp(self.database)
        self.assertEqual(result, (None, None, False, _NOW))
        self.assertEqual(phases, ["fetch", "clock"])

    def test_commit_at_deferred_first_fetch_is_not_mistaken_for_a_future_row(
        self,
    ) -> None:
        latest = _NOW + timedelta(microseconds=1)
        phases: list[str] = []
        cursor = MagicMock(spec=sqlite3.Cursor)
        connection = metrics._connect_database(self.database, read_only=True)
        self.addCleanup(connection.close)

        def fetch() -> tuple[str]:
            phases.append("fetch")
            insert_observation(self.database, {"ts": latest.isoformat()})
            rows = connection.execute("SELECT ts FROM observations ORDER BY ts DESC")
            cursor.__iter__.return_value = rows
            return rows.fetchone()

        def sample(_tz: object) -> datetime:
            phases.append("clock")
            return latest

        cursor.fetchone.side_effect = fetch
        deferred = Mock(execute=Mock(return_value=cursor), close=connection.close)
        logger = Mock()
        monitor = CollectionMonitor(
            self.database, RecoveryConfig(), logger, started_at=_NOW
        )
        with (
            patch(
                "ambientweather2sqlite.database.metrics._connect_database",
                return_value=deferred,
            ),
            patch(
                "ambientweather2sqlite.database.metrics.datetime", wraps=datetime
            ) as clock,
        ):
            clock.now.side_effect = sample
            monitor.check()
        self.assertEqual(phases, ["fetch", "clock"])
        self.assertEqual(monitor._last_verified_timestamp, latest)
        self.assertFalse(monitor._timestamp_error)
        logger.error.assert_not_called()

    def test_rejected_first_rows_stream_to_valid_recovery_without_changing_data(
        self,
    ) -> None:
        stale = _NOW - timedelta(seconds=600)
        insert_observation(self.database, {"ts": stale.isoformat()})
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.executemany(
                "INSERT INTO observations (ts) VALUES (?)",
                [("malformed",), ((_NOW + timedelta(days=1)).isoformat(),), (123,)],
            )
        events = Mock()
        monitor = CollectionMonitor(
            self.database, RecoveryConfig(), Mock(), events, started_at=stale
        )
        monitor.check(_NOW.replace(tzinfo=None))
        self.assertTrue(monitor.stalled)
        self.assertTrue(monitor._timestamp_error)
        recovery_count = monitor.config.recovery_observations
        for step in range(1, recovery_count + 1):
            observed_at = _NOW + timedelta(seconds=step)
            insert_observation(self.database, {"ts": observed_at.isoformat()})
            supplied = observed_at.astimezone(timezone(timedelta(hours=-7)))
            monitor.check(supplied)
            self.assertEqual(
                monitor.fresh_observations, step if step < recovery_count else 0
            )
        self.assertFalse(monitor.stalled)
        self.assertEqual(
            [call.args[0] for call in events.call_args_list],
            ["collection_stalled", "collection_resumed"],
        )
        with closing(sqlite3.connect(self.database)) as connection:
            before = connection.execute(
                "SELECT ts FROM observations ORDER BY ts"
            ).fetchall()
        metrics._query_monitor_timestamp(self.database, _NOW + timedelta(seconds=4))
        with closing(sqlite3.connect(self.database)) as connection:
            after = connection.execute(
                "SELECT ts FROM observations ORDER BY ts"
            ).fetchall()
        self.assertEqual(after, before)
        self.assertEqual(len(after), 7)
