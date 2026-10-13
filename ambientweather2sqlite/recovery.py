"""Monitor committed observations inside the collector process."""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from .database.metrics import _query_monitor_timestamp
from .time_utils import normalize_utc

if TYPE_CHECKING:
    import logging

    from .models import RecoveryConfig

type CollectionEvent = Callable[[str, str | None, float], None]


def _observation_age(latest: datetime, now: datetime) -> float:
    age = (now - normalize_utc(latest)).total_seconds()
    if age < 0:
        msg = "latest observation timestamp is in the future"
        raise ValueError(msg)
    return age


@dataclass(slots=True)
class CollectionMonitor:
    """Check SQLite independently of possibly stalled receiver requests.

    A stale database produces one error per outage. Recovery requires distinct,
    advancing committed timestamps across several checks. No station requests,
    process restarts, power changes, or synthetic observations are performed.
    """

    database_path: str
    config: RecoveryConfig
    logger: logging.Logger
    on_event: CollectionEvent | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    stalled: bool = field(default=False, init=False)
    fresh_observations: int = field(default=0, init=False)
    _last_timestamp: datetime | None = field(default=None, init=False)
    _last_verified_timestamp: datetime | None = field(default=None, init=False)
    _database_error: bool = field(default=False, init=False)
    _timestamp_error: bool = field(default=False, init=False)
    _stop: threading.Event = field(default_factory=threading.Event, init=False)
    _thread: threading.Thread | None = field(default=None, init=False)

    def check(self, now: datetime | None = None) -> None:
        """Inspect freshness once; database failures never imply recovery."""
        try:
            raw, latest, rejected, checked_at = _query_monitor_timestamp(
                self.database_path, now
            )
            age = _observation_age(
                latest or self._last_verified_timestamp or self.started_at,
                checked_at,
            )
        except (sqlite3.Error, OSError, ValueError) as error:
            self._report_database_error(error)
            return
        if self._stop.is_set():
            return
        if self._database_error:
            self.logger.info("Collection freshness database checks restored")
            self._database_error = False
        if rejected and not self._timestamp_error:
            self.logger.error(
                "Ignoring invalid or future observation timestamps in freshness checks",
            )
        self._timestamp_error = rejected
        if latest is not None and (
            self._last_verified_timestamp is None
            or latest > self._last_verified_timestamp
        ):
            self._last_verified_timestamp = latest
        self._check_freshness(raw, latest, age)

    def _report_database_error(self, error: Exception) -> None:
        self.fresh_observations = 0
        if not self._database_error:
            self.logger.error("Cannot verify collection freshness: %s", error)
        self._database_error = True

    def _check_freshness(
        self,
        raw: str | None,
        latest: datetime | None,
        age: float,
    ) -> None:
        if age >= self.config.stale_seconds:
            self.fresh_observations = 0
            self._last_timestamp = latest
            if not self.stalled:
                self.stalled = True
                self.logger.error(
                    "Collection stalled: no fresh observations for %.0f seconds "
                    "(latest: %s). Retries and rediscovery continue; check the "
                    "receiver and network. A manual receiver power restart may "
                    "be required; automatic power cycling is disabled.",
                    age,
                    raw,
                )
                self._notify("collection_stalled", raw, age)
            return
        if self.stalled and latest is not None:
            if self._last_timestamp is not None and latest < self._last_timestamp:
                self.fresh_observations = 0
                return
            if self._last_timestamp is None or latest > self._last_timestamp:
                self.fresh_observations += 1
            if self.fresh_observations >= self.config.recovery_observations:
                self.stalled = False
                self.logger.info(
                    "Collection resumed: verified %d advancing fresh "
                    "observations (latest: %s, age: %.0f seconds)",
                    self.fresh_observations,
                    raw,
                    age,
                )
                self._notify("collection_resumed", raw, age)
                self.fresh_observations = 0
        self._last_timestamp = latest

    def _notify(self, event: str, raw: str | None, age: float) -> None:
        if self.on_event is not None and not self._stop.is_set():
            try:
                self.on_event(event, raw, age)
            except (Exception,):  # noqa: B013
                self.logger.exception("Collection event callback failed: %s", event)

    def start(self) -> None:
        """Start an independent thread within the existing daemon."""
        self._thread = threading.Thread(
            target=self._run,
            name="aw2sqlite-collection-monitor",
            daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.check()
            except (Exception,):  # noqa: B013
                self.fresh_observations = 0
                self.logger.exception("Unexpected collection monitor check failure")
            self._stop.wait(self.config.check_interval_seconds)

    def stop(self) -> None:
        """Wake the monitor immediately and bound shutdown waiting."""
        self._stop.set()
        if (
            self._thread is not None
            and self._thread.ident is not None
            and self._thread is not threading.current_thread()
        ):
            self._thread.join(timeout=1.0)
            if self._thread.is_alive():
                self.logger.warning(
                    "Collection monitor is still finishing an in-flight check "
                    "or callback",
                )


@contextmanager
def monitor_collection(
    database_path: str,
    config: RecoveryConfig | None,
    logger: logging.Logger,
    on_event: CollectionEvent | None = None,
) -> Iterator[None]:
    """Keep the monitor alive during metadata fetching and collection."""
    if config is None or not config.enabled:
        yield
        return
    monitor = CollectionMonitor(database_path, config, logger, on_event)
    try:
        monitor.start()
        yield
    finally:
        monitor.stop()
