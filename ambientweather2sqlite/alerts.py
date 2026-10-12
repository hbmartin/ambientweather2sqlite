"""Webhook notifications for station outages and recoveries."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from http.client import HTTPException
from typing import TYPE_CHECKING

from . import mureq

if TYPE_CHECKING:
    import logging

    from .models import AlertConfig


@dataclass(slots=True)
class AlertNotifier:
    """Track consecutive fetch failures and POST webhook events.

    A ``station_offline`` event fires once when the failure count reaches
    the configured threshold; a ``station_online`` event fires on the first
    success after that. Webhook errors are logged and never raised.
    """

    config: AlertConfig
    station_url: str
    logger: logging.Logger
    consecutive_failures: int = 0
    alert_sent: bool = False
    _pending_failure_peak: int = field(default=0, init=False)

    def record_failure(self) -> None:
        """Count a failed fetch, firing station_offline at the threshold."""
        self.consecutive_failures += 1
        if (
            not self.alert_sent
            and self.consecutive_failures >= self.config.failure_threshold
        ):
            self.alert_sent = True
            self._remember_failure_peak()
            self._post("station_offline")
        else:
            self._remember_failure_peak()

    def _remember_failure_peak(self) -> None:
        if self.alert_sent:
            self._pending_failure_peak = max(
                self._pending_failure_peak, self.consecutive_failures
            )

    def record_fetch_success(self) -> None:
        """Reset fetch failures without declaring stored-data recovery."""
        self._remember_failure_peak()
        self.consecutive_failures = 0

    def record_success(self) -> None:
        """Reset the failure count, firing station_online after an outage."""
        if self.alert_sent:
            self._remember_failure_peak()
            self._post("station_online")
        self.consecutive_failures = 0
        self.alert_sent = False
        self._pending_failure_peak = 0

    def _post(self, event: str) -> None:
        payload: dict[str, object] = {
            "event": event,
            "station_url": self.station_url,
            "consecutive_failures": (
                self._pending_failure_peak
                if event == "station_online"
                else self.consecutive_failures
            ),
            "timestamp": datetime.now(UTC).isoformat(),
        }
        self._send(payload)

    def collection_event(
        self,
        event: str,
        last_observation_ts: str | None,
        age_seconds: float,
    ) -> None:
        """Send a freshness transition without changing fetch-failure counters."""
        self._send(
            {
                "event": event,
                "station_url": self.station_url,
                "last_observation_ts": last_observation_ts,
                "age_seconds": age_seconds,
                "timestamp": datetime.now(UTC).isoformat(),
            }
        )

    def _send(self, payload: dict[str, object]) -> None:
        try:
            mureq.post(
                self.config.webhook_url,
                json=payload,
                timeout=self.config.timeout_seconds,
            )
        except (HTTPException, OSError, TimeoutError) as e:
            self.logger.warning("Alert webhook failed: %s", e)
