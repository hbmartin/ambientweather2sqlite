"""Webhook notifications for station outages and recoveries."""

from __future__ import annotations

from dataclasses import dataclass
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

    def record_failure(self) -> None:
        """Count a failed fetch, firing station_offline at the threshold."""
        self.consecutive_failures += 1
        if (
            not self.alert_sent
            and self.consecutive_failures >= self.config.failure_threshold
        ):
            self.alert_sent = True
            self._post("station_offline")

    def record_success(self) -> None:
        """Reset the failure count, firing station_online after an outage."""
        if self.alert_sent:
            self._post("station_online")
        self.consecutive_failures = 0
        self.alert_sent = False

    def _post(self, event: str) -> None:
        payload = {
            "event": event,
            "station_url": self.station_url,
            "consecutive_failures": self.consecutive_failures,
            "timestamp": datetime.now(UTC).isoformat(),
        }
        try:
            mureq.post(
                self.config.webhook_url,
                json=payload,
                timeout=self.config.timeout_seconds,
            )
        except (HTTPException, OSError, TimeoutError) as e:
            self.logger.warning("Alert webhook failed: %s", e)
