"""Render database metrics in the Prometheus text exposition format."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from .database.connection import _TS_COL, _parse_stored_timestamp

if TYPE_CHECKING:
    from .models import DbMetrics, Observation

_PREFIX = "aw2sqlite"


def _escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def build_prometheus_metrics(
    metrics: DbMetrics,
    latest_observation: Observation | None,
    *,
    now: datetime | None = None,
) -> str:
    """Build a Prometheus text-format payload from database state.

    Includes row count, database size, seconds since the last observation,
    and one gauge per sensor from the most recent observation.
    """
    current_time = now if now is not None else datetime.now(UTC)
    lines = [
        f"# HELP {_PREFIX}_observations_total Total number of observations recorded.",
        f"# TYPE {_PREFIX}_observations_total counter",
        f"{_PREFIX}_observations_total {metrics['row_count']}",
        f"# HELP {_PREFIX}_db_file_size_bytes Size of the SQLite database file.",
        f"# TYPE {_PREFIX}_db_file_size_bytes gauge",
        f"{_PREFIX}_db_file_size_bytes {metrics['db_file_size_bytes']}",
    ]

    if latest_observation is not None:
        ts_value = latest_observation.get(_TS_COL)
        if isinstance(ts_value, str):
            age = (current_time - _parse_stored_timestamp(ts_value)).total_seconds()
            lines.extend(
                (
                    f"# HELP {_PREFIX}_last_observation_age_seconds "
                    "Seconds since the most recent observation.",
                    f"# TYPE {_PREFIX}_last_observation_age_seconds gauge",
                    f"{_PREFIX}_last_observation_age_seconds {age}",
                ),
            )

        sensor_lines = [
            f'{_PREFIX}_sensor_value{{sensor="{_escape_label_value(column)}"}} {value}'
            for column, value in latest_observation.items()
            if column != _TS_COL and isinstance(value, int | float)
        ]
        if sensor_lines:
            lines.extend(
                (
                    f"# HELP {_PREFIX}_sensor_value "
                    "Latest sensor readings by sensor name.",
                    f"# TYPE {_PREFIX}_sensor_value gauge",
                    *sensor_lines,
                ),
            )

    return "\n".join(lines) + "\n"
