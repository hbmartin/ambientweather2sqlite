"""SQLite persistence and query layer for weather observations."""

from .aggregation import (
    query_daily_aggregated_data,
    query_hourly_aggregated_data,
    query_range_aggregated_data,
)
from .backup import backup_database
from .connection import _parse_stored_timestamp as parse_stored_timestamp
from .export import export_observations
from .metrics import (
    DEFAULT_GAP_THRESHOLD_SECONDS,
    query_db_metrics,
    query_latest_timestamp,
    query_observation_gaps,
)
from .migrations import inspect_database_migration, migrate_database
from .schema import create_database_if_not_exists
from .sensor_metadata import persist_sensor_metadata, query_sensor_metadata
from .writes import insert_observation

__all__ = [
    "DEFAULT_GAP_THRESHOLD_SECONDS",
    "backup_database",
    "create_database_if_not_exists",
    "export_observations",
    "insert_observation",
    "inspect_database_migration",
    "migrate_database",
    "parse_stored_timestamp",
    "persist_sensor_metadata",
    "query_daily_aggregated_data",
    "query_db_metrics",
    "query_hourly_aggregated_data",
    "query_latest_timestamp",
    "query_observation_gaps",
    "query_range_aggregated_data",
    "query_sensor_metadata",
]
