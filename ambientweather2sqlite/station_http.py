"""Serialize receiver HTTP requests across threads within one process."""

from threading import Lock

# The ObserverIP can stall when web requests overlap. Collector, metadata,
# discovery, and the live API proxy share this lock; freshness checks use SQLite.
STATION_REQUEST_LOCK = Lock()
