"""Pure UTC normalization for supplied datetimes."""

from datetime import UTC, datetime


def normalize_utc(value: datetime) -> datetime:
    """Interpret naive values as UTC and preserve timezone-aware instants."""
    if value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
