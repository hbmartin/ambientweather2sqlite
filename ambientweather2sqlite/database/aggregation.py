"""Timezone-aware daily, hourly, and range aggregation queries."""

from __future__ import annotations

import re
from contextlib import closing
from datetime import UTC, date, datetime, time, timedelta
from datetime import timezone as timezone_cls
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from ambientweather2sqlite.exceptions import (
    InvalidColumnNameError,
    InvalidDateError,
    InvalidDateRangeError,
    InvalidFormatError,
    InvalidPriorDaysError,
    InvalidTimezoneError,
    MissingAggregationFieldsError,
)

from .connection import (
    _DEFAULT_TABLE_NAME,
    _TS_COL,
    _connect_database,
    _format_sqlite_timestamp,
    _parse_stored_timestamp,
)
from .identifiers import _quote_identifier

if TYPE_CHECKING:
    import sqlite3

    from ambientweather2sqlite.models import (
        AggregatedRow,
        AggregationField,
        HourlyAggregatedData,
    )


def _validate_timezone(tz: str | None) -> str | ZoneInfo:
    if not tz or tz == "localtime":
        return "localtime"

    try:
        if ":" in tz:
            hours_str, minutes_str = tz.split(":")
            hours, minutes = int(hours_str), int(minutes_str)
            # int("-0") == 0, so the sign must come from the string
            sign = -1 if hours_str.strip().startswith("-") else 1
            offset_hours = sign * (abs(hours) + minutes / 60)
        else:
            val = float(tz)
            # Heuristic for (+-)HHMM format
            if abs(val) > 24:  # noqa: PLR2004
                sign = -1 if val < 0 else 1
                abs_val = abs(val)
                offset_hours = sign * (abs_val // 100 + (abs_val % 100) / 60)
            else:
                offset_hours = val
    except ValueError:
        pass
    else:
        try:
            timezone_cls(timedelta(hours=offset_hours))
        except (OverflowError, ValueError) as e:
            raise InvalidTimezoneError(tz) from e
        return f"{offset_hours} hours"

    try:
        return ZoneInfo(tz)
    except (ModuleNotFoundError, ValueError, KeyError) as e:
        raise InvalidTimezoneError(tz) from e


def _parse_aggregation_fields(
    aggregation_fields: list[str],
) -> list[AggregationField]:
    parsed_fields = []

    for field in aggregation_fields:
        # Parse field like "avg_outHumi" into ("avg", "outHumi")
        match = re.match(r"^(avg|max|min|sum)_(.+)$", field, re.IGNORECASE)
        if not match:
            raise InvalidFormatError(field)

        agg_func, column_name = match.groups()

        # Sanitize column name (basic SQL injection protection)
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", column_name):
            raise InvalidColumnNameError(column_name)

        parsed_fields.append((agg_func.upper(), column_name, field))

    if not parsed_fields:
        raise MissingAggregationFieldsError

    return parsed_fields


def _select_parts_from_parsed_fields(
    parsed_fields: list[AggregationField],
    datetime_expression: str,
) -> list[str]:
    select_parts = [datetime_expression]

    select_parts.extend(
        f"{agg_func}({_quote_identifier(column_name)}) as {_quote_identifier(alias)}"
        for agg_func, column_name, alias in parsed_fields
    )

    select_parts.append("COUNT(*) as count")

    return select_parts


def _parse_query_date(date_string: str) -> date:
    try:
        return date.fromisoformat(date_string)
    except ValueError as e:
        raise InvalidDateError(date_string) from e


def _current_date_for_timezone(timezone: str | ZoneInfo) -> date:
    if isinstance(timezone, ZoneInfo):
        return datetime.now(timezone).date()

    if timezone == "localtime":
        return datetime.now().date()

    offset_hours = float(timezone.removesuffix(" hours"))
    return (datetime.now(UTC) + timedelta(hours=offset_hours)).date()


def _normalize_hourly_date_range(
    start_date: str,
    end_date: str | None,
    timezone: str | ZoneInfo,
) -> tuple[date, date]:
    start_date_obj = _parse_query_date(start_date)
    if end_date is None:
        candidate_end_date = _current_date_for_timezone(timezone)
        end_date_obj = max(candidate_end_date, start_date_obj)
    else:
        end_date_obj = _parse_query_date(end_date)

    if end_date_obj < start_date_obj:
        raise InvalidDateRangeError(
            start_date_obj.isoformat(),
            end_date_obj.isoformat(),
        )

    return start_date_obj, end_date_obj


def _date_keys_in_range(start_date: date, end_date: date) -> list[str]:
    day_count = (end_date - start_date).days + 1
    return [
        (start_date + timedelta(days=offset)).isoformat() for offset in range(day_count)
    ]


def _empty_hourly_slots() -> list[AggregatedRow | None]:
    return [None for _ in range(24)]


def _fetch_rows_for_zoneinfo_range(
    db_path: str,
    columns: set[str],
    start_date: date,
    end_date: date,
    timezone: ZoneInfo,
) -> list[sqlite3.Row]:
    start_ts = _format_sqlite_timestamp(
        datetime.combine(start_date, time.min, tzinfo=timezone),
    )
    end_ts = _format_sqlite_timestamp(
        datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=timezone),
    )
    select_columns = ", ".join(
        _quote_identifier(column) for column in [_TS_COL, *sorted(columns)]
    )
    query = (
        f"SELECT {select_columns} FROM {_quote_identifier(_DEFAULT_TABLE_NAME)} "
        f"WHERE {_quote_identifier(_TS_COL)} >= ? "
        f"AND {_quote_identifier(_TS_COL)} < ? "
        f"ORDER BY {_quote_identifier(_TS_COL)}"
    )

    with closing(
        _connect_database(db_path, read_only=True, use_row_factory=True),
    ) as conn:
        cursor = conn.cursor().execute(query, (start_ts, end_ts))
        return cursor.fetchall()


def _aggregate_rows(
    rows: list[sqlite3.Row],
    parsed_fields: list[AggregationField],
) -> AggregatedRow:
    result: AggregatedRow = {"count": len(rows)}

    for agg_func, column_name, alias in parsed_fields:
        values = [row[column_name] for row in rows if row[column_name] is not None]
        if not values:
            result[alias] = None
        elif agg_func == "AVG":
            result[alias] = sum(values) / len(values)
        elif agg_func == "MAX":
            result[alias] = max(values)
        elif agg_func == "MIN":
            result[alias] = min(values)
        elif agg_func == "SUM":
            result[alias] = sum(values)

    return result


def _query_daily_aggregated_data_with_zoneinfo(
    db_path: str,
    parsed_fields: list[AggregationField],
    prior_days: int,
    timezone: ZoneInfo,
) -> list[AggregatedRow]:
    today = datetime.now(timezone).date()
    start_date = today - timedelta(days=prior_days)
    rows = _fetch_rows_for_zoneinfo_range(
        db_path=db_path,
        columns={column_name for _, column_name, _ in parsed_fields},
        start_date=start_date,
        end_date=today,
        timezone=timezone,
    )
    rows_by_date: dict[str, list[sqlite3.Row]] = {}

    for row in rows:
        date_key = _parse_stored_timestamp(row[_TS_COL]).astimezone(timezone).date()
        rows_by_date.setdefault(date_key.isoformat(), []).append(row)

    result: list[AggregatedRow] = []
    for date_key in sorted(rows_by_date):
        row_result: AggregatedRow = {"date": date_key}
        row_result.update(_aggregate_rows(rows_by_date[date_key], parsed_fields))
        result.append(row_result)

    return result


def _query_hourly_aggregated_data_with_zoneinfo(
    db_path: str,
    parsed_fields: list[AggregationField],
    start_date: date,
    end_date: date,
    timezone: ZoneInfo,
) -> HourlyAggregatedData:
    rows = _fetch_rows_for_zoneinfo_range(
        db_path=db_path,
        columns={column_name for _, column_name, _ in parsed_fields},
        start_date=start_date,
        end_date=end_date,
        timezone=timezone,
    )
    rows_by_date_and_hour: dict[str, dict[int, list[sqlite3.Row]]] = {}

    for row in rows:
        local_dt = _parse_stored_timestamp(row[_TS_COL]).astimezone(timezone)
        date_key = local_dt.date().isoformat()
        hour_rows = rows_by_date_and_hour.setdefault(date_key, {})
        hour_rows.setdefault(local_dt.hour, []).append(row)

    result: HourlyAggregatedData = {
        date_key: _empty_hourly_slots()
        for date_key in _date_keys_in_range(start_date, end_date)
    }
    for date_key, hour_rows in rows_by_date_and_hour.items():
        hours = result[date_key]
        for hour, bucket_rows in hour_rows.items():
            row_result: AggregatedRow = {
                "date": date_key,
                "hour": f"{hour:02d}",
            }
            row_result.update(_aggregate_rows(bucket_rows, parsed_fields))
            hours[hour] = row_result

    return result


def _local_isoformat_to_utc_timestamp(
    value: str,
    timezone: str | ZoneInfo,
) -> str:
    """Convert an ISO date/datetime in the query timezone to a stored UTC string."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as e:
        raise InvalidDateError(value) from e

    if parsed.tzinfo is not None:
        return _format_sqlite_timestamp(parsed)
    if isinstance(timezone, ZoneInfo):
        return _format_sqlite_timestamp(parsed.replace(tzinfo=timezone))
    if timezone == "localtime":
        return _format_sqlite_timestamp(parsed.astimezone())

    try:
        offset_hours = float(timezone.removesuffix(" hours"))
        fixed_offset = timezone_cls(timedelta(hours=offset_hours))
    except (OverflowError, ValueError) as e:
        raise InvalidTimezoneError(timezone) from e
    return _format_sqlite_timestamp(parsed.replace(tzinfo=fixed_offset))


def query_range_aggregated_data(
    db_path: str,
    aggregation_fields: list[str],
    start: str,
    end: str | None = None,
    tz: str | None = None,
) -> AggregatedRow:
    """Aggregate observations over an arbitrary [start, end) window.

    Args:
        db_path: Path to SQLite database file
        aggregation_fields: List of aggregation specifications like ["avg_outHumi"]
        start: Window start as ISO date or datetime, interpreted in `tz`
        end: Window end (exclusive), defaults to now when None
        tz: Timezone string (e.g., 'America/New_York', '+05:30')

    Returns:
        Dict with the resolved UTC window bounds, aggregated values, and count

    """
    parsed_fields = _parse_aggregation_fields(aggregation_fields)
    timezone = _validate_timezone(tz)

    start_ts = _local_isoformat_to_utc_timestamp(start, timezone)
    if end is None:
        end_ts = _format_sqlite_timestamp(datetime.now(UTC))
    else:
        end_ts = _local_isoformat_to_utc_timestamp(end, timezone)

    if end_ts < start_ts:
        raise InvalidDateRangeError(start_ts, end_ts)

    select_parts = [
        f"{agg_func}({_quote_identifier(column_name)}) as {_quote_identifier(alias)}"
        for agg_func, column_name, alias in parsed_fields
    ]
    select_parts.append("COUNT(*) as count")

    query = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM {_quote_identifier(_DEFAULT_TABLE_NAME)} "
        f"WHERE {_quote_identifier(_TS_COL)} >= ? "
        f"AND {_quote_identifier(_TS_COL)} < ?"
    )

    with closing(
        _connect_database(db_path, read_only=True, use_row_factory=True),
    ) as conn:
        cursor = conn.cursor().execute(query, (start_ts, end_ts))
        row: AggregatedRow = dict(cursor.fetchone())

    return {"start": start_ts, "end": end_ts, **row}


def query_daily_aggregated_data(
    db_path: str,
    aggregation_fields: list[str],
    prior_days: int = 7,
    tz: str | None = None,
) -> list[AggregatedRow]:
    """Query SQLite database with dynamic aggregation fields.

    Args:
        db_path: Path to SQLite database file
        aggregation_fields: List of aggregation specifications like ["avg_outHumi"]
        prior_days: Number of days to include in the query (not including today)
        tz: Timezone string (e.g., 'America/New_York', '+05:30')

    Returns:
        Sorted list of dicts of aggregated values

    """
    if not isinstance(prior_days, int):
        raise InvalidPriorDaysError(prior_days)

    parsed_fields = _parse_aggregation_fields(aggregation_fields)
    timezone = _validate_timezone(tz)
    if isinstance(timezone, ZoneInfo):
        return _query_daily_aggregated_data_with_zoneinfo(
            db_path=db_path,
            parsed_fields=parsed_fields,
            prior_days=prior_days,
            timezone=timezone,
        )

    table_name: str = _DEFAULT_TABLE_NAME
    date_column: str = _TS_COL

    quoted_date_column = _quote_identifier(date_column)
    datetime_expression = f"DATE({quoted_date_column}, '{timezone}') as date"
    date_filter_expr = f"DATE({quoted_date_column}, '{timezone}')"

    select_parts = _select_parts_from_parsed_fields(
        parsed_fields=parsed_fields,
        datetime_expression=datetime_expression,
    )

    query = f"""
    SELECT
        {",".join(select_parts)}
    FROM {_quote_identifier(table_name)}
    WHERE {date_filter_expr} >= DATE('now', '{timezone}', '-{prior_days} days')
    GROUP BY {date_filter_expr}
    ORDER BY date
    """

    with closing(
        _connect_database(db_path, read_only=True, use_row_factory=True),
    ) as conn:
        cursor = conn.cursor().execute(query)
        return [dict(row) for row in cursor]


def query_hourly_aggregated_data(
    db_path: str,
    aggregation_fields: list[str],
    start_date: str,
    end_date: str | None = None,
    tz: str | None = None,
) -> HourlyAggregatedData:
    """Query SQLite database with dynamic aggregation fields for date range.

    Args:
        db_path: Path to SQLite database file
        aggregation_fields: List of aggregation specifications like ["avg_outHumi"]
        start_date: Start date to query (YYYY-MM-DD)
        end_date: End date to query (YYYY-MM-DD), defaults to today if None
        tz: Timezone string (e.g., 'America/New_York', '+05:30')

    Returns:
        Dict mapping date strings to 24 hourly slots.
        Each slot contains an aggregated result dict or None when that hour
        has no matching rows.

    """
    table_name: str = _DEFAULT_TABLE_NAME
    date_column: str = _TS_COL

    parsed_fields = _parse_aggregation_fields(aggregation_fields)
    timezone = _validate_timezone(tz)
    start_date_obj, end_date_obj = _normalize_hourly_date_range(
        start_date=start_date,
        end_date=end_date,
        timezone=timezone,
    )
    if isinstance(timezone, ZoneInfo):
        return _query_hourly_aggregated_data_with_zoneinfo(
            db_path=db_path,
            parsed_fields=parsed_fields,
            start_date=start_date_obj,
            end_date=end_date_obj,
            timezone=timezone,
        )

    quoted_date_column = _quote_identifier(date_column)
    datetime_expression = f"DATE({quoted_date_column}, '{timezone}') as date"
    hour_expression = f"strftime('%H', {quoted_date_column}, '{timezone}') as hour"
    date_filter_expr = f"DATE({quoted_date_column}, '{timezone}')"
    group_by_expr = f"strftime('%Y-%m-%d %H', {quoted_date_column}, '{timezone}')"

    select_parts = _select_parts_from_parsed_fields(
        parsed_fields=parsed_fields,
        datetime_expression=datetime_expression + ", " + hour_expression,
    )

    where_clause = f"WHERE {date_filter_expr} >= ? AND {date_filter_expr} <= ?"
    params = (start_date_obj.isoformat(), end_date_obj.isoformat())

    query = f"""
    SELECT
        {",".join(select_parts)}
    FROM {_quote_identifier(table_name)}
    {where_clause}
    GROUP BY {group_by_expr}
    ORDER BY date, hour
    """

    with closing(
        _connect_database(db_path, read_only=True, use_row_factory=True),
    ) as conn:
        cursor = conn.cursor().execute(query, params)
        result: HourlyAggregatedData = {
            date_key: _empty_hourly_slots()
            for date_key in _date_keys_in_range(start_date_obj, end_date_obj)
        }
        for row in cursor:
            row_dict: AggregatedRow = dict(row)
            date_key = row_dict.get("date")
            hour = row_dict.get("hour")
            if isinstance(date_key, str) and isinstance(hour, str):
                result[date_key][int(hour)] = row_dict
        return result
