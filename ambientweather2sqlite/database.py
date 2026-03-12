import re
import sqlite3
from collections.abc import Mapping
from contextlib import closing
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from .exceptions import (
    InvalidColumnNameError,
    InvalidDateError,
    InvalidDateRangeError,
    InvalidFormatError,
    InvalidPriorDaysError,
    InvalidTimezoneError,
    MissingAggregationFieldsError,
    UnexpectedEmptyDictionaryError,
)

_DEFAULT_TABLE_NAME = "observations"
_TS_COL = "ts"
_SQLITE_BUSY_TIMEOUT_MS = 5_000
_SQLITE_MMAP_SIZE_BYTES = 268_435_456
type AggregationField = tuple[str, str, str]
type AggregatedRow = dict[str, str | int | float | None]
type HourlyAggregatedData = dict[str, list[AggregatedRow | None]]
type ObservationValue = str | int | float | None
type Observation = Mapping[str, ObservationValue]


def _column_name(text: str) -> str:
    result = []
    for char in text:
        if char.isalnum() or char == "_":
            result.append(char)
        else:
            result.append("_")
    return "".join(result)


def _validate_timezone(tz: str | None) -> str | ZoneInfo:
    if not tz or tz == "localtime":
        return "localtime"

    try:
        if ":" in tz:
            hours, minutes = map(int, tz.split(":"))
            offset_hours = (
                hours + (minutes / 60) if hours >= 0 else hours - (minutes / 60)
            )
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
        f"{agg_func}({column_name}) as {alias}"
        for agg_func, column_name, alias in parsed_fields
    )

    select_parts.append("COUNT(*) as count")

    return select_parts


def _select_parts_from_aggregation_fields(
    aggregation_fields: list[str],
    datetime_expression: str,
) -> list[str]:
    return _select_parts_from_parsed_fields(
        _parse_aggregation_fields(aggregation_fields),
        datetime_expression,
    )


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
    end_date_obj = (
        _parse_query_date(end_date)
        if end_date is not None
        else _current_date_for_timezone(timezone)
    )

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


def _configure_connection(
    conn: sqlite3.Connection,
    *,
    read_only: bool,
    use_row_factory: bool = False,
) -> sqlite3.Connection:
    conn.execute(f"PRAGMA busy_timeout = {_SQLITE_BUSY_TIMEOUT_MS}")
    if use_row_factory:
        conn.row_factory = sqlite3.Row
    if not read_only:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute(f"PRAGMA mmap_size={_SQLITE_MMAP_SIZE_BYTES}")
    return conn


def _connect_database(
    db_path: str,
    *,
    read_only: bool,
    use_row_factory: bool = False,
) -> sqlite3.Connection:
    connect_target = f"file:{db_path}?mode=ro" if read_only else db_path
    conn = sqlite3.connect(
        connect_target,
        uri=read_only,
        timeout=_SQLITE_BUSY_TIMEOUT_MS / 1000,
    )
    return _configure_connection(
        conn,
        read_only=read_only,
        use_row_factory=use_row_factory,
    )


def _format_sqlite_timestamp(value: datetime) -> str:
    """Store timestamps as UTC naive strings in SQLite's text format.

    Callers should pass timezone-aware datetimes because the value is converted to
    UTC before its tzinfo is stripped for storage.
    """
    return value.astimezone(UTC).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _parse_stored_timestamp(value: str) -> datetime:
    """Parse stored timestamps and normalize them to UTC-aware datetimes.

    Naive timestamps are assumed to have been stored in UTC. Aware timestamps are
    converted to UTC before being returned.
    """
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


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
    select_columns = ", ".join([_TS_COL, *sorted(columns)])
    query = (
        f"SELECT {select_columns} FROM {_DEFAULT_TABLE_NAME} "
        f"WHERE {_TS_COL} >= ? AND {_TS_COL} < ? ORDER BY {_TS_COL}"
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


def _ensure_columns(
    conn: sqlite3.Connection,
    required_columns: set[str],
    table_name: str = _DEFAULT_TABLE_NAME,
) -> list[str]:
    """Checks if a table has columns for every string in required_columns.
    If not, adds the missing columns with REAL type.

    Args:
        conn (sqlite3.Connection): Connection to the SQLite database
        required_columns (set): Set of column names that should exist
        table_name (str): Name of the table to check/modify

    Returns:
        list: List of column names that were added

    Raises:
        sqlite3.Error: If there's a database error

    """
    added_columns = []

    cursor = conn.cursor()

    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}  # row[1] is column name

    missing_columns = required_columns - existing_columns

    for column_name in missing_columns:
        valid_column_name = _column_name(column_name)
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {valid_column_name} REAL")
        added_columns.append(column_name)

    cursor.close()
    conn.commit()

    return added_columns


def create_database_if_not_exists(
    db_path: str,
    table_name: str = _DEFAULT_TABLE_NAME,
) -> bool:
    """Check if a SQLite database exists at the specified path.
    If not, create the database and a table with the given name.

    Args:
        db_path (str): Path to the SQLite database file
        table_name (str): Name of the table to create

    Returns:
        bool: True if database was created, False if it already existed

    """
    if Path(db_path).exists():
        return False

    with closing(_connect_database(db_path, read_only=False)) as conn:
        cursor = conn.cursor()

        table_schema = f"""
            CREATE TABLE {table_name} (
                {_TS_COL} TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """

        cursor.execute(table_schema)
        conn.commit()

        print(f"Database created with table '{table_name}' at: {db_path}")
        return True


def _insert_dict_row(
    conn: sqlite3.Connection,
    table_name: str,
    data_dict: Observation,
) -> int | None:
    """Alternative version that takes an existing connection.

    Args:
        conn (sqlite3.Connection): Existing database connection
        table_name (str): Name of the table to insert into
        data_dict (dict): Dictionary where keys are column names and values are the data

    Returns:
        int: The rowid of the inserted row

    """
    if not data_dict:
        raise UnexpectedEmptyDictionaryError

    cursor = conn.cursor()

    columns = [_column_name(c) for c in list(data_dict.keys())]
    values = list(data_dict.values())

    placeholders = ", ".join(["?" for _ in values])
    columns_str = ", ".join(columns)

    query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    cursor.execute(query, values)
    conn.commit()
    return cursor.lastrowid


def insert_observation(
    db_path: str,
    observation: Observation,
) -> None:
    with closing(_connect_database(db_path, read_only=False)) as conn:
        _ensure_columns(conn, set(observation.keys()))
        _insert_dict_row(conn, _DEFAULT_TABLE_NAME, observation)


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

    datetime_expression = f"DATE({date_column}, '{timezone}') as date"
    date_filter_expr = f"DATE({date_column}, '{timezone}')"

    select_parts = _select_parts_from_parsed_fields(
        parsed_fields=parsed_fields,
        datetime_expression=datetime_expression,
    )

    query = f"""
    SELECT
        {','.join(select_parts)}
    FROM {table_name}
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

    datetime_expression = f"DATE({date_column}, '{timezone}') as date"
    hour_expression = f"strftime('%H', {date_column}, '{timezone}') as hour"
    date_filter_expr = f"DATE({date_column}, '{timezone}')"
    group_by_expr = f"strftime('%Y-%m-%d %H', {date_column}, '{timezone}')"

    select_parts = _select_parts_from_parsed_fields(
        parsed_fields=parsed_fields,
        datetime_expression=datetime_expression + ", " + hour_expression,
    )

    where_clause = f"WHERE {date_filter_expr} >= ? AND {date_filter_expr} <= ?"
    params = (start_date_obj.isoformat(), end_date_obj.isoformat())

    query = f"""
    SELECT
        {','.join(select_parts)}
    FROM {table_name}
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
