import re
import sqlite3
from pathlib import Path

from .exceptions import (
    InvalidColumnNameError,
    InvalidDateError,
    InvalidFormatError,
    InvalidPriorDaysError,
    MissingAggregationFieldsError,
    UnexpectedEmptyDictionaryError,
)

_DEFAULT_TABLE_NAME = "observations"
_TS_COL = "ts"


def _column_name(text: str) -> str:
    result = []
    for char in text:
        if char.isalnum() or char == "_":
            result.append(char)
        else:
            result.append("_")
    return "".join(result)


def ensure_columns(
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

    with sqlite3.connect(db_path) as conn:
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


def insert_dict_row(
    conn: sqlite3.Connection,
    table_name: str,
    data_dict: dict[str, float | None],
) -> int | None:
    """Alternative version that takes an existing connection.

    Args:
        conn (sqlite3.Connection): Existing database connection
        table_name (str): Name of the table to insert into
        data_dict (dict): Dictionary where keys are column names and values are the data

    Returns:
        int: The rowid of the inserted row

    Note:
        This version does not automatically commit. Call conn.commit() if needed.

    """
    if not data_dict:
        raise UnexpectedEmptyDictionaryError

    cursor = conn.cursor()

    # Extract column names and values
    columns = [_column_name(c) for c in list(data_dict.keys())]
    values = list(data_dict.values())

    # Create placeholders for the VALUES clause
    placeholders = ", ".join(["?" for _ in values])
    columns_str = ", ".join(columns)

    # Construct and execute the INSERT statement
    query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    cursor.execute(query, values)
    conn.commit()
    return cursor.lastrowid


def insert_observation(db_path: str, observation: dict[str, float | None]) -> None:
    with sqlite3.connect(db_path) as conn:
        ensure_columns(conn, set(observation.keys()))
        insert_dict_row(conn, _DEFAULT_TABLE_NAME, observation)


def _select_parts_from_aggregation_fields(
    aggregation_fields: list[str],
    datetime_expression: str,
) -> list[str]:
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

    # Build SELECT clause
    select_parts = [datetime_expression]

    select_parts.extend(
        f"{agg_func}({column_name}) as {alias}"
        for agg_func, column_name, alias in parsed_fields
    )

    select_parts.append("COUNT(*) as count")

    return select_parts


def query_daily_aggregated_data(
    db_path: str,
    aggregation_fields: list[str],
    prior_days: int = 7,
    table_name: str = _DEFAULT_TABLE_NAME,
    date_column: str = _TS_COL,
) -> dict[str, dict[str, float | int]]:
    """Query SQLite database with dynamic aggregation fields.

    Args:
        db_path: Path to SQLite database file
        aggregation_fields: List of aggregation specifications like ["avg_outHumi"]
        prior_days: Number of days to include in the query (not including today)
        table_name: Name of the table to query (default: "observations")
        date_column: Name of the timestamp column (default: "ts")

    Returns:
        Dict keyed by date string, with each value being a dict of aggregated values

    Example:
        {
            '2024-01-01': {
                'avg_outHumi': 65.5,
                'max_gustspeed': 25.3,
                'sum_eventrain': 2.1,
                'record_count': 144
            },
            '2024-01-02': {...}
        }

    """
    select_parts = _select_parts_from_aggregation_fields(
        aggregation_fields=aggregation_fields,
        datetime_expression=f"DATE({date_column}) as date",
    )

    if not isinstance(prior_days, int):
        raise InvalidPriorDaysError(prior_days)

    # Construct the full query
    query = f"""
    SELECT
        {','.join(select_parts)}
    FROM {table_name}
    WHERE DATE({date_column}) >= DATE('now', '-{prior_days} days')
    GROUP BY DATE({date_column})
    ORDER BY date
    """

    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        # Enable row factory to get column names
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute(query)

        # Convert to nested dict format
        result = {}
        for row in cursor:
            date_key = row["date"]
            row_dict = {key: row[key] for key in row.keys() if key != "date"}
            result[date_key] = row_dict

        return result


def query_hourly_aggregated_data(
    db_path: str,
    aggregation_fields: list[str],
    date: str,
    table_name: str = _DEFAULT_TABLE_NAME,
    date_column: str = _TS_COL,
) -> list[dict[str, float | int] | None]:
    """Query SQLite database with dynamic aggregation fields.

    Args:
        db_path: Path to SQLite database file
        aggregation_fields: List of aggregation specifications like ["avg_outHumi"]
        date: Date to query (YYYY-MM-DD)
        table_name: Name of the table to query (default: "observations")
        date_column: Name of the timestamp column (default: "ts")

    Returns:
        Dict keyed by date string, with each value being a dict of aggregated values

    """
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        raise InvalidDateError(date)

    select_parts = _select_parts_from_aggregation_fields(
        aggregation_fields=aggregation_fields,
        datetime_expression=f"strftime('%H', {date_column}) as hour",
    )

    # Construct the full query
    query = f"""
    SELECT
        {','.join(select_parts)}
    FROM {table_name}
    WHERE DATE({date_column}) = '{date}'
    GROUP BY strftime('%Y-%m-%d %H', {date_column})
    ORDER BY hour
    """

    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        # Enable row factory to get column names
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute(query)

        # Convert to nested dict format
        result: list[dict[str, float | int] | None] = [None for _ in range(24)]
        for row in cursor:
            result[int(row["hour"])] = dict(row)

        return result
