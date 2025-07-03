import re
import sqlite3
from datetime import datetime
from typing import cast
from zoneinfo import ZoneInfo

from .exceptions import (
    DatabaseNotInitializedError,
    InvalidColumnNameError,
    InvalidDateError,
    InvalidFormatError,
    InvalidPriorDaysError,
    InvalidTimezoneError,
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


def _validate_timezone(tz: str | None) -> str:
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
        if (offset := ZoneInfo(tz).utcoffset(datetime.now())) is not None:
            hours = offset.total_seconds() / 3600
            return f"{hours} hours"
    except (ModuleNotFoundError, ValueError, KeyError) as e:
        raise InvalidTimezoneError(tz) from e
    raise InvalidTimezoneError(tz)


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

    select_parts = [datetime_expression]

    select_parts.extend(
        f"{agg_func}({column_name}) as {alias}"
        for agg_func, column_name, alias in parsed_fields
    )

    select_parts.append("COUNT(*) as count")

    return select_parts


class DatabaseManager:
    """Manages persistent database connections and operations."""

    def __init__(self, db_path: str):
        self.conn: sqlite3.Connection = self._create_database_if_not_exists(db_path)
        self.conn.row_factory = sqlite3.Row
        self._ensure_logs_table()

    def _ensure_logs_table(self) -> None:
        """Create logs table if it doesn't exist."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                error TEXT,
                message TEXT
            )
        """,
        )
        self.conn.commit()

    def log_error(self, error_name: str, message: str) -> None:
        """Log an error to the database."""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO logs (error, message) VALUES (?, ?)",
            (error_name, message),
        )
        self.conn.commit()

    def close(self) -> None:
        """Close database connection."""
        self.conn.close()

    def _ensure_columns(
        self,
        required_columns: set[str],
        table_name: str = _DEFAULT_TABLE_NAME,
    ) -> list[str]:
        """Checks if a table has columns for every string in required_columns.
        If not, adds the missing columns with REAL type.

        Args:
            required_columns (set): Set of column names that should exist
            table_name (str): Name of the table to check/modify

        Returns:
            list: List of column names that were added

        Raises:
            sqlite3.Error: If there's a database error

        """
        added_columns = []

        cursor = self.conn.cursor()

        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_columns = {
            row[1] for row in cursor.fetchall()
        }  # row[1] is column name

        missing_columns = required_columns - existing_columns

        for column_name in missing_columns:
            valid_column_name = _column_name(column_name)
            cursor.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {valid_column_name} REAL",
            )
            added_columns.append(column_name)

        cursor.close()
        self.conn.commit()

        return added_columns

    def _create_database_if_not_exists(
        self,
        db_path: str,
        table_name: str = _DEFAULT_TABLE_NAME,
    ) -> sqlite3.Connection:
        """Check if a SQLite database exists at the specified path.
        If not, create the database and a table with the given name.

        Args:
            db_path (str): Path to the SQLite database file
            table_name (str): Name of the table to create

        Returns:
            sqlite3.Connection: Connection to the SQLite database

        """
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        table_schema = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {_TS_COL} TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """

        cursor.execute(table_schema)
        conn.commit()

        return conn

    def insert_dict_row(
        self,
        table_name: str,
        data_dict: dict[str, float | None],
    ) -> int | None:
        """Alternative version that takes an existing connection.

        Args:
            table_name (str): Name of the table to insert into
            data_dict (dict): Dictionary of keys as column names and values as data

        Returns:
            int: The rowid of the inserted row

        """
        if not data_dict:
            raise UnexpectedEmptyDictionaryError

        cursor = self.conn.cursor()

        columns = [_column_name(c) for c in list(data_dict.keys())]
        values = list(data_dict.values())

        placeholders = ", ".join(["?" for _ in values])
        columns_str = ", ".join(columns)

        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        cursor.execute(query, values)
        self.conn.commit()
        return cursor.lastrowid

    def insert_observation(self, observation: dict[str, float | None]) -> None:
        """Insert an observation into the database."""
        self._ensure_columns(set(observation.keys()))
        self.insert_dict_row(_DEFAULT_TABLE_NAME, observation)

    def query_daily_aggregated_data(
        self,
        aggregation_fields: list[str],
        prior_days: int = 7,
        tz: str | None = None,
    ) -> list[dict[str, float | int | str]]:
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

        timezone = _validate_timezone(tz)

        table_name: str = _DEFAULT_TABLE_NAME
        date_column: str = _TS_COL

        datetime_expression = f"DATE({date_column}, '{timezone}') as date"
        date_filter_expr = f"DATE({date_column}, '{timezone}')"

        select_parts = _select_parts_from_aggregation_fields(
            aggregation_fields=aggregation_fields,
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

        cursor = self.conn.cursor()
        cursor.execute(query)
        return [dict(row) for row in cursor]

    def query_hourly_aggregated_data(
        self,
        aggregation_fields: list[str],
        date: str,
        tz: str | None = None,
    ) -> list[dict[str, float | int | str] | None]:
        """Query SQLite database with dynamic aggregation fields.

        Args:
            db_path: Path to SQLite database file
            aggregation_fields: List of aggregation specifications like ["avg_outHumi"]
            date: Date to query (YYYY-MM-DD)
            tz: Timezone string (e.g., 'America/New_York', '+05:30')

        Returns:
            Sorted list of dicts of aggregates or None for each hour

        """
        table_name: str = _DEFAULT_TABLE_NAME
        date_column: str = _TS_COL

        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
            raise InvalidDateError(date)

        timezone = _validate_timezone(tz)

        datetime_expression = f"strftime('%H', {date_column}, '{timezone}') as hour"
        date_filter_expr = f"DATE({date_column}, '{timezone}')"
        group_by_expr = f"strftime('%Y-%m-%d %H', {date_column}, '{timezone}')"

        select_parts = _select_parts_from_aggregation_fields(
            aggregation_fields=aggregation_fields,
            datetime_expression=datetime_expression,
        )

        query = f"""
        SELECT
            {','.join(select_parts)}
        FROM {table_name}
        WHERE {date_filter_expr} = '{date}'
        GROUP BY {group_by_expr}
        ORDER BY hour
        """

        cursor = self.conn.cursor()
        cursor.execute(query)

        result: list[dict[str, float | int | str] | None] = [None for _ in range(24)]
        for row in cursor:
            result[int(row["hour"])] = dict(row)

        return result


# Global database manager instance
db_manager: DatabaseManager | None = None


def get_db_manager(db_path: str | None = None) -> DatabaseManager:
    """Get the global database manager instance."""
    global db_manager  # noqa: PLW0603
    if db_manager is None:
        if db_path is None:
            raise DatabaseNotInitializedError
        db_manager = DatabaseManager(db_path)

    return cast("DatabaseManager", db_manager)
