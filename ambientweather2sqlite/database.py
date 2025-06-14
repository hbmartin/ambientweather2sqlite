import sqlite3
from pathlib import Path

from .exceptions import UnexpectedEmptyDictionaryError

_DEFAULT_TABLE_NAME = "observations"


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
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"  # noqa: S608
    cursor.execute(query, values)
    conn.commit()
    return cursor.lastrowid


def insert_observation(db_path: str, observation: dict[str, float | None]) -> None:
    with sqlite3.connect(db_path) as conn:
        ensure_columns(conn, set(observation.keys()))
        insert_dict_row(conn, _DEFAULT_TABLE_NAME, observation)
