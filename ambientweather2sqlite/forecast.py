import json
import sqlite3
from datetime import UTC, datetime

from . import mureq
from .exceptions import ForecastCooldownError

_HOURLY_FIELD_MAP = {
    "time": "forecast_time",
    "timestamp": "timestamp_ms",
    "temperature": "temperature",
    "feelsLike": "feels_like",
    "humidity": "humidity",
    "windSpeed": "wind_speed",
    "precipitation": "precipitation",
    "precipitationProbability": "precipitation_probability",
    "cloudCover": "cloud_cover",
    "condition": "condition",
    "icon": "icon",
    "pressure": "pressure",
}

_DAILY_FIELD_MAP = {
    "date": "forecast_date",
    "high": "high",
    "low": "low",
    "humidity": "humidity",
    "windSpeed": "wind_speed",
    "precipitation": "precipitation",
    "precipitationProbability": "precipitation_probability",
    "condition": "condition",
    "icon": "icon",
    "sunrise": "sunrise",
    "sunset": "sunset",
}


def get_last_fetch_time(db_path: str) -> datetime | None:
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        row = conn.execute(
            "SELECT MAX(fetched_at) FROM forecast_batches",
        ).fetchone()
        if row and row[0]:
            return datetime.fromisoformat(row[0]).replace(tzinfo=UTC)
    return None


def can_fetch(db_path: str, cooldown_hours: float = 23.0) -> bool:
    last_fetch = get_last_fetch_time(db_path)
    if last_fetch is None:
        return True
    elapsed = (datetime.now(UTC) - last_fetch).total_seconds() / 3600
    return elapsed >= cooldown_hours


def fetch_forecast(api_url: str) -> dict:
    response = mureq.get(api_url)
    return json.loads(response.body)


def _map_row(source: dict, field_map: dict) -> dict:
    result = {}
    for src_key, dst_key in field_map.items():
        if src_key in source:
            result[dst_key] = source[src_key]
    return result


def _insert_mapped_rows(
    conn: sqlite3.Connection,
    table: str,
    rows: list[dict],
) -> None:
    if not rows:
        return
    columns = list(rows[0].keys())
    placeholders = ", ".join(["?"] * len(columns))
    columns_str = ", ".join(columns)
    sql = (
        f"INSERT INTO {table} ({columns_str})"  # noqa: S608
        f" VALUES ({placeholders})"
    )
    conn.executemany(
        sql,
        [tuple(r[c] for c in columns) for r in rows],
    )


def store_forecast(db_path: str, forecast_data: dict) -> int:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            "INSERT INTO forecast_batches"
            " (location_name, lat, lon, country, timezone)"
            " VALUES (?, ?, ?, ?, ?)",
            (
                forecast_data.get("location", {}).get("name"),
                forecast_data.get("location", {}).get("lat"),
                forecast_data.get("location", {}).get("lon"),
                forecast_data.get("location", {}).get("country"),
                forecast_data.get("location", {}).get("timezone"),
            ),
        )
        batch_id = cursor.lastrowid

        for provider_name, provider_data in forecast_data.get("hourly", {}).items():
            rows = []
            for entry in provider_data:
                mapped = _map_row(entry, _HOURLY_FIELD_MAP)
                mapped["batch_id"] = batch_id
                mapped["provider"] = provider_name
                rows.append(mapped)
            _insert_mapped_rows(conn, "forecast_hourly", rows)

        for provider_name, provider_data in forecast_data.get("daily", {}).items():
            rows = []
            for entry in provider_data:
                mapped = _map_row(entry, _DAILY_FIELD_MAP)
                mapped["batch_id"] = batch_id
                mapped["provider"] = provider_name
                rows.append(mapped)
            _insert_mapped_rows(conn, "forecast_daily", rows)

        conn.commit()
        return batch_id


def fetch_and_store_forecast(
    api_url: str,
    db_path: str,
    *,
    force: bool = False,
) -> int:
    if not force and not can_fetch(db_path):
        last_fetch = get_last_fetch_time(db_path)
        raise ForecastCooldownError(str(last_fetch))
    forecast_data = fetch_forecast(api_url)
    return store_forecast(db_path, forecast_data)
