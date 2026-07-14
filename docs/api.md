# HTTP JSON API

When a port is configured, the daemon starts an HTTP server in a background thread with CORS enabled (`Access-Control-Allow-Origin: *`). Server requests are logged to `<database_stem>_server.log`.

The server binds to `localhost` by default; set `host` in the config (or pass `--host`) to bind another address, e.g. `0.0.0.0` to serve the local network. When exposing the server, set `auth_token` in the config - every request must then send an `Authorization: Bearer <token>` header or it is rejected with `401`.

## Endpoints

| Endpoint | Description |
|----------|-------------|
| [`GET /`](#get-live-data) | Current readings fetched live from the station |
| [`GET /daily`](#get-daily-daily-aggregated-data) | Aggregates grouped by date |
| [`GET /hourly`](#get-hourly-hourly-aggregated-data) | Aggregates grouped by date and hour |
| [`GET /range`](#get-range-arbitrary-range-aggregation) | Aggregates over an arbitrary time window |
| [`GET /health`](#get-health-health-check) | Health check; `503` when collection has stalled |
| [`GET /metrics`](#get-metrics-database-metrics) | Database metrics as JSON or Prometheus text |

## `GET /` - Live Data

Returns current sensor readings fetched directly from the weather station, along with human-readable labels.

**Response:**

```json
{
  "data": {
    "outTemp": 75.5,
    "outHumi": 60.0,
    "windspeed": 3.2,
    "gustspeed": 8.1,
    "eventrain": 0.0
  },
  "metadata": {
    "labels": {
      "outTemp": "Outside Temperature",
      "outHumi": "Outside Humidity",
      "windspeed": "Wind Speed",
      "gustspeed": "Gust Speed",
      "eventrain": "Event Rain"
    }
  }
}
```

## `GET /daily` - Daily Aggregated Data

Returns aggregated sensor data grouped by date.

**Query Parameters:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `tz`      | Yes      | -       | Timezone (see [Timezone Support](#timezone-support)) |
| `q`       | Yes      | -       | Aggregation field(s), repeatable (see [Aggregation Fields](#aggregation-fields)) |
| `days`    | No       | `7`     | Number of prior days to include |

**Examples:**

```text
/daily?tz=America/New_York&q=avg_outHumi&days=7
/daily?tz=Europe/London&q=min_outTemp&q=sum_eventrain
```

**Response:**

```json
{
  "data": [
    {
      "date": "2025-06-26",
      "avg_outHumi": 62.3,
      "count": 1440
    },
    {
      "date": "2025-06-27",
      "avg_outHumi": 58.5,
      "count": 1440
    }
  ]
}
```

## `GET /hourly` - Hourly Aggregated Data

Returns aggregated sensor data grouped by date and wall-clock hour. Both
`start_date` and `end_date` are inclusive calendar dates. Each date contains
exactly 24 slots (indices 0-23), with `null` for hours that have no data.

**Query Parameters:**

| Parameter    | Required | Default | Description |
|--------------|----------|---------|-------------|
| `tz`         | Yes      | -       | Timezone (see [Timezone Support](#timezone-support)) |
| `q`          | Yes      | -       | Aggregation field(s), repeatable (see [Aggregation Fields](#aggregation-fields)) |
| `start_date` | Yes      | -       | Start date in `YYYY-MM-DD` format |
| `end_date`   | No       | today   | Inclusive end date in `YYYY-MM-DD` format |
| `date`       | -        | -       | Backward-compatible alias for `start_date` |

**Examples:**

```text
/hourly?start_date=2025-06-27&tz=America/Chicago&q=avg_outHumi
/hourly?start_date=2025-06-26&end_date=2025-06-27&tz=%2B05%3A30&q=max_gustspeed
/hourly?date=2025-06-27&tz=UTC&q=avg_outHumi
```

**Response (truncated for brevity):**

```json
{
  "data": {
    "2025-06-27": [
      {
        "date": "2025-06-27",
        "hour": "00",
        "avg_outHumi": 72.1,
        "count": 60
      },
      null,
      null
    ]
  }
}
```

The actual array always contains 24 entries, one per wall-clock hour. On an
IANA-timezone fall-back day, both physical occurrences of the repeated hour are
combined in the same slot and its `count` includes both. On a spring-forward
day, the skipped hour is `null`.

## `GET /range` - Arbitrary Range Aggregation

Returns aggregates over a single arbitrary `[start, end)` time window.

**Query Parameters:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `tz`      | Yes      | -       | Timezone used to interpret `start`/`end` (see [Timezone Support](#timezone-support)) |
| `q`       | Yes      | -       | Aggregation field(s), repeatable (see [Aggregation Fields](#aggregation-fields)) |
| `start`   | Yes      | -       | Window start: ISO date (`2026-06-22`) or datetime (`2026-06-22 06:00:00`) |
| `end`     | No       | now     | Window end (exclusive), same formats as `start` |

**Example:**

```text
/range?tz=America/New_York&q=avg_outTemp&q=max_gustspeed&start=2026-06-22&end=2026-06-29
```

**Response:**

```json
{
  "data": {
    "start": "2026-06-22 04:00:00",
    "end": "2026-06-29 04:00:00",
    "avg_outTemp": 71.2,
    "max_gustspeed": 18.3,
    "count": 10080
  }
}
```

`start` and `end` in the response are the resolved UTC bounds of the window.

## `GET /health` - Health Check

Returns the daemon health status, last observation timestamp, seconds since
that observation, and row count.

**Response:**

```json
{
  "status": "ok",
  "last_observation_ts": "2026-03-12 10:30:00",
  "age_seconds": 42.5,
  "row_count": 10000
}
```

If the most recent observation is older than 180 seconds (the same
threshold `status` uses for gap detection), `status` becomes `"stale"` and
the endpoint responds with HTTP `503` - so Docker `HEALTHCHECK`, Kubernetes
probes, and uptime monitors alert on stalled collection out of the box. An
empty database reports `"ok"` with `age_seconds: null`, so a freshly
deployed daemon passes its readiness probe before the first observation.

See [Recipes](recipes.md#uptime-monitoring) for container and Kubernetes probe configuration.

## `GET /metrics` - Database Metrics

Returns database summary statistics. The default response is JSON:

```json
{
  "row_count": 10000,
  "db_file_size_bytes": 1048576,
  "earliest_ts": "2025-01-01 00:00:00",
  "latest_ts": "2026-03-12 10:30:00",
  "column_count": 25
}
```

**Prometheus format:** with `?format=prometheus`, or when the `Accept` header
asks for `text/plain`/OpenMetrics (as Prometheus scrapers do), the endpoint
serves the Prometheus text exposition format instead - point a Prometheus
scrape job at `/metrics` and it works without configuration:

```text
# HELP aw2sqlite_observations_total Total number of observations recorded.
# TYPE aw2sqlite_observations_total counter
aw2sqlite_observations_total 10000
# HELP aw2sqlite_db_file_size_bytes Size of the SQLite database file.
# TYPE aw2sqlite_db_file_size_bytes gauge
aw2sqlite_db_file_size_bytes 1048576
# HELP aw2sqlite_last_observation_age_seconds Seconds since the most recent observation.
# TYPE aw2sqlite_last_observation_age_seconds gauge
aw2sqlite_last_observation_age_seconds 42.5
# HELP aw2sqlite_sensor_value Latest sensor readings by sensor name.
# TYPE aw2sqlite_sensor_value gauge
aw2sqlite_sensor_value{sensor="outTemp"} 72.5
```

See [Recipes](recipes.md#prometheus-and-grafana) for a ready-made scrape job.

## Aggregation Fields

Aggregation fields use the format `<function>_<column>`, where:

- **Functions:** `avg`, `max`, `min`, `sum` (case-insensitive)
- **Column:** Any physical sensor column name in the database (e.g. `outTemp`, `outHumi`, `gustspeed`, `eventrain`)

Ordinary station names remain unchanged. For a normalized or collision-encoded
custom name, find the corresponding physical column name in
`aw2sqlite_sensor_columns`, an export header, or the generated Datasette
metadata.

Multiple fields can be requested by repeating the `q` parameter. A `count` field is always included in the response indicating how many observations were aggregated.

## Timezone Support

The `tz` parameter is required for aggregation endpoints and accepts:

| Format | Example | Description |
|--------|---------|-------------|
| IANA timezone name | `America/New_York`, `Europe/London`, `UTC` | Full DST-aware conversion |
| UTC offset with colon | `+05:30`, `-08:00` | Fixed offset |
| UTC offset without colon | `+0530`, `-0800` | Fixed offset (HHMM interpreted) |
| Decimal offset | `+5.5`, `-8.0` | Fixed offset in hours |

URL-encode `+` as `%2B` when needed (e.g. `%2B05%3A30` for `+05:30`).

## Error Responses

All errors return JSON with an `error` field:

```json
{"error": "description of the error"}
```

| Status | Cause |
|--------|-------|
| 400    | Invalid input: bad timezone, date format, date range, aggregation field, or missing required parameters |
| 401    | Missing or wrong bearer token (only when `auth_token` is configured) |
| 404    | Unknown endpoint |
| 500    | Server error (e.g. weather station unreachable) |
| 503    | `/health` only: most recent observation is older than 180 seconds |
