# AmbientWeather to SQLite

[![PyPI](https://img.shields.io/pypi/v/ambientweather2sqlite.svg)](https://pypi.org/project/ambientweather2sqlite/)
[![Lint](https://github.com/hbmartin/ambientweather2sqlite/actions/workflows/lint.yml/badge.svg)](https://github.com/hbmartin/ambientweather2sqlite/actions/workflows/lint.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![codecov](https://codecov.io/gh/hbmartin/ambientweather2sqlite/graph/badge.svg?token=3gZI67ci1N)](https://codecov.io/gh/hbmartin/ambientweather2sqlite)
[![Checked with pyrefly](https://img.shields.io/badge/🪲-pyrefly-fe8801.svg)](https://pyrefly.org/)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/hbmartin/ambientweather2sqlite)

A project to record minute-by-minute weather observations from an AmbientWeather station over the local network - no API needed!

## Key Features

* **Local Network Operation:** Direct connection to weather stations without external API dependencies
* **Auto-Discovery:** Network scanner to automatically find weather stations on your subnet
* **Continuous Data Collection:** Automated daemon process collecting data at 60-second intervals
* **Dynamic Schema Management:** Automatic database schema evolution as new sensors are detected
* **Observation Deduplication:** UNIQUE index on timestamps prevents duplicate rows on daemon restart
* **Data Validation:** Warning logs emitted for implausible sensor readings (e.g. temperature of 999)
* **HTTP JSON API:** Optional web server with live data, aggregation, health check, and metrics endpoints
* **Datasette Compatible:** Metadata JSON follows the Datasette metadata format with labels, units, and licensing
* **Structured Logging:** Optional JSONL log format for log aggregator ingestion
* **Interactive Configuration:** Command-line setup wizard with optional network auto-scan
* **Gap Detection:** `status` reports periods with missing observations, including ongoing outages
* **Data Export:** Dump observations as CSV or JSON, with optional time bounds
* **Prometheus Metrics:** `/metrics` serves the Prometheus text format for Grafana dashboards
* **MQTT Publishing:** Optional per-observation publish to an MQTT broker (e.g. Home Assistant)
* **Webhook Alerts:** Optional webhook notification when the station goes offline or recovers
* **API Authentication:** Optional bearer token and configurable bind address for the HTTP server
* **macOS & Linux Services:** Generate launchd plists or systemd user units for running as a background daemon
* **Docker Support:** Dockerfile and compose example included
* **Zero Dependencies:** Pure Python with no (potentially) untrusted 3rd parties

## Installation

If you have uv installed, you can run it directly:

```bash
uvx ambientweather2sqlite
```

Or install it with curl:

```bash
curl -LsSf uvx.sh/ambientweather2sqlite/install.sh | sh
```

Requires Python 3.13+.

### Docker

A `Dockerfile` and `docker-compose.yml` are included. All state lives in a
`/data` volume; create the config once with the interactive wizard, then start
the service:

```bash
mkdir -p data
docker compose run --rm aw2sqlite config --config /data/aw2sqlite.toml
docker compose up -d
```

The container runs as UID/GID `1001`; if you bind-mount `./data`, make sure that
directory is writable by that user. If your data was created by an older image
that ran as root (this applies to named volumes too — Docker only copies
ownership into a volume when it is first created empty), fix it once with:

```bash
docker compose run --rm --user root --entrypoint chown aw2sqlite -R 1001:1001 /data
```

The container starts the server bound to `0.0.0.0` so the published port is
reachable; set `auth_token` in the config if others can reach that port.

## CLI

The tool uses a subcommand-based CLI. An `aw2sqlite` alias is also available.

```bash
aw2sqlite <command> [options]
```

### Commands

| Command | Description |
|---------|-------------|
| `serve` | Start the daemon and optional API server (default if no command given) |
| `config` | Run the interactive configuration wizard |
| `once` | Fetch a single observation and print it as JSON (no DB write) |
| `status` | Show database metrics (row count, file size, timestamp range) and observation gaps |
| `export` | Export observations as CSV or JSON |
| `backup` | Write a compacted backup copy of the database (safe while the daemon is running) |
| `install-launchd` | Generate a macOS launchd plist for running as a service |
| `install-systemd` | Generate a systemd user unit for running as a service (Linux) |

### `aw2sqlite serve`

```bash
aw2sqlite serve [--port PORT] [--host HOST] [--config CONFIG_PATH] [--log-format {text,json}]
```

| Flag | Type | Default | Description |
|----------|------|---------|-------------|
| `--port PORT` | Integer | Config file value, or disabled | Port number for the HTTP JSON API server |
| `--host HOST` | String | Config file value, or `localhost` | Bind address for the HTTP JSON API server |
| `--config CONFIG_PATH` | String | `./aw2sqlite.toml`, then `~/.aw2sqlite.toml` | Path to a TOML config file |
| `--log-format` | `text` or `json` | `text` | Log output format. `json` outputs single-line JSONL for log aggregators |

For backward compatibility, `aw2sqlite --port 8080` (without a subcommand) is equivalent to `aw2sqlite serve --port 8080`.

### `aw2sqlite config`

```bash
aw2sqlite config [--config CONFIG_PATH]
```

Runs the interactive setup wizard. On start, you are offered an automatic network scan that:

1. Detects your local /24 subnet
2. Scans for devices with port 80 open
3. Probes each device for a weather station's `livedata.htm` page

If the scan finds no stations, you can retry or enter the URL manually. If multiple stations are found, you choose which one to use.

### `aw2sqlite once`

```bash
aw2sqlite once [--config CONFIG_PATH]
```

Fetches a single observation from the weather station and prints it as JSON to stdout. Useful for verifying the station URL is correct during setup. Does not write to the database.

### `aw2sqlite status`

```bash
aw2sqlite status [--config CONFIG_PATH] [--gap-hours HOURS] [--gap-threshold SECONDS]
```

Prints database metrics as JSON, including any observation gaps found in the
scan window (default: gaps longer than 180 seconds within the last 24 hours).
A gap with `"end": null` is ongoing - the collector has not recorded anything
since `start`.

```json
{
  "row_count": 10000,
  "db_file_size_bytes": 1048576,
  "earliest_ts": "2025-01-01 00:00:00",
  "latest_ts": "2026-03-12 10:30:00",
  "column_count": 25,
  "gaps": [
    {
      "start": "2026-03-12 04:12:00",
      "end": "2026-03-12 06:30:00",
      "duration_seconds": 8280.0
    }
  ]
}
```

| Flag | Default | Description |
|------|---------|-------------|
| `--gap-hours` | `24` | Window (in hours) to scan for gaps |
| `--gap-threshold` | `180` | Minimum seconds between observations to count as a gap |

### `aw2sqlite export`

```bash
aw2sqlite export [--format {csv,json}] [--start TS] [--end TS] [--output FILE] [--config CONFIG_PATH]
```

Exports the observations table, oldest first, as CSV (default) or JSON. Output
goes to stdout unless `--output` is given. `--start` (inclusive) and `--end`
(exclusive) accept UTC timestamps like `2026-01-01` or `"2026-01-01 12:00:00"`
and are compared against the stored `ts` column.

```bash
aw2sqlite export --format csv --start 2026-01-01 --end 2026-02-01 --output january.csv
```

### `aw2sqlite backup`

```bash
aw2sqlite backup DESTINATION [--config CONFIG_PATH]
```

Writes a compacted, self-contained copy of the database to `DESTINATION`
using SQLite's `VACUUM INTO`. This is safe while the daemon is running (a
raw file copy of a WAL database is not) and the destination must not
already exist.

```bash
aw2sqlite backup /backups/aw2sqlite-$(date +%F).db
```

### `aw2sqlite install-launchd`

```bash
aw2sqlite install-launchd [--config CONFIG_PATH]
```

Generates a macOS launchd plist file at `~/Library/LaunchAgents/com.ambientweather2sqlite.plist` and prints `launchctl` instructions to load/unload the service.

### `aw2sqlite install-systemd`

```bash
aw2sqlite install-systemd [--config CONFIG_PATH]
```

Generates a systemd user unit at `~/.config/systemd/user/ambientweather2sqlite.service` and prints `systemctl --user` instructions to enable/disable the service. Use `loginctl enable-linger $USER` to keep it running after logout.

## Setup

On the first run, if no config file is found, you will be guided through an interactive setup wizard that prompts for:

1. **Network auto-scan** or **manual URL entry** for the weather station
2. **Database Path** - defaults to `./aw2sqlite.db`
3. **Server Port** - for the JSON API server (leave blank to disable)
4. **Output TOML Filename** - defaults to `./aw2sqlite.toml`

### Config File

The generated config file is a TOML file:

```toml
live_data_url = "http://192.168.0.226/livedata.htm"
database_path = "/path/to/aw2sqlite.db"
port = 8080         # optional, omit to disable the JSON server
host = "localhost"  # optional bind address; use "0.0.0.0" to expose on the network
auth_token = "s3cret" # optional; require "Authorization: Bearer s3cret" on API requests
log_format = "text" # optional, "text" (default) or "json" for JSONL logs

# Optional: POST a webhook when the station goes offline / comes back
[alerts]
webhook_url = "https://hooks.example.com/alert"
failure_threshold = 3  # optional; consecutive failed fetches before alerting
timeout_seconds = 5.0  # optional; webhook request timeout

# Optional: publish each observation as JSON to an MQTT broker
[mqtt]
host = "192.168.0.10"
topic = "weather/observations"
port = 1883             # optional (default: 1883)
username = "user"       # optional
password = "pass"       # optional
client_id = "aw2sqlite" # optional
tls = false             # optional (default: false)
```

Config file lookup order:
1. Path provided via `--config`
2. `./aw2sqlite.toml` in the current directory
3. `~/.aw2sqlite.toml` in the home directory

The config is validated at load: `live_data_url` must start with `http://`
or `https://`, and `port` values (top-level and `[mqtt]`) must be in
1-65535. Invalid configs make the CLI exit with a one-line error.

## Data Collection

The daemon continuously fetches live data from your weather station's HTTP endpoint, parses sensor readings from the HTML page, and inserts them into the SQLite database every 60 seconds.

- Current readings are displayed in the terminal as labeled JSON
- Errors (timeouts, HTTP failures) are logged to `<database_stem>_daemon.log` and the daemon continues running
- Implausible sensor values (e.g. temperature outside -150..200 range) emit a warning log
- If a fetch parses zero sensors (e.g. the station page format changed), a warning is logged and nothing is inserted for that interval
- Duplicate observations (same timestamp) are silently ignored via `INSERT OR IGNORE`
- Samples are scheduled against a monotonic clock so intervals do not drift with fetch latency
- When an `[mqtt]` config table is present, each observation is also published to the broker as JSON (failures are logged and never interrupt collection)
- When an `[alerts]` config table is present, a `station_offline` webhook fires after `failure_threshold` consecutive failed fetches (default 3) and a `station_online` webhook fires on the first success afterwards - one alert per outage, and webhook errors never interrupt collection. The POST body is JSON: `{"event": "station_offline", "station_url": "...", "consecutive_failures": 3, "timestamp": "2026-07-11T12:00:00+00:00"}`
- A metadata file (`<database_stem>_metadata.json`) is generated in [Datasette metadata format](https://docs.datasette.io/en/stable/metadata.html) with human-readable sensor labels, units, and licensing (compatible with the [datasette-pint](https://github.com/simonw/datasette-pint) plugin)
- Press `Ctrl+C` to stop; `SIGTERM` (used by launchd/systemd/Docker) triggers the same graceful shutdown

### Database Schema

The `observations` table is created with a single `ts` (TIMESTAMP) column and a UNIQUE index on `ts` for deduplication. Sensor columns are added dynamically as `REAL` columns when new data fields are encountered.

SQLite is configured with WAL journal mode, normal synchronous writes, in-memory temp storage, and 256MB memory-mapped I/O.

## HTTP JSON API

When a port is configured, the daemon starts an HTTP server in a background thread with CORS enabled (`Access-Control-Allow-Origin: *`). Server requests are logged to `<database_stem>_server.log`.

The server binds to `localhost` by default; set `host` in the config (or pass `--host`) to bind another address, e.g. `0.0.0.0` to serve the local network. When exposing the server, set `auth_token` in the config - every request must then send an `Authorization: Bearer <token>` header or it is rejected with `401`.

### `GET /` - Live Data

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

### `GET /daily` - Daily Aggregated Data

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

### `GET /hourly` - Hourly Aggregated Data

Returns aggregated sensor data grouped by date and hour. Each date contains exactly 24 slots (indices 0-23), with `null` for hours that have no data.

**Query Parameters:**

| Parameter    | Required | Default | Description |
|--------------|----------|---------|-------------|
| `tz`         | Yes      | -       | Timezone (see [Timezone Support](#timezone-support)) |
| `q`          | Yes      | -       | Aggregation field(s), repeatable (see [Aggregation Fields](#aggregation-fields)) |
| `start_date` | Yes      | -       | Start date in `YYYY-MM-DD` format |
| `end_date`   | No       | today   | End date in `YYYY-MM-DD` format |
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

The actual array always contains 24 entries, one per hour.

### `GET /range` - Arbitrary Range Aggregation

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

### `GET /health` - Health Check

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

### `GET /metrics` - Database Metrics

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

### Aggregation Fields

Aggregation fields use the format `<function>_<column>`, where:

- **Functions:** `avg`, `max`, `min`, `sum` (case-insensitive)
- **Column:** Any sensor column name in the database (e.g. `outTemp`, `outHumi`, `gustspeed`, `eventrain`)

Multiple fields can be requested by repeating the `q` parameter. A `count` field is always included in the response indicating how many observations were aggregated.

### Timezone Support

The `tz` parameter is required for aggregation endpoints and accepts:

| Format | Example | Description |
|--------|---------|-------------|
| IANA timezone name | `America/New_York`, `Europe/London`, `UTC` | Full DST-aware conversion |
| UTC offset with colon | `+05:30`, `-08:00` | Fixed offset |
| UTC offset without colon | `+0530`, `-0800` | Fixed offset (HHMM interpreted) |
| Decimal offset | `+5.5`, `-8.0` | Fixed offset in hours |

URL-encode `+` as `%2B` when needed (e.g. `%2B05%3A30` for `+05:30`).

### Error Responses

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

## Development

Pull requests and issue reports are welcome. For major changes, please open an issue first to discuss what you would like to change.

### Core Architecture
<img src="media/arch.svg" />

### Control Flow
<img src="media/flow.svg" />

## Legal

&copy; [Harold Martin](https://www.linkedin.com/in/harold-martin-98526971/) - released under [GPLv3](LICENSE.md)

AmbientWeather is a trademark of Ambient, LLC.
