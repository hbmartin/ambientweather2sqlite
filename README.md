# AmbientWeather to SQLite

[![PyPI](https://img.shields.io/pypi/v/ambientweather2sqlite.svg)](https://pypi.org/project/ambientweather2sqlite/)
[![Python](https://img.shields.io/pypi/pyversions/ambientweather2sqlite.svg)](https://pypi.org/project/ambientweather2sqlite/)
[![License](https://img.shields.io/pypi/l/ambientweather2sqlite.svg)](LICENSE.md)
[![Lint](https://github.com/hbmartin/ambientweather2sqlite/actions/workflows/lint.yml/badge.svg)](https://github.com/hbmartin/ambientweather2sqlite/actions/workflows/lint.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![codecov](https://codecov.io/gh/hbmartin/ambientweather2sqlite/graph/badge.svg?token=3gZI67ci1N)](https://codecov.io/gh/hbmartin/ambientweather2sqlite)
[![Checked with pyrefly](https://img.shields.io/badge/🪲-pyrefly-fe8801.svg)](https://pyrefly.org/)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/hbmartin/ambientweather2sqlite)

Record minute-by-minute weather observations from an AmbientWeather station over your local network - no cloud account, no API key, no dependencies.

## Quick Start

```bash
uvx ambientweather2sqlite config   # scan the network, find your station, write a config
uvx ambientweather2sqlite serve    # collect every 60s into SQLite, serve the JSON API
```

The wizard scans your subnet for the station, so for most setups those two commands are all there is to it. To check that the station is reachable without touching the database:

```bash
uvx ambientweather2sqlite once     # print one live observation as JSON
```

Requires Python 3.13+. **[Full documentation →](https://hbmartin.github.io/ambientweather2sqlite/)**

## Contents

- [Why this exists](#why-this-exists)
- [Features](#features)
- [Installation](#installation)
- [CLI](#cli)
- [Configuration](#configuration)
- [Data Collection](#data-collection)
- [HTTP JSON API](#http-json-api)
- [Recipes](#recipes)
- [Development](#development)
- [Legal](#legal)

## Why this exists

Your station already serves its readings on your own network. This reads them straight off the device, so collection keeps working when the vendor's cloud is down, nothing is rate-limited, no account is involved, and your data never leaves the house.

## Features

* **Local Network Operation:** Talks directly to the station's `livedata.htm` - no cloud API, no API key, no account
* **Zero Dependencies:** Pure Python with no (potentially) untrusted 3rd parties
* **Auto-Discovery:** The setup wizard scans your subnet and finds the station for you
* **A Database That Keeps Up With Your Hardware:** Sensor columns are added automatically as new readings appear, timestamps are deduplicated across restarts, stale console pages are skipped so an unchanged reading never becomes a fake per-minute sample, and implausible values are flagged
* **Plugs Into What You Already Run:** Home Assistant (MQTT discovery), Prometheus and Grafana (`/metrics`), and Datasette (generated metadata) - see [Recipes](docs/recipes.md)
* **Runs As A Service:** Docker, launchd, and systemd units included, with an [HTTP JSON API](docs/api.md) for live and aggregated data

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

The included `docker-compose.yml` caps Docker's captured stdout/stderr with a
`json-file` logging driver (`max-size: 10m`, `max-file: 3`); without a limit the
default driver keeps container logs forever. The daemon also detects that its
stdout is not a terminal and prints one concise line per cycle instead of the
interactive countdown, so the captured stream stays small.

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
| `migrate` | Inspect or explicitly apply a versioned, automatically backed-up database migration |
| `metadata` | Refresh the Datasette sidecar, or regenerate it offline from the database |
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
already exist. Sensor-name mappings, labels, and units are stored inside the
database and are included in the backup.

```bash
aw2sqlite backup /backups/aw2sqlite-$(date +%F).db
```

### `aw2sqlite migrate`

```bash
aw2sqlite migrate [--check | --backup PATH] [--config CONFIG_PATH]
```

Legacy application schemas are never changed as a side effect of `serve`.
Inspect the plan first, then apply it explicitly:

```bash
aw2sqlite migrate --check
aw2sqlite migrate
```

Applying a migration first creates a timestamped compact backup beside the
database; `--backup` chooses a different destination. If duplicate legacy
timestamps must be merged, all source rows are also retained in the
`aw2sqlite_migration_conflicts` audit table.

### `aw2sqlite metadata`

```bash
aw2sqlite metadata [--offline] [--config CONFIG_PATH]
```

Without `--offline`, refreshes labels and units from the station. With
`--offline`, recreates `<database_stem>_metadata.json` entirely from metadata
embedded in the database—useful after restoring a backup. If the database has
no stored sensor metadata, the command exits with status 1 and leaves any
existing sidecar unchanged.

### `aw2sqlite install-launchd`

```bash
aw2sqlite install-launchd [--config CONFIG_PATH]
```

Generates a macOS launchd plist file at `~/Library/LaunchAgents/com.ambientweather2sqlite.plist` and prints `launchctl` instructions to load/unload the service.

launchd captures the daemon's stdout/stderr to `~/Library/Logs/ambientweather2sqlite.stdout.log` and `.stderr.log`. Because stdout is not a terminal there, the daemon emits only one concise line per cycle, so these files grow slowly - but launchd itself never rotates them. For a long-running install, add a `newsyslog` rule to cap them, e.g. in `/etc/newsyslog.d/ambientweather2sqlite.conf`:

```text
# logfilename                                        [owner:group]  mode count size when flags
/Users/YOU/Library/Logs/ambientweather2sqlite.stdout.log            644  4     5120 *    J
/Users/YOU/Library/Logs/ambientweather2sqlite.stderr.log            644  4     5120 *    J
```

### `aw2sqlite install-systemd`

```bash
aw2sqlite install-systemd [--config CONFIG_PATH]
```

Generates a systemd user unit at `~/.config/systemd/user/ambientweather2sqlite.service` and prints `systemctl --user` instructions to enable/disable the service. Use `loginctl enable-linger $USER` to keep it running after logout.

## Configuration

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
discovery = false       # optional; publish Home Assistant discovery configs
discovery_prefix = "homeassistant" # optional
```

Config file lookup order:
1. Path provided via `--config`
2. `./aw2sqlite.toml` in the current directory
3. `~/.aw2sqlite.toml` in the home directory

The config is validated at load: `live_data_url` must start with `http://`
or `https://`, and `port` values (top-level and `[mqtt]`) must be in
1-65535. Invalid configs make the CLI exit with a one-line error.

### Home Assistant

With `discovery = true` in the `[mqtt]` table, the daemon publishes one
retained [MQTT discovery](https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery)
config message per sensor at startup (under
`<discovery_prefix>/sensor/aw2sqlite_<client_id>/<column>/config`). Home
Assistant then creates the entities automatically, with names, units, and
device classes derived from the station's own labels and unit settings.
Sensor values are read from the regular observation topic via a value
template, so no extra state publishing happens. Retained configs persist
across Home Assistant restarts; set `discovery_prefix` if your HA instance
uses a non-default prefix.

## Data Collection

The daemon continuously fetches live data from your weather station's HTTP endpoint, parses sensor readings from the HTML page, and inserts them into the SQLite database every 60 seconds.

- On an interactive terminal, current readings are displayed as labeled JSON with a live countdown to the next fetch. When stdout is captured (Docker, systemd, launchd, or a redirect), the daemon prints one concise line per cycle instead - the countdown and full-JSON redraw rely on terminal cursor control that would otherwise accumulate as unbounded noise in a log file
- Errors (timeouts, HTTP failures) are logged to `<database_stem>_daemon.log` and the daemon continues running. That log (and the API server's `<database_stem>_server.log`) uses a size-capped rotating handler (5 MB × 4 files) so it cannot grow without bound
- Implausible sensor values (e.g. temperature outside -150..200 range) emit a warning log
- If a fetch parses zero sensors (e.g. the station page format changed), a warning is logged and nothing is inserted for that interval
- Stale console pages are skipped: the station only refreshes its readings every few minutes, so polling more often re-serves an identical page. The daemon detects this using the station's own reported observation time (the `Time`-named field on `livedata.htm`, e.g. `CurrTime`) and skips the write while that time is unchanged; consoles that expose no such field fall back to skipping when every sensor value matches the last stored reading. This prevents a single unchanged reading from being fabricated into many distinct per-minute observations. Because the reported time carries no timezone or seconds it is used only as a change signal - the stored `ts` remains the collector's wall clock. The first poll after a (re)start has no prior reading, so it always inserts
- Duplicate observations (same timestamp) are silently ignored via `INSERT OR IGNORE`
- Samples are scheduled against a monotonic clock so intervals do not drift with fetch latency
- When an `[mqtt]` config table is present, each observation is also published to the broker as JSON (failures are logged and never interrupt collection)
- When an `[alerts]` config table is present, a `station_offline` webhook fires after `failure_threshold` consecutive failed fetches (default 3) and a `station_online` webhook fires on the first success afterward - one alert per outage, and webhook errors never interrupt collection. The POST body is JSON: `{"event": "station_offline", "station_url": "...", "consecutive_failures": 3, "timestamp": "2026-07-11T12:00:00+00:00"}`
- Sensor-name mappings, labels, and units are persisted in SQLite. The adjacent `<database_stem>_metadata.json` Datasette sidecar is refreshed when new sensor columns appear and can be regenerated offline after a restore (compatible with the [datasette-pint](https://github.com/simonw/datasette-pint) plugin)
- Press `Ctrl+C` to stop; `SIGTERM` (used by launchd/systemd/Docker) triggers the same graceful shutdown

### Database Schema

The `observations` table is created with a `NOT NULL` `ts` (TIMESTAMP) column
and a UNIQUE index for exact timestamp deduplication. Supplied ISO timestamps
are normalized to UTC. Sensor columns are added dynamically as `REAL` columns;
the `aw2sqlite_sensor_columns` table gives every raw station field a stable,
collision-free physical name and stores its label/unit metadata.

SQLite is configured with WAL journal mode, normal synchronous writes, in-memory temp storage, and 256MB memory-mapped I/O.

**→ [Full database design reference](docs/database.md)** - schema evolution, timestamp semantics, indexes, migrations, concurrency, backup behavior, and integrity boundaries.

## HTTP JSON API

When a port is configured, the daemon starts an HTTP server in a background thread with CORS enabled. It binds to `localhost` by default; set `host` (or pass `--host`) to bind another address, e.g. `0.0.0.0` to serve the local network. When exposing the server, set `auth_token` in the config - every request must then send an `Authorization: Bearer <token>` header or it is rejected with `401`.

| Endpoint | Description |
|----------|-------------|
| `GET /` | Current readings fetched live from the station, with human-readable labels |
| `GET /daily` | Aggregates grouped by date |
| `GET /hourly` | Aggregates grouped by date and hour, 24 slots per day |
| `GET /range` | Aggregates over an arbitrary `[start, end)` window |
| `GET /health` | Health check; returns `503` once collection has stalled |
| `GET /metrics` | Database metrics as JSON, or Prometheus text for a scraper |

**→ [Full API reference](docs/api.md)** - query parameters, response bodies, aggregation fields, timezone formats, and error codes.

## Recipes

Copy-paste configurations for [Datasette](docs/recipes.md#datasette), [Prometheus and Grafana](docs/recipes.md#prometheus-and-grafana), [Home Assistant](docs/recipes.md#home-assistant), [uptime monitoring](docs/recipes.md#uptime-monitoring), [nightly backups](docs/recipes.md#nightly-backups), and [ad-hoc SQL analysis](docs/recipes.md#ad-hoc-analysis).

## Development

Pull requests and issue reports are welcome. For major changes, please open an issue first to discuss what you would like to change.

```bash
uv sync --dev
uv run pytest tests/ --cov=ambientweather2sqlite
```

**→ [Development guide](docs/development.md)** - the full check suite, Semgrep rules, project conventions, and architecture diagrams.

## Legal

&copy; [Harold Martin](https://www.linkedin.com/in/harold-martin-98526971/) - released under [GPLv3](LICENSE.md)

AmbientWeather is a trademark of Ambient, LLC.
