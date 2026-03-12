# AmbientWeather to SQLite

[![PyPI](https://img.shields.io/pypi/v/ambientweather2sqlite.svg)](https://pypi.org/project/ambientweather2sqlite/)
[![Lint](https://github.com/hbmartin/ambientweather2sqlite/actions/workflows/lint.yml/badge.svg)](https://github.com/hbmartin/ambientweather2sqlite/actions/workflows/lint.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Code style: black](https://img.shields.io/badge/🐧️-black-000000.svg)](https://github.com/psf/black)
[![Checked with pyrefly](https://img.shields.io/badge/🪲-pyrefly-fe8801.svg)](https://pyrefly.org/)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/hbmartin/ambientweather2sqlite)

A project to record minute-by-minute weather observations from an AmbientWeather station over the local network - no API needed!

## Key Features

* **Local Network Operation:** Direct connection to weather stations without external API dependencies
* **Continuous Data Collection:** Automated daemon process collecting data at 60-second intervals
* **Dynamic Schema Management:** Automatic database schema evolution as new sensors are detected
* **HTTP JSON API:** Optional web server providing live data access, hourly and daily aggregation endpoints
* **Interactive Configuration:** Command-line setup wizard for initial configuration
* **Zero Dependencies:** Pure Python with no (potentially) untrusted 3rd parties

## Installation

* macOS: `brew install pipx && pipx install ambientweather2sqlite`
* Ubuntu / Debian: `sudo apt update && sudo apt install pipx && pipx install ambientweather2sqlite`
* Fedora: `sudo dnf install pipx && pipx install ambientweather2sqlite`

Requires Python 3.14+.

## Setup

```
ambientweather2sqlite [<port>] [<config_path>]
```

Both arguments are optional and can be provided in any order:
- `<port>` - Port number for the HTTP JSON API server (overrides config file value)
- `<config_path>` - Path to an existing TOML config file

On the first run, if no config file is found, you will be guided through an interactive setup wizard that prompts for:

1. **AmbientWeather Live Data URL** - e.g. `http://192.168.0.226/livedata.htm` (required)
2. **Database Path** - defaults to `./aw2sqlite.db`
3. **Server Port** - for the JSON API server (leave blank to disable)
4. **Output TOML Filename** - defaults to `./aw2sqlite.toml`

### Config File

The generated config file is a TOML file:

```toml
live_data_url = "http://192.168.0.226/livedata.htm"
database_path = "/path/to/aw2sqlite.db"
port = 8080  # optional, omit to disable the JSON server
```

Config file lookup order:
1. Path provided as CLI argument
2. `./aw2sqlite.toml` in the current directory
3. `~/.aw2sqlite.toml` in the home directory

## Data Collection

The daemon continuously fetches live data from your weather station's HTTP endpoint, parses sensor readings from the HTML page, and inserts them into the SQLite database every 60 seconds.

- Current readings are displayed in the terminal as labeled JSON
- Errors (timeouts, HTTP failures) are logged to `<database_stem>_daemon.log` and the daemon continues running
- A metadata file (`<database_stem>_metadata.json`) is generated with human-readable sensor labels and units (compatible with the [datasette-pint](https://github.com/simonw/datasette-pint) plugin)
- Press `Ctrl+C` to stop

### Database Schema

The `observations` table is created with a single `ts` (TIMESTAMP) column. Sensor columns are added dynamically as `REAL` columns when new data fields are encountered.

SQLite is configured with WAL journal mode, normal synchronous writes, in-memory temp storage, and 256MB memory-mapped I/O.

## HTTP JSON API

When a port is configured, the daemon starts a threaded HTTP server on `localhost` with CORS enabled (`Access-Control-Allow-Origin: *`). Server requests are logged to `<database_stem>_server.log`.

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
```
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
```
/hourly?start_date=2025-06-27&tz=America/Chicago&q=avg_outHumi
/hourly?start_date=2025-06-26&end_date=2025-06-27&tz=%2B05%3A30&q=max_gustspeed
/hourly?date=2025-06-27&tz=UTC&q=avg_outHumi
```

**Response:**
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
      null,
      "... 24 slots total, one per hour ..."
    ]
  }
}
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
| 404    | Unknown endpoint |
| 500    | Server error (e.g. weather station unreachable) |

## Development

Pull requests and issue reports are welcome. For major changes, please open an issue first to discuss what you would like to change.

### Core Architecture
<img src="media/arch.svg" />

### Control Flow
<img src="media/flow.svg" />

## Legal

&copy; [Harold Martin](https://www.linkedin.com/in/harold-martin-98526971/) - released under [GPLv3](LICENSE.md)

AmbientWeather is a trademark of Ambient, LLC.
