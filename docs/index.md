# AmbientWeather to SQLite

Record minute-by-minute weather observations from an AmbientWeather station over
your local network - no cloud account, no API key, no dependencies.

## Quick Start

```bash
uvx ambientweather2sqlite config   # scan the network, find your station, write a config
uvx ambientweather2sqlite serve    # collect every 60s into SQLite, serve the JSON API
```

The wizard scans your subnet for the station, so for most setups those two
commands are all there is to it. To check that the station is reachable without
touching the database:

```bash
uvx ambientweather2sqlite once     # print one live observation as JSON
```

Requires Python 3.13+.

## Why this exists

Your station already serves its readings on your own network. This reads them
straight off the device, so collection keeps working when the vendor's cloud is
down, nothing is rate-limited, no account is involved, and your data never leaves
the house.

## Features

- **Local network operation** - talks directly to the station's `livedata.htm`, so there is no cloud API, no API key, and no account.
- **Zero dependencies** - pure Python with no third-party runtime packages to trust or patch.
- **Auto-discovery** - the setup wizard scans your subnet and finds the station for you.
- **A database that keeps up with your hardware** - sensor columns are added automatically as new readings appear, timestamps are deduplicated, and implausible values are flagged.
- **Plugs into what you already run** - Home Assistant (MQTT discovery), Prometheus and Grafana (`/metrics`), and Datasette (generated metadata). See [Recipes](recipes.md).
- **Runs as a service** - Docker, launchd, and systemd units included, with an HTTP JSON API for live and aggregated data.

## Installation

If you have [uv](https://docs.astral.sh/uv/) installed, you can run it directly:

```bash
uvx ambientweather2sqlite
```

Or install it with curl:

```bash
curl -LsSf uvx.sh/ambientweather2sqlite/install.sh | sh
```

## Where to go next

<div class="grid cards" markdown>

- :material-api: **[HTTP JSON API](api.md)** - live readings, daily/hourly/range aggregation, health, and Prometheus metrics.
- :material-book-open-variant: **[Recipes](recipes.md)** - Datasette, Grafana, Home Assistant, uptime monitoring, and backups.
- :material-code-braces: **[Development](development.md)** - project setup, checks, and architecture.

</div>

The CLI reference, configuration file format, and data-collection behavior are
documented in the
[README](https://github.com/hbmartin/ambientweather2sqlite#readme).

## Legal

&copy; [Harold Martin](https://www.linkedin.com/in/harold-martin-98526971/) - released under [GPLv3](https://github.com/hbmartin/ambientweather2sqlite/blob/main/LICENSE.md)

AmbientWeather is a trademark of Ambient, LLC.
