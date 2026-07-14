# Recipes

Ready-made configurations for the tools this project is designed to plug into.
All of them assume the daemon is already collecting into `aw2sqlite.db` - see
the [Quick Start](index.md#quick-start).

## Datasette

The daemon writes a `<database_stem>_metadata.json` file in the
[Datasette metadata format](https://docs.datasette.io/en/stable/metadata.html),
containing human-readable sensor labels, units, and licensing. Point Datasette
at the database and that file to get a browsable, queryable UI for free:

```bash
uvx datasette aw2sqlite.db -m aw2sqlite_metadata.json
```

The units in the metadata are understood by the
[datasette-pint](https://github.com/simonw/datasette-pint) plugin, which renders
them alongside the values:

```bash
uvx --with datasette-pint datasette aw2sqlite.db -m aw2sqlite_metadata.json
```

!!! tip "Backup first"

    Run Datasette against a [backup copy](#nightly-backups) rather than the live
    database if you want to browse while the daemon is writing.

## Prometheus and Grafana

The `/metrics` endpoint serves the Prometheus text exposition format whenever the
`Accept` header asks for `text/plain` - which is exactly what a Prometheus
scraper sends - so no extra configuration is needed on this side. Add the station
to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: ambientweather
    scrape_interval: 60s
    static_configs:
      - targets: ["192.168.0.5:8080"]
```

If you set an `auth_token` in the config, pass it to the scrape job:

```yaml
scrape_configs:
  - job_name: ambientweather
    scrape_interval: 60s
    authorization:
      credentials: "s3cret"
    static_configs:
      - targets: ["192.168.0.5:8080"]
```

Every sensor is exported as a labeled gauge, so a Grafana panel for outdoor
temperature is a one-liner:

```promql
aw2sqlite_sensor_value{sensor="outTemp"}
```

Useful alerting expressions:

```promql
# Collection has stalled - no observation in 5 minutes
aw2sqlite_last_observation_age_seconds > 300

# Database growth has flatlined over the last hour
increase(aw2sqlite_observations_total[1h]) == 0
```

## Home Assistant

Enable MQTT discovery in the `[mqtt]` table of your config:

```toml
[mqtt]
host = "192.168.0.10"
topic = "weather/observations"
discovery = true
```

On startup the daemon publishes one retained discovery config per sensor under
`<discovery_prefix>/sensor/aw2sqlite_<client_id>/<column>/config`. Home Assistant
creates the entities automatically, with names, units, and device classes derived
from the station's own labels - no YAML entity definitions to write, and the
retained configs survive a Home Assistant restart.

Sensor values are read from the regular observation topic via a value template,
so enabling discovery does not add any extra publishing traffic.

Set `discovery_prefix` if your Home Assistant instance uses a non-default prefix.

## Uptime monitoring

`/health` returns `503` once the most recent observation is older than 180
seconds, so any check that treats non-2xx as failure will catch stalled
collection without custom parsing.

Container healthcheck:

```yaml
healthcheck:
  test:
    - "CMD"
    - "python"
    - "-c"
    - >-
      import urllib.request;
      request = urllib.request.Request(
        "http://localhost:8080/health",
        headers={"Authorization": "Bearer s3cret"},
      );
      urllib.request.urlopen(request)
  interval: 60s
  timeout: 5s
  retries: 3
```

Kubernetes liveness probe:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
    httpHeaders:
      - name: Authorization
        value: Bearer s3cret
  initialDelaySeconds: 30
  periodSeconds: 60
```

Replace `s3cret` with the configured `auth_token`. If authentication is disabled,
omit the `Authorization` header from either probe.

An empty database reports `"ok"` with `age_seconds: null`, so a freshly deployed
daemon passes its readiness probe before the first observation lands.

## Nightly backups

`aw2sqlite backup` uses SQLite's `VACUUM INTO`, which is safe to run while the
daemon is writing - a plain file copy of a WAL database is not. The destination
must not already exist, which makes it a natural fit for a dated filename:

```bash
aw2sqlite backup /backups/aw2sqlite-$(date +%F).db
```

As a cron entry that keeps 30 days of history:

```cron
0 3 * * * aw2sqlite backup /backups/aw2sqlite-$(date +\%F).db && find /backups -name 'aw2sqlite-*.db' -mtime +30 -delete
```

Note the escaped `\%` - cron treats an unescaped `%` as a newline.

## Ad-hoc analysis

Export a date range to CSV for a spreadsheet or notebook:

```bash
aw2sqlite export --format csv --start 2026-01-01 --end 2026-02-01 --output january.csv
```

Or query the database directly - it is a plain SQLite file with one `REAL` column
per sensor:

```bash
sqlite3 aw2sqlite.db "
  SELECT date(ts) AS day, round(max(outTemp), 1) AS high, round(min(outTemp), 1) AS low
  FROM observations
  WHERE ts >= date('now', '-7 days')
  GROUP BY day
  ORDER BY day;
"
```

Check for collection gaps, including one that is still ongoing:

```bash
aw2sqlite status --gap-hours 168 | jq '.gaps'
```
