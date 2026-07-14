# Database design

ambientweather2sqlite stores each station sample as one row in a wide SQLite
table. The table starts with a timestamp and grows sideways as the station
reports new sensors. This remains directly queryable with ordinary SQL while
supporting different station models, firmware versions, and later accessories.

## Logical model

Each current database contains these application-owned objects:

| Object | Kind | Purpose |
| --- | --- | --- |
| `observations` | Table | One row per collected sample, with one nullable `REAL` column per discovered sensor |
| `aw2sqlite_sensor_columns` | Table | Stable mapping from station field names to physical SQL columns, plus labels and units |
| `idx_observations_ts` | Unique index | Rejects duplicate exact, non-`NULL` timestamp values and accelerates time-ordered reads |
| `PRAGMA user_version` | Schema marker | Identifies the application schema version used by explicit migrations |

`aw2sqlite_migration_conflicts` is created only when a legacy migration must
merge duplicate timestamps. It preserves a JSON snapshot of every source row
involved in those merges.

The initial observation schema is equivalent to:

```sql
CREATE TABLE observations (
    ts TIMESTAMP NOT NULL
        DEFAULT (
            STRFTIME('%Y-%m-%d %H:%M:%S', 'now')
            || SUBSTR(STRFTIME('%f', 'now'), 3) || '000'
        )
);

CREATE UNIQUE INDEX idx_observations_ts ON observations(ts);
```

After collection begins, a typical table might look like this:

```sql
CREATE TABLE observations (
    ts TIMESTAMP NOT NULL
        DEFAULT (
            STRFTIME('%Y-%m-%d %H:%M:%S', 'now')
            || SUBSTR(STRFTIME('%f', 'now'), 3) || '000'
        ),
    outTemp REAL,
    outHumi REAL,
    avgwind REAL,
    gustspeed REAL,
    eventrain REAL
);
```

The sensor list is station-dependent and may include channels such as `pm25`,
`soilmoisture1`, or `solarrad`.

### Column semantics

| Column | Declared type | Nullability | Meaning |
| --- | --- | --- | --- |
| `ts` | `TIMESTAMP` | `NOT NULL` in newly created databases | Canonical UTC collection time stored without an offset |
| Every sensor column | `REAL` | Nullable | Numeric reading reported by the station at that time |

SQLite declarations express affinity rather than strict storage types. Normal
collection parses readings as floating-point values. An unreadable value is SQL
`NULL`; a sensor omitted from one response is also `NULL` in that row.

The implicit `rowid` is an implementation detail, not a stable observation
identifier. Application-written timestamps are canonical and unique, but a
legacy database or unsupported direct SQL write can contain other exact values.

## Why a dynamic wide table

Station models do not all return the same fields, and accessories can appear
later. A wide additive schema keeps every observation self-contained, works
with ordinary SQL/Datasette/CSV/JSON tools, and avoids release-specific
migrations for sensor changes.

The tradeoff is sparsity: historical rows have `NULL` for newly discovered
sensors, and removed sensors leave their columns behind. Column order is not an
API. Consumers should select columns by name.

### Sensor identifier mapping

Station field names and physical SQL names are connected by
`aw2sqlite_sensor_columns`:

- ordinary names such as `outTemp` remain unchanged;
- punctuation is normalized when that produces an unclaimed SQL-safe name;
- empty, digit-leading, case-conflicting, or colliding names receive an
  injective `aw2_<utf8-hex>` name;
- all generated SQL quotes identifiers;
- the persisted mapping prevents two raw fields from sharing one column.

For example, the first `soil-moisture.1` field normally receives
`soil_moisture_1`. If `soil.moisture.1` also appears, it receives a distinct
encoded column instead of silently overwriting the first value.

Datasette metadata and aggregation fields use physical names. Live JSON and
MQTT continue to use the station's raw field names. Query `PRAGMA
table_info(observations)` or `aw2sqlite_sensor_columns` when a custom field needs
to be correlated across those surfaces.

## Write path

For every successful collection cycle, the application:

1. parses sensor inputs from `livedata.htm`, excluding battery, station ID,
   and station-provided time fields;
2. generates a microsecond UTC timestamp when `ts` is missing, blank, or not a
   string;
3. parses any supplied timestamp as ISO 8601, converts it to UTC, and rejects an
   invalid value;
4. warns about implausible numeric readings without rejecting them;
5. resolves raw sensor names through the persisted mapping;
6. adds missing quoted `REAL` columns; and
7. performs the parameterized `INSERT OR IGNORE` in the same transaction.

If timestamp deduplication ignores the row, mapping and column additions from
that attempted row are rolled back as well. A duplicate does not merge readings
or mutate the schema.

### Schema evolution

Sensor evolution remains additive: columns are added on first successful
observation, reused thereafter, and never automatically renamed, retyped,
dropped, or backfilled. Labels and units are refreshed after new columns are
discovered and persisted in the mapping table as well as the adjacent Datasette
sidecar.

Application schema changes are separately versioned with `PRAGMA user_version`.
The daemon refuses to alter a legacy schema implicitly. Inspect and apply an
upgrade explicitly:

```bash
aw2sqlite migrate --check
aw2sqlite migrate
```

`migrate` creates a timestamped `VACUUM INTO` backup before changing anything.
Use `--backup PATH` to choose the destination. If a pre-uniqueness database has
duplicate timestamps, the migration keeps the earliest `rowid`, fills every
column from the last non-`NULL` value in `rowid` order, stores every original
row in `aw2sqlite_migration_conflicts`, removes redundant rows, and creates the
unique index. It also rebuilds legacy observation tables so `ts` is `NOT NULL`.
User-managed observation indexes and triggers are recreated after that table
rebuild.
If any legacy row has a `NULL` timestamp, migration stops without changing the
source and reports the backup path so the row can be repaired deliberately. The
source backup is the authoritative pre-migration snapshot.

## Timestamp model

Application writes use a canonical UTC-without-offset representation:

```text
YYYY-MM-DD HH:MM:SS[.ffffff]
```

The six-digit fractional part is present only when microseconds are nonzero;
zero microseconds are stored without a fraction. Supplied ISO 8601 timestamps
are normalized to this representation. For example,
`2026-07-13T11:42:09-07:00` becomes `2026-07-13 18:42:09`, while
`2026-07-13T11:42:09.123456-07:00` becomes
`2026-07-13 18:42:09.123456`. This makes exact uniqueness, lexical ordering,
range filtering, aggregation, gaps, and staleness agree.

Direct SQL is supported for inspection and custom read queries, not as an
application write API. The SQLite default remains useful for emergency/manual
inserts, but direct writers bypass normalization and sensor mapping and can
violate application semantics.

Export and arbitrary `/range` windows are half-open:

```text
start <= ts < end
```

`/hourly` instead accepts an inclusive range of calendar dates: both
`start_date` and `end_date` are returned. These interfaces are intentionally
different because one operates on instants and the other on named local days.

IANA timezone daily/hourly aggregation loads the relevant UTC range and buckets
rows in Python. The response always has 24 wall-clock slots per date. On a
fall-back transition, both physical occurrences of a repeated hour are merged
into that hour's single slot; `count` includes both. On a spring-forward day,
the skipped hour is `null`. Fixed-offset and local aggregation use SQLite
date/time modifiers.

`AVG`, `MIN`, `MAX`, and `SUM` ignore `NULL` values. The returned `count` is the
number of observation rows in the bucket, including rows where the requested
sensor is `NULL`.

## Indexing and query behavior

`idx_observations_ts` accelerates latest/earliest reads, ordered export, raw
half-open range scans, gap detection, and IANA queries after their local bounds
have been converted to UTC. Fixed-offset and local daily/hourly paths apply
SQLite `DATE`/`strftime` functions to `ts`; those calendar expressions may scan
the observations table rather than use the timestamp index for filtering.

The application creates no sensor-value indexes. A user-managed index can help
a custom workload that filters heavily by a sensor value.

There is no retention policy. At the one-minute default, uninterrupted
collection produces about 1,440 rows per day or 525,600 per year. Storage and
calendar-query cost grow with history, sensor count, `NULL` density, and WAL
checkpoint state.

## Connections and concurrency

The daemon uses short-lived connections. Reads use SQLite's read-only URI mode;
writes use these settings:

| Setting | Value | Effect |
| --- | --- | --- |
| `busy_timeout` | 5,000 ms | Wait briefly for a lock; applied to reads and writes |
| `journal_mode` | `WAL` | Readers can continue while an observation commits |
| `synchronous` | `NORMAL` | Retains consistency while allowing a recent commit to be lost after power failure |
| `temp_store` | `MEMORY` | Keeps temporary query structures in memory for that connection |
| `mmap_size` | 268,435,456 bytes | Requests up to 256 MiB of memory-mapped database I/O |

Schema resolution and insertion use one `BEGIN IMMEDIATE` transaction, which
serializes concurrent schema writers. WAL still allows only one writer at a
time. Long external write transactions can exhaust the busy timeout; the daemon
logs that SQLite error and continues with the next cycle.

SQLite may create `<database>-wal` and `<database>-shm` sidecars. They are live
database state and must not be copied independently.

## Files, backup, and export

For `/var/lib/aw2sqlite/weather.db`, the application may create:

| Path | Contents |
| --- | --- |
| `weather.db` | Observations, schema version, sensor mapping, and persisted labels/units |
| `weather.db-wal`, `weather.db-shm` | SQLite WAL sidecars while needed |
| `weather_metadata.json` | Regenerable Datasette sidecar |
| `weather_daemon.log`, `weather_server.log` | Rotating application logs |

Use `aw2sqlite backup` rather than copying the main file while the daemon runs.
It uses `VACUUM INTO` to create a compact self-contained snapshot of committed
database state, including the sensor mapping and stored labels/units. It refuses
to overwrite an existing destination and removes a partial destination after
failure.

The JSON sidecar and configuration are not copied into the database backup.
After restoring the database alongside a valid config, recreate the sidecar
without contacting the station:

```bash
aw2sqlite metadata --offline
```

Run `aw2sqlite metadata` without `--offline` to refresh labels and units from the
station. Offline regeneration exits with status 1 and preserves any existing
sidecar when the database has no stored sensor metadata. Logs are operational
artifacts and are not part of either backup.

`aw2sqlite export` reads all physical columns, orders by `ts`, and writes CSV or
JSON. `--start` is inclusive and `--end` exclusive. Dynamic schemas mean export
headers can differ across databases or across points in one database's life.

`status` and `/metrics` report row count, main-file size, earliest/latest
timestamps, and column count. The reported size excludes WAL sidecars.

## Integrity boundaries

The application write path maintains these invariants:

- timestamps are canonical UTC strings and non-`NULL`;
- exact application timestamps are unique;
- every raw sensor name has one stable, case-insensitively unique physical name;
- sensor columns have `REAL` affinity and evolve only additively;
- schema changes and row insertion are atomic; and
- duplicate inserts do not mutate existing rows or the schema.

No `CHECK` constraints reject sensor ranges because firmware, units, and
supported hardware vary. Direct SQL writes can bypass most application
invariants and are unsupported.

For non-destructive inspection:

```bash
sqlite3 -readonly /path/to/aw2sqlite.db
```

Useful commands and queries:

```sql
.schema observations
PRAGMA user_version;
PRAGMA table_info(observations);
PRAGMA index_list(observations);
PRAGMA quick_check;

SELECT raw_name, column_name, label, unit
FROM aw2sqlite_sensor_columns
ORDER BY column_name;

SELECT COUNT(*), MIN(ts), MAX(ts) FROM observations;
SELECT ts, outTemp, outHumi
FROM observations
ORDER BY ts DESC
LIMIT 10;
```
