# Database design

ambientweather2sqlite stores each station sample as one row in a single SQLite
table. The table starts with only a timestamp and grows sideways as the station
reports new sensors. This keeps the database directly queryable with ordinary
SQL while allowing different AmbientWeather models and firmware versions to
expose different fields.

## Logical model

Each configured database contains one application-owned table and one index:

| Object | Kind | Purpose |
| --- | --- | --- |
| `observations` | Table | One row per collection time, with one nullable column per discovered sensor |
| `idx_observations_ts` | Unique index | Enforces one application observation per timestamp and accelerates time-ordered reads |

There are no lookup tables, foreign keys, materialized aggregates, or schema
version tables. Labels and units are presentation metadata rather than database
columns; they are written to the adjacent
`<database_stem>_metadata.json` file in Datasette's metadata format.

The initial schema is equivalent to:

```sql
CREATE TABLE observations (
    ts TIMESTAMP DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'now'))
);

CREATE UNIQUE INDEX idx_observations_ts ON observations(ts);
```

After the first observation, a typical database might look like this:

```sql
CREATE TABLE observations (
    ts TIMESTAMP DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'now')),
    outTemp REAL,
    outHumi REAL,
    avgwind REAL,
    gustspeed REAL,
    eventrain REAL
);
```

The sensor list above is only an example. The actual columns are determined by
the station and may include additional channels such as `pm25`,
`soilmoisture1`, or `solarrad`.

### Column semantics

| Column | Declared type | Nullability | Meaning |
| --- | --- | --- | --- |
| `ts` | `TIMESTAMP` | Nullable in SQLite; populated by the application | Collection time, normally stored as a UTC timestamp string |
| Every sensor column | `REAL` | Nullable | Numeric reading reported by the station at that time |

SQLite uses dynamic typing, so the declarations express affinity rather than a
strict storage contract. Normal collection parses sensor readings as floating
point values. A disconnected or unreadable sensor becomes SQL `NULL`; a sensor
that was not present in a particular response is also `NULL` for that row.

The table also has SQLite's implicit `rowid`. It is an implementation detail,
not a stable observation identifier. Use `ts` to identify and order
observations.

## Why a dynamic wide table

AmbientWeather station models do not all return the same fields, and accessory
sensors can appear later. A wide, additive schema provides three useful
properties:

- every observation is self-contained;
- common SQL, Datasette, CSV, and JSON tools can consume the data without
  joins or decoding a JSON blob;
- newly discovered sensors require no release-specific migration.

The tradeoff is a sparse schema: older rows have `NULL` for sensors discovered
later, and removed or disconnected sensors leave their columns behind. Column
order reflects schema evolution and is not an API guarantee. Consumers should
select columns by name rather than position.

## Write path

For every successful collection cycle, the write path performs the following
operations:

1. The station parser reads disabled inputs from `livedata.htm`. Battery,
   station ID, and station-provided time fields are intentionally excluded.
2. If `ts` is missing, blank, or not a string, the application supplies the
   current UTC time with microsecond precision.
3. Numeric values are checked against generous plausibility bounds. Outliers
   generate warnings but are still stored; this is observability, not data
   rejection.
4. Field names are converted to SQLite column names by preserving letters,
   numbers, and underscores and replacing every other character with `_`.
5. Missing columns are added with `ALTER TABLE observations ADD COLUMN ...
   REAL`.
6. The row is written with a parameterized `INSERT OR IGNORE` and committed.

For example, a station field named `soil-moisture.1` would be stored in a
column named `soil_moisture_1`. Station-provided names normally already use
SQL-friendly identifiers. Since the database does not store a separate mapping
from original to normalized names, custom producers should avoid names that
normalize to the same column.

The unique timestamp index is the deduplication boundary. If an observation
with the same non-`NULL` `ts` already exists, the entire later insert is ignored;
it does not update or merge sensor values into the existing row.

### Schema evolution

Schema evolution is intentionally additive:

- sensor columns are added on first sight;
- existing sensor columns are reused;
- columns are never automatically renamed, retyped, or dropped;
- historical rows are never backfilled when a new sensor appears.

No schema version number is required for sensor changes. At startup, the
application checks for the `observations` table. A missing table is initialized
even if the database file itself already exists.

Existing databases created before timestamp uniqueness was enforced are
migrated on startup. If duplicate timestamps prevent creation of the unique
index, the migration:

1. keeps the earliest `rowid` for each duplicated timestamp;
2. fills each column with the last non-`NULL` value in `rowid` order;
3. removes the other duplicate rows; and
4. creates the unique index.

This merge behavior belongs only to that compatibility migration. Normal
duplicate inserts remain all-or-nothing ignores.

## Timestamp model

Application-generated timestamps use an ISO-compatible, UTC-without-offset
representation:

```text
2026-07-13 18:42:09.123456
```

“Without offset” means the stored string has no `Z` or `+00:00`, but its value
is interpreted as UTC. This representation makes chronological ordering and
range filtering work with ordinary string comparisons when values remain in the
canonical format. Microseconds are included for application-generated values;
the SQLite default is a fallback for direct SQL inserts and normally provides
millisecond precision.

The public write function accepts a caller-supplied nonblank timestamp string
as-is. Code that writes observations programmatically or with a SQLite client is
therefore responsible for using canonical UTC values. Offset-bearing,
non-padded, or otherwise inconsistent strings can produce surprising ordering,
range, aggregation, and staleness results.

Time-window operations consistently use half-open intervals:

```text
start <= ts < end
```

This makes adjacent ranges composable without double-counting a boundary row.
Export bounds are compared directly with the stored strings, so a date such as
`2026-07-13` acts as that day's midnight boundary. API aggregation bounds are
interpreted in the requested timezone and converted to UTC first.

IANA timezone aggregation, such as `America/Los_Angeles`, loads the relevant
UTC rows and performs calendar bucketing in Python so daylight-saving
transitions are handled correctly. Local and fixed-offset aggregation can use
SQLite date/time modifiers directly. `AVG`, `MIN`, `MAX`, and `SUM` ignore
`NULL` sensor values, while the returned `count` is `COUNT(*)` and therefore
counts every observation row in the bucket.

## Indexing and query behavior

`idx_observations_ts` is the only application-created index. It supports the
primary access patterns:

- latest observation and timestamp;
- oldest-to-newest export;
- inclusive-start/exclusive-end range scans;
- gap detection between adjacent samples;
- daily, hourly, and arbitrary-range aggregation.

There are no indexes on sensor columns because normal queries constrain time
and aggregate sensor values. A custom workload that filters heavily by sensor
value may benefit from a user-managed index, but the application neither
creates nor maintains one.

The database has no retention policy. At the default one-minute interval, an
uninterrupted station produces about 1,440 rows per day or 525,600 rows per
year. Storage depends on the number of sensor columns, their `NULL` density,
and SQLite's WAL/checkpoint state.

## Connections and concurrency

The daemon opens short-lived connections rather than sharing a global
connection. Read paths open the database through SQLite's read-only URI mode;
write paths configure the connection as follows:

| Setting | Value | Effect |
| --- | --- | --- |
| `busy_timeout` | 5,000 ms | Wait briefly for a lock before returning an error; applied to reads and writes |
| `journal_mode` | `WAL` | Allows readers to continue while the daemon commits observations |
| `synchronous` | `NORMAL` | Reduces sync work for WAL commits; database consistency is retained, but a recent commit can be lost after a power failure |
| `temp_store` | `MEMORY` | Keeps temporary query structures in memory for that connection |
| `mmap_size` | 268,435,456 bytes | Requests up to 256 MiB of memory-mapped database I/O |

WAL still permits only one writer at a time. The busy timeout smooths over
brief contention from another SQLite client, but long external write
transactions can cause a collection insert to fail. The daemon logs SQLite
errors and continues with the next cycle.

While WAL is active, SQLite may create `<database>.db-wal` and
`<database>.db-shm` sidecar files. They are part of the live database state and
must not be copied independently.

## Files, backup, and export

For a configured path such as `/var/lib/aw2sqlite/weather.db`, the application
may create:

| Path | Contents |
| --- | --- |
| `weather.db` | Main SQLite database |
| `weather.db-wal`, `weather.db-shm` | SQLite WAL sidecars while needed |
| `weather_metadata.json` | Datasette labels, units, source, and licensing metadata |
| `weather_daemon.log`, `weather_server.log` | Rotating application logs |

The `.db` file and its two SQLite sidecars represent live database state.
Metadata and logs are adjacent application artifacts and are not embedded in a
database backup.

Use `aw2sqlite backup` rather than copying `weather.db` while the daemon is
running. The command uses SQLite's `VACUUM INTO` to create a compact,
self-contained snapshot of committed data. It refuses to overwrite an existing
destination and removes a partial destination after failure. Uncommitted data
from another writer is not included.

`aw2sqlite export` reads all physical columns, orders rows by `ts`, and writes
CSV or JSON. `--start` is inclusive and `--end` is exclusive. Because sensor
columns are dynamic, exports from databases—or from the same database at
different points in its life—can have different headers.

The `status` command and `/metrics` endpoint report the row count, main database
file size, earliest and latest timestamps, and current column count. The file
size is the size of the main `.db` file and does not include WAL sidecars.

## Integrity boundaries

The collection path maintains these application-level invariants:

- generated timestamps are canonical UTC strings;
- normal writes contain a nonblank timestamp;
- non-`NULL` timestamps are unique;
- sensor columns use `REAL` affinity and are only added;
- duplicate normal inserts do not mutate existing rows.

Some of these rules are deliberately not strict SQL constraints. `ts` is not
declared `NOT NULL`, and SQLite unique indexes permit multiple `NULL` values.
There are no `CHECK` constraints for sensor ranges because firmware, units, and
supported hardware vary; implausible values are warned about instead. Direct
SQL writers can bypass the application invariants and should validate their
input accordingly.

For a non-destructive inspection of a running database:

```bash
sqlite3 -readonly /path/to/aw2sqlite.db
```

Useful commands and queries include:

```sql
.schema observations
PRAGMA table_info(observations);
PRAGMA index_list(observations);
PRAGMA quick_check;

SELECT COUNT(*) FROM observations;
SELECT MIN(ts), MAX(ts) FROM observations;
SELECT ts, outTemp, outHumi
FROM observations
ORDER BY ts DESC
LIMIT 10;
```

Use the actual names returned by `PRAGMA table_info(observations)` because the
available sensors differ by station.
