# User Guide

Quick start and operational guide for Debian Observer Release 1.

## Installation

### A) From source

```shell
git clone <repository>
cd debian-observer
python3 -m venv your.venv
source your.venv/bin/activate
pip install -e "."
```

### B) pipx

```shell
pipx install dist/debian_observer.whl
```

### C) Docker (Compose)

```shell
cd deploy/compose
docker compose up -d
```

Then

```shell
docker exec -ti debian-observer bash
```

## Services

| Service | Port | Purpose |
|---------|------|---------|
| NATS | 4222 | JetStream event backbone |
| NATS monitoring | 8222 | Health and metrics |
| API | 8088 | FastAPI analytical endpoints |


## Quick Start: Smoke Test with Real Debian Data

For a quick end-to-end test using a real public Debian data source:

```bash
debian-observer ingest sources \
  --package hello \
  --raw-dir /tmp/smoke-raw \
  --events-dir /tmp/smoke-events
```

This fetches version history for the `hello` package from `sources.debian.org`.

**Expected output (sources.debian.org):**
```
Ingested N records
Run ID: run-xxxxxx
Checksum: ...
Events produced: N
Parquet written: /tmp/smoke-events/run-xxxxxx.parquet
```

**Mirror package list (Packages.gz):**

For a quick test with actual Debian Archive `Packages.gz` data:

```bash
debian-observer ingest mirror \
  --suite stable \
  --limit 5 \
  --raw-dir /tmp/mirror-raw \
  --events-dir /tmp/mirror-events
```

**Expected output (mirror):**
```
Ingested 5 records
Run ID: run-xxxxxx
Checksum: ...
Events produced: 5
Parquet written: /tmp/mirror-events/run-xxxxxx.parquet
```

Mirror records carry `distribution` as the archive family (`debian`,
or `ubuntu` for Ubuntu mirrors), and split the `--suite` path:
suite roles (`oldstable`, `stable`, `testing`) land in `suite`,
codenames and pockets (`bookworm`, `sid`, `bookworm-security`) land in
`release`.

**UDD public mirror (real PostgreSQL):**

For querying the actual UDD public mirror:

```bash
debian-observer ingest udd \
  --table packages \
  --limit 3 \
  --raw-dir /tmp/udd-raw \
  --events-dir /tmp/udd-events
```

This queries the `packages` table from `udd-mirror.debian.net:5432`.
The default table is `packages`, which includes package (binary), source, version, distribution, release (suite/pocket), architecture, component, section and priority columns.

**Expected output (udd):**
```
Ingested 3 records
Run ID: run-xxxxxx
Checksum: ...
Events produced: 3
Parquet written: /tmp/udd-events/run-xxxxxx.parquet
```

Available tables include: `packages`, `sources`, `upload_history`.

**Custom UDD server (e.g. private mirror):**

Set environment variables to override the default public mirror:

| Variable | Default | Description |
|----------|---------|-------------|
| `UDD_HOST` | `udd-mirror.debian.net` | PostgreSQL host |
| `UDD_PORT` | `5432` | PostgreSQL port |
| `UDD_DATABASE` | `udd` | Database name |
| `UDD_USER` | `udd-mirror` | Username |
| `UDD_PASSWORD` | `udd-mirror` | Password |

Example for a local mirror at `192.168.1.218`:

```bash
UDD_HOST=192.168.1.218 UDD_USER=udd UDD_PASSWORD=udd \
  debian-observer ingest udd \
  --table packages \
  --limit 3 \
  --raw-dir /tmp/udd-raw \
  --events-dir /tmp/udd-events
```

**UDD dump file (no live database):**

`ingest udddump` is **acquisition only** (ADR-012 amendment,
2026-10-23): it downloads the dump over HTTPS (`--url`, the
mandatory scope selector) and stores it **byte-preserving** as the
raw artifact `data/raw/udddump-<YYYYMMDD-HHMMSS>-<run-id>.dump` —
exactly as served, no decompression, no `.csv.xz` — with a manifest
sidecar (`<artifact>.manifest.json`) and a provenance record. No
parsing happens here: no pg_restore, no gates, no events.

```bash
debian-observer ingest udddump \
  --url https://udd.example.org/udd.dump \
  --raw-dir data/raw
```

**Expected output (udddump download):**
```
Downloaded https://udd.example.org/udd.dump
Run ID: run-xxxxxxxxxxxx
Raw artifact: data/raw/udddump-20261023-101500-run-xxxxxxxxxxxx.dump
Manifest: data/raw/udddump-20261023-101500-run-xxxxxxxxxxxx.dump.manifest.json
Checksum: <sha256 of the stored bytes>
Size: <n> bytes
```

The manifest sidecar records the provenance of the download; the
record count is unknown until normalization (`record_count: null`):

```json
{
  "dataset_name": "udddump-run-9f2c1ab34d5e",
  "source": "udddump",
  "source_version": null,
  "retrieved_at": "2026-10-23T10:15:42.314159+00:00",
  "checksum": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "size_bytes": 2934776238,
  "record_count": null,
  "license": null
}
```

**Eventize a table of the dump (`normalize udddump`):**

The reading is normalization's job: `normalize udddump` loads the
artifact's manifest sidecar (source and run id), validates `--table`
against the dump's table of contents (the error lists the restorable
tables) and the table's COPY header against the registered shapes,
then streams the COPY block through `pg_restore --data-only --table`
**on the host** (`pg_restore` on PATH; the dev and deploy images ship
`postgresql-client`) — no database connection, no restore, and no
container indirection (ADR-006) — and writes `PackageVersionObserved`
events as JSON lines to `--output` (default stdout). `--limit` unset
normalizes the whole table with a warning on stderr.

```bash
debian-observer normalize udddump \
  data/raw/udddump-20261023-101500-run-9f2c1ab34d5e.dump \
  --table packages \
  --limit 3 > events.jsonl
```

Re-eventizing the same artifact with other filters (`--table`,
`--limit`) is supported: the raw download is not consumed, and each
run re-reads the manifest sidecar.

Today only packages-shaped tables are eventizable (required COPY
columns: version + any of: package, source — mirroring the
validator); more table shapes will land as new event types do.
Records that fail canonical transformation are skipped with a warning
naming the cause — the raw artifact preserves them (ADR-002).

**Verify outputs:**

Raw artifact (xz-compressed CSV):
```bash
xz -dc /tmp/smoke-raw/sources.debian.org-*.csv.xz | head -n 3
```

Parquet schema:
```bash
python3 -c "
import pyarrow.parquet as pq
table = pq.read_table('/tmp/smoke-events/run-*.parquet')
print(f'Rows: {table.num_rows}, Cols: {table.num_columns}')
print(f'Columns: {table.column_names}')
"
```


## Production Ingest: Full Dataset

For larger datasets (e.g. complete UDD), the acquisition layer supports configurable sources.

```bash
debian-observer ingest udd \
  --table packages \
  --raw-dir data/raw \
  --events-dir data/events
```

### Verify:

```bash
debian-observer --help
```

## Quick Start

Run a basic ingestion using a configured data source:

```bash
debian-observer ingest sources \
  --package <package> \
  --raw-dir data/raw
```

Options: `--limit` (mirror/udd; unset ingests the whole source
with a warning), `--component`/`--architecture` (mirror; default
`main`/`amd64`), `--events-dir` (event log output).
See `debian-observer ingest <source> --help` for full options.

## Observability

OpenTelemetry tracing and metrics are wired throughout the pipeline,
messaging and the API; the API exposes `/metrics/`. Console telemetry
output appears only for long-lived processes: metrics export every 5
seconds while a process runs, and traces flush at process exit —
short-lived CLI commands show no telemetry output.


## Normalization

Transform raw artifacts into canonical domain events. `normalize` is
a command group with one subcommand per raw surface (ADR-012
amendment, 2026-10-23); the source name and run id are always read
from each artifact's manifest sidecar (`.manifest.json`):

- `normalize artifacts <paths…>` — pipeline `.csv.xz` artifacts
  (one or more, positional). Their events are concatenated in
  order, each stamped with its own source and run id:

  ```bash
  debian-observer normalize artifacts data/raw/<file>.csv.xz > events.jsonl
  ```

- `normalize udddump <dump-artifact> --table <t> [--limit n]` — UDD
  `pg_dump` download artifacts written by `ingest udddump --url`
  (see the udddump section above for the gates, the pg_restore PATH
  dependency and an example invocation over an existing manifest
  with other filters).

Run `debian-observer normalize <surface> --help` for all options
(output file, invalid-records file, udddump filters).


## Validate raw records

Validate raw artifacts against the expected UDD shape before
normalization: a record needs a `version` and a `package` or
`source` name. `validate` takes one or more raw artifacts and reads
each artifact's source from its manifest sidecar. Invalid records
are reported with a reason on stderr and the run prints
valid/invalid counts per artifact; the exit code stays 0 even when
invalid records are found — validation is a report, not a gate.

```bash
debian-observer validate data/raw/<file>.csv.xz
```

Mock artifacts (`mock-debian-*.csv.xz`) hold synthetic rows that
mirror the UDD `packages` shape, so `validate` counts them valid —
they are test rows, not real UDD data.

Run `debian-observer validate --help` for all options.

## Query stored data

Run DuckDB SQL queries over ingested Parquet events:

```bash
debian-observer query \
  "SELECT package_id, version, release FROM events WHERE release = 'sid'" \
  --events-dir data/events
```

**Aggregations:**

```bash
debian-observer query \
  "SELECT architecture, COUNT(*) as count FROM events GROUP BY architecture ORDER BY count DESC" \
  --events-dir data/events
```

**Output formats:**

| Format | Flag | Use case |
|--------|------|----------|
| Table | `--format table` (default) | Human-readable |
| JSON | `--format json` | Machine parsing |
| CSV | `--format csv` | Spreadsheet import |

**Example: export to CSV**

```bash
debian-observer query \
  "SELECT entity_id, version, observed_at FROM events" \
  --events-dir data/events \
  --format csv > events.csv
```

Data is stored in **Parquet** format with a **DuckDB** analytical layer for SQL queries.


## Pipeline

The ingestion pipeline integrates ParquetWriter for optional persistent storage. After running `debian-observer ingest`, raw artifacts
(xz-compressed CSV, `*.csv.xz`), the run manifest (JSON,
`*.manifest.json` next to each raw artifact) and Parquet events
are available in `data/`. Ingestion eventizes raw observations into
the append-only event log — one Parquet file per ingestion run —
and nothing else: the consolidated `package_versions` dataset is a
**consumer output** (see Consolidate below), not a pipeline step.



## Prosumers (Pipeable Processors)

The prosumers are stand-alone CLI tools that read and write
observation events via a common pipeable contract.  They are
intended for ad-hoc analytical pipelines and for researchers who
want to compose their own event-processing steps without writing
Python code.

Every prosumer accepts at least one input and at least one output
from the following four options:

| Flag | Meaning |
|------|---------|
|  | Read Parquet events from disk |
|  | Read ND-JSON records from stdin |
|  | Write Parquet events to disk |
|  | Write ND-JSON records to stdout |

### 

Folds a stream of  events into a single
consolidated  table.  Duplicate
 values collapse; the surviving row per observation key

is the one with the latest .



### 

Pass-through filter for observation events.  Accepts repeatable
 and  flags (AND logic).  Non-observation
records are passed through unchanged.




## Prosumers (Pipeable Processors)

The prosumers are stand-alone CLI tools that read and write
observation events via a common pipeable contract.  They are
intended for ad-hoc analytical pipelines and for researchers who
want to compose their own event-processing steps without writing
Python code.

Every prosumer accepts at least one input and at least one output
from the following four options:

| Flag | Meaning |
|------|---------|
| `--file-in PATH` | Read Parquet events from disk |
| `--stream-in` | Read ND-JSON records from stdin |
| `--file-out PATH` | Write Parquet events to disk |
| `--stream-out` | Write ND-JSON records to stdout |

### `debian-observer-consolidate`

Folds a stream of `PackageVersionObserved` events into a single
consolidated `PackageVersionConsolidated` table.  Duplicate
`event_id` values collapse; the surviving row per observation key
`(package_id, version, distribution, release, suite, architecture)`
is the one with the latest `(observed_at, event_id)`.

```bash
# File to file (batch rebuild)
debian-observer-consolidate --file-in events.parquet --file-out package_versions.parquet

# Stream to stream (pipe through a filter first)
cat events.ndjson | debian-observer-consolidate --stream-in --stream-out > consolidated.ndjson
```

### `debian-observer-filter`

Pass-through filter for observation events.  Accepts repeatable
`--distribution` and `--release` flags (AND logic).  Non-observation
records are passed through unchanged.

```bash
# Keep only Debian sid observations
debian-observer-filter --file-in events.parquet --file-out sid.parquet \
  --distribution debian --release sid

# Pipe: consolidate then filter
debian-observer-consolidate --stream-in --stream-out | \
  debian-observer-filter --stream-in --stream-out --distribution debian | \
  debian-observer-consolidate --stream-in --stream-out > debian-only.parquet
```

## Consolidate

The `consolidate` command folds the event log into the consolidated
`package_versions` table (spec §10.2, ADR-003 amendment 2026-10-23):

```bash
debian-observer consolidate --events-dir data/parquet \
  --dataset-dir data/normalized/package_versions
```

`--events-dir` is required; `--dataset-dir` defaults to
`data/normalized/package_versions`. The consumer reads every
`*.parquet` event file in the events dir, keeps the
`PackageVersionObserved` events and consolidates them: one row per
full observation PK `(package_id, version, distribution, release,
suite, architecture)`; attribute columns (`source_package`,
`component`, `section`, `priority`, `source`) are last-observed-wins
(ordered by `observed_at`, then `event_id`);
`observed_first_at`/`observed_last_at` are the min/max `observed_at`
of the group. The output is ONE re-generable file —
`package_versions.parquet`, written atomically (tmp file + rename),
so a concurrent reader never sees a partial file. The fold is
idempotent by construction: folding the same log twice yields the
same table, and duplicated log files collapse (spec §15.1). The
command is local only — no NATS/streaming is involved.

Query the consolidated table with DuckDB (ADR-003 example, on the
dataset schema):

```sql
SELECT package_id, version, release
FROM read_parquet('data/normalized/package_versions/*.parquet')
WHERE release = 'sid'
```

The table is **state, not log**: it is fully derived from the event
log and can be re-generated at any time. The event log remains the
replay surface; the dataset is the consolidated projection.


## Publish

`publish` projects the append-only event log onto NATS JetStream —
the stream is a **view**, not a source of truth: it is fully
rebuildable from the Parquet log at any time, and re-publishing the
same log is a no-op (every message carries
`Nats-Msg-Id: <event_id>`, so JetStream absorbs duplicates
server-side within the stream's duplicate window):

```bash
debian-observer publish --events-dir data/parquet \
  [--stream debian-events] [--server nats://localhost:4222]
```

`--events-dir` is required; `--stream` defaults to `debian-events`
and `--server` to `nats://localhost:4222` (repeatable). The stream
is created if missing with the ADR-004 topology. Subjects are
**flat per event type** — `debian.package.observed`,
`debian.package_version.observed`, `debian.release.observed`,
`debian.migration.observed` — no per-package partitioning: the
package id travels in the payload (ADR-004 amendment, 2026-10-23).
Echo: files read, events read, per-subject publish counts,
duplicates absorbed, stream name and servers.


## Consume (external recipe — no CLI)

The tool ships **no `consume` subcommand**: a queue consumer is a
pipeline the researcher owns, not a command. Copy
`examples/consume_packages.py`, adjust the durable name, subject
and dataset dir (environment variables:
`DEBIAN_OBSERVER_{SERVER,STREAM,DURABLE,SUBJECT,DATASET_DIR}`),
and run it as its own process:

```bash
DEBIAN_OBSERVER_DATASET_DIR=data/normalized/package_versions \
  python examples/consume_packages.py
```

The queue state lives **server-side** in JetStream under the durable
name (plus the dataset table itself): `kill -9` the process and
JetStream redelivers the unacked messages — the consolidation merge
absorbs the duplicates (idempotent fold), so the consumer resumes
where it left off. The ack happens only **after** the durable table
write (ADR-005 amendment, 2026-10-23). `consolidate --events-dir`
remains the batch rebuild of the same table:
`fold(log parquet) == fold(stream)`.

To persist consumed events into Parquet event files (instead of
consolidating), register the library `PersistingHandler` yourself:

```python
from debian_observer.consumer_app import ConsumerApp
from debian_observer.storage.handlers import PersistingHandler

app = ConsumerApp()
handler = PersistingHandler("data/parquet", flush_threshold=100)
app.register_handler("PackageVersionObserved", handler)
```


## Messaging (NATS JetStream)

Events are published to NATS JetStream for durable messaging.

### Docker Compose

Start NATS:

```bash
cd deploy/compose
docker compose up -d nats
```

Verify:

```bash
curl http://localhost:8222/healthz
```

### Replay events

Re-publish events to NATS (dry-run by default):

```bash
debian-observer replay --input events.jsonl --dry-run
```

### Consume and persist to storage

The :class:`PersistingHandler` connects a consumer to Parquet storage
(a library recipe — see the external example above; there is no
`consume` CLI).


## API Endpoints

### Serve the API

Start the FastAPI service with data loaded from Parquet:

```bash
debian-observer serve \
  --events-dir data/events \
  --host 0.0.0.0 \
  --port 8088
```

Without `--events-dir`, the API starts with an empty store (useful for testing), unless the `EVENTS_DIR` environment variable is set.

### Available endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Service health check |
| `/metrics/` | GET | Available telemetry instruments |
| `/packages/{name}` | GET | Package detail |
| `/packages/{name}/versions` | GET | Package versions |
| `/releases/` | GET | List releases |
| `/migrations/` | GET | List migrations |
| `/datasets/` | GET | List dataset manifests |
| `/events/{event_id}` | GET | Event by ID |


## Testing

All features include tests. The test suite uses `pytest` with coverage reporting.

### Dev Container (recommended)

Open the project in VS Code with the Dev Container extension. Dependencies are pre-installed and tests run automatically on container creation.

```bash
pytest tests/ -q
```

### Local virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest tests/
```

### Consumer testing

The queue consumer is exercised through the external recipe (see
Consume above): start NATS, publish a small log, run the example
process. The crash test scripts it end to end:

```bash
scripts/crash-test-consumer.sh
```

In the devcontainer, NATS is available on `localhost:4222`.


## Troubleshooting

**No Parquet files found:** ensure `--events-dir` matches where ingestion writes.

**NATS connection refused:** verify `docker compose ps` shows nats healthy.

