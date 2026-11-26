# User Guide

Operational guide for the Debian Observer CLI.

For other documentation start at [README](../README.md).

## Table of contents

Installation

  - 3 alternatives: [source code](#a-from-source), [pipx](#b-pipx), [Docker compose](#c-docker-compose)
  - [Quick checks](#smoke-tests-with-real-debian-data)

Data Processing

  1. [Ingestion](#ingestion) into raw records
  2. [Validation](#validation-of-raw-records) of raw records
  3. [Normalization](#normalization) of raw records
  4. [Query](#query-stored-data) stored data
  5. [Prosumers](#prosumers-pipeable-processors) (pipeable processors)
     - Examples: [Consolidate](#example-debian-observer-consolidate), [Filter](#example-debian-observer-filter)
     - [Queue consumer](#queue-consumer-nats)
     - [Testing](#testing)
  6. [Publication](#publication)

[Pipeline Data Layout](#pipeline-data-layout)

Observability:

  - (Debian) Domain [observability](#observability)
  - Platform [execution analytics](#execution-analytics)

[Troubleshooting](#troubleshooting)


## Installation

### A) From source

```shell
git clone <repository>
cd debian-observer
python3 -m venv .your.venv
source .your.venv/bin/activate
pip install -e "."
```

### B) pipx

Wheels are build artifacts (never committed). Build one from the
repo root (see docs/CONTRIBUTING.md, Building/Packaging):

```shell
python3 -m pip install --upgrade build
python3 -m build
pipx install dist/debian_observer-<version>-py3-none-any.whl
```

### C) Docker (Compose)

```shell
cd deploy/compose
docker compose up -d
```

Both the `api` service (port 8088, `debian-observer serve`) and the
`debian-observer` CLI service run the image built from the sources
(`pip install -e .` at `/app`). The CLI service bind-mounts the repo
over `/app`, so it runs your working tree — no wheel needed:

```shell
docker exec -ti debian-observer bash
debian-observer --help
```

## Smoke Tests with Real Debian Data

### Sources.debian.org

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

**Verify outputs:**

Raw artifact (xz-compressed CSV):
```bash
xz -dc /tmp/smoke-raw/sources.debian.org-*.csv.xz | head -n 3
```

Parquet schema (`read_table` takes the directory, not a glob):
```bash
python3 -c "
import pyarrow.parquet as pq
table = pq.read_table('/tmp/smoke-events')
print(f'Rows: {table.num_rows}, Cols: {table.num_columns}')
print(f'Columns: {table.column_names}')
"
```

### Mirror Package List (Packages.gz)

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

### UDD Public Mirror

For querying the actual UDD public mirror:

```bash
debian-observer ingest udd \
  --table packages \
  --limit 3 \
  --raw-dir /tmp/udd-raw \
  --events-dir /tmp/udd-events
```

This queries the `packages` table from `udd-mirror.debian.net:5432`.
`--table` is required. The `packages` table includes package
(binary), source, version, distribution, release (suite/pocket),
architecture, component, section and priority columns.

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

### UDD Dump File (no live database)

`ingest udddump` is **acquisition only** ([ADR-012][ADR-012] amendment,
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

### Eventize a Table of the Dump (`normalize udddump`)

The reading is normalization's job: `normalize udddump` loads the
artifact's manifest sidecar (source and run id), validates `--table`
against the dump's table of contents (the error lists the restorable
tables) and the table's COPY header against the registered shapes,
then streams the COPY block through `pg_restore --data-only --table`
**on the host** (`pg_restore` on PATH; the dev and deploy images ship
`postgresql-client`) — no database connection, no restore, and no
container indirection ([ADR-006][ADR-006]) — and writes the events to the
Parquet observation event log under `--events-dir`
(default `data/observed`): one directory per event type, one file per run
(the prosumers' input). `--limit` unset normalizes the whole table with a
warning on stderr.

The Parquet log is always written (`--events-dir` has a default).
`--output FILE` adds a JSON-lines copy of the same events (`-` for
stdout); without it, nothing goes to stdout but the summary on
stderr.

```bash
debian-observer normalize udddump \
  data/raw/udddump-20261023-101500-run-9f2c1ab34d5e.dump \
  --table packages \
  --limit 3
# -> data/observed/package_version_observed/<run_id>.parquet
```

Re-eventizing the same artifact with other filters (`--table`,
`--limit`) is supported: the raw download is not consumed, and each
run re-reads the manifest sidecar.

The event type comes from `--eventizer` (one of `dependencies`,
`package-versions` — the default — and `popcon`):

- `package-versions`: `PackageVersionObserved`, one event per row.
  Required COPY columns: `version` plus any of `package`/`source`
  (the `packages` shape).
- `dependencies`: `DependencyObserved`, one event per
  `depends`/`pre_depends` clause (the `packages` table carries
  both columns). Required COPY columns: `package`, `version`,
  `architecture`, `distribution`, `release`, `depends`,
  `pre_depends`.
- `popcon`: `PopconObserved`, for the UDD `popcon` table. Required
  COPY columns: `package`, `insts`.

```bash
debian-observer normalize udddump \
  data/raw/udddump-20261023-101500-run-9f2c1ab34d5e.dump \
  --table packages \
  --eventizer dependencies \
  --events-dir data/observed
```

Records that fail canonical transformation are skipped with a warning
naming the cause — the raw artifact preserves them ([ADR-002][ADR-002]).


## Ingestion

For larger datasets (e.g. complete UDD), the acquisition layer supports configurable sources.

```bash
debian-observer ingest udd \
  --table packages \
  --raw-dir data/raw \
  --events-dir data/observed
```

Ingestion options across sources: `--limit` (mirror/udd; unset ingests
the whole source with a warning), `--component`/`--architecture`
(mirror; default `main`/`amd64`), `--events-dir` (event log output,
default `data/observed`). See `debian-observer ingest <source> --help`
for full options and `debian-observer --help` for the command list.

## Validation of raw records

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

## Normalization

Transform raw artifacts into canonical domain events. `normalize` is
a command group with one subcommand per raw surface ([ADR-012][ADR-012]
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

Each `normalize` invocation records a `normalization` run (records,
events, duration, throughput) into `execution_runs.duckdb` beside the
directory of the artifacts it reads (see
[Pipeline Data Layout](#pipeline-data-layout); `ingest` records
into `<raw_dir>/../execution_runs.duckdb`) — the same analytics
database `execution-report` reads; see
[Execution analytics](#execution-analytics).


## Query stored data

Run DuckDB SQL queries over ingested Parquet events:

```bash
debian-observer query \
  "SELECT package_id, version, release FROM events WHERE release = 'sid'" \
  --events-dir data/observed
```

**Aggregations:**

```bash
debian-observer query \
  "SELECT architecture, COUNT(*) as count FROM events GROUP BY architecture ORDER BY count DESC" \
  --events-dir data/observed
```

**Output formats:**

| Format | Flag             | Use case |
|:-------|:-----------------|----------|
| Table  | `--format table` (default) | Human-readable |
| JSON   | `--format json`  | Machine parsing |
| CSV    | `--format csv`   | Spreadsheet import |

**Example: export to CSV**

```bash
debian-observer query                                  \
  "SELECT entity_id, version, observed_at FROM events" \
  --events-dir data/observed                           \
  --format csv > events.csv
```

Data is stored in **Parquet** format with a **DuckDB** analytical layer for
SQL queries.


## Prosumers (Pipeable Processors)

The prosumers are stand-alone CLI tools that read and write
observation events via a common pipeable contract. They are
intended for ad-hoc analytical pipelines and for researchers who
want to compose their own event-processing steps without writing
Python code.

The product ships two kinds of prosumer code, in separate places:

  - **Libraries** (`src/debian_observer/prosumers/`):
    The importable building blocks to fabricate your own prosumer
    - `stream` (`Record`, `Source`/`Sink`, file and ND-JSON stream variants,
      `open_source`/`open_sink`),
    - `cli` (`common_io_options()` and the `build_source`/`build_sink`
      flag-to-IO factories) and
    - `packages` (the `fold_package_versions` consolidation engine).
    Import them as `from debian_observer.prosumers import ...`.
  - **Examples** (`src/prosumers/`):
    The shipped example executables, `consolidate.py` and `filter.py` —
    decoupled from the `debian-observer` package, installed as the
    `debian-observer-consolidate` and `debian-observer-filter` commands.
    Each is a self-contained Click script built on the libraries above:
    copy one as a starting point for your own prosumer.

Every prosumer accepts at least one input and at least one output
from the following four options:

| Flag              | Meaning                         |
|:------------------|:--------------------------------|
| `--file-in PATH`  | Read Parquet events from disk   |
| `--file-out PATH` | Write Parquet events to disk    |
| `--stream-in`     | Read ND-JSON records from stdin |
| `--stream-out`    | Write ND-JSON records to stdout |

- `--file-in` takes a Parquet file, a glob or a directory (its
  `*.parquet` files, no recursion): `data/observed` after `ingest`,
  `data/observed/package_version_observed` after `normalize udddump`.
- `--stream-in` reads one JSON record per line: the prosumer records
  another prosumer's `--stream-out` writes, or the event JSON lines
  `normalize` writes.
- `--file-out` writes one Parquet file atomically (tmp file + rename);
  a directory target gets a `sink.parquet` inside.

### Example: `debian-observer-consolidate`

Folds the `PackageVersionObserved` events of its input into the
consolidated `PackageVersionConsolidated` table (other event types
are ignored; an input without any fails with
`No PackageVersionObserved events found.`):

- One row per full observation key
  `(package_id, version, distribution, release, suite, architecture)`.
- Attribute columns (`source_package`, `component`, `section`,
  `priority`, `source`) are last-observed-wins, ordered by
  `(observed_at, event_id)`.
- `observed_first_at`/`observed_last_at` are the min/max `observed_at`
  of the group (in `_meta` on `--stream-out`).
- The fold is idempotent: duplicate `event_id` values collapse, so
  folding the same events twice — or duplicated log files — yields
  the same table.
- A summary goes to stderr:
  `Events read: N | PVO events: N | Consolidated rows: N`.

```bash
# File to file: rebuild the consolidated table from the event log
debian-observer-consolidate --file-in data/observed \
  --file-out data/normalized/package_versions/package_versions.parquet

# Stream to stream: fold the events normalize writes
debian-observer normalize artifacts data/raw/<file>.csv.xz | \
  debian-observer-consolidate --stream-in --stream-out > consolidated.ndjson

# File to stream: inspect the folded rows as ND-JSON
debian-observer-consolidate --file-in data/observed --stream-out
```

Query the consolidated table with DuckDB ([ADR-003][ADR-003] example, on the
dataset schema) — or with `debian-observer query`, whose `--dataset-dir`
defaults to `data/normalized/package_versions`:

```sql
SELECT package_id, version, release
FROM read_parquet('data/normalized/package_versions/*.parquet')
WHERE release = 'sid'
```

The table is **state, not log**: it is fully derived from the event log and
can be re-generated at any time. The event log remains the replay surface while
the dataset is the consolidated projection.

### Example: `debian-observer-filter`

Pass-through filter for observation events. Accepts repeatable
`--distribution` and `--release` flags (AND logic). Records without
a `distribution` value (not package observations) pass through
unchanged; without any flag everything passes, with a warning.

```bash
# Keep only Debian sid observations
debian-observer-filter --file-in data/observed \
                       --file-out sid.parquet   \
                       --distribution debian --release sid

# Pipe: filter, then consolidate what passed
debian-observer-filter --file-in data/observed --stream-out --distribution debian | \
debian-observer-consolidate --stream-in --file-out debian-only.parquet
```

Filter first: `debian-observer-consolidate` folds observation events,
so it cannot take its own consolidated output as input.

### Queue consumer (NATS)

A queue consumer is a pipeline the researcher owns: the tool ships
no `consume` subcommand. `examples/consume_packages.py` is the
recipe — it folds `debian.package_version.observed` off JetStream
into the consolidated `package_versions` table:

1. Copy it into your own project as a starting point.
2. Adjust the server, stream, durable name, subject and dataset dir
   (environment variables
   `DEBIAN_OBSERVER_{SERVER,STREAM,DURABLE,SUBJECT,DATASET_DIR}`), and
   run it as its own process:

   ```bash
   DEBIAN_OBSERVER_DATASET_DIR=data/normalized/package_versions \
     python examples/consume_packages.py
   ```

The queue state lives **server-side** in JetStream under the durable name
(plus the dataset table itself): `kill -9` the process and JetStream redelivers
the unacked messages. The consolidation merge absorbs the duplicates
(idempotent fold), so the consumer resumes where it left off, and the
ack happens only **after** the durable table write ([ADR-005][ADR-005]
amendment of 2026-10-23): a crash before the ack means redelivery, not
loss.

#### Consume and persist to storage

To persist consumed events into Parquet event files (instead of
consolidating), register the library `PersistingHandler` yourself:

```python
from debian_observer.consumer_app import ConsumerApp
from debian_observer.storage.handlers import PersistingHandler

app = ConsumerApp()
handler = PersistingHandler("data/observed", flush_threshold=100)
app.register_handler("PackageVersionObserved", handler)
```

### Testing

All features include tests. The test suite uses `pytest` with coverage reporting.

#### Dev Container (recommended)

Open the project in VS Code with the Dev Container extension. Dependencies are pre-installed and tests run automatically on container creation.

```bash
pytest tests/ -q
```

#### Local virtual environment

```bash
python3 -m venv .your.venv
source .your.venv/bin/activate
pip install -e ".[dev]"
pytest tests/
```

#### Consumer testing

The queue consumer is exercised through the external recipe (see
[Queue consumer](#queue-consumer-nats)): start NATS, publish a small log, run the example
process. The crash test scripts it end to end:

```bash
scripts/crash-test-consumer.sh
```

NATS is not part of the devcontainer: the devcontainer image ships
the NATS CLI only. Start NATS from deploy/compose —
`docker compose up -d nats` — ports 4222/8222 are published on the
host (NATS on `localhost:4222`).


## Publication

`publish` projects the append-only observation event log onto NATS JetStream
for durable messaging.

The stream is a **view**, not a source of truth: it is fully rebuildable
from the Parquet log at any time, and re-publishing the same log is a no-op
(every message carries `Nats-Msg-Id: <event_id>`, so JetStream absorbs
duplicates server-side within the stream's duplicate window).

### Messaging (NATS JetStream)

Publishing requires NATS JetStream to be available.

Start NATS:

```bash
cd deploy/compose
docker compose up -d nats
```

Verify:

```bash
curl http://localhost:8222/healthz
```

### Publish

```bash
debian-observer publish --events-dir data/observed \
  [--stream debian-events] [--server nats://localhost:4222]
```

  - `--events-dir` is required
  - `--stream` defaults to `debian-events` and `--server` to
    `nats://localhost:4222` (repeatable).

The stream is created if missing with the [ADR-004][ADR-004] topology.
Subjects are **flat per event type**:

  - `debian.package.observed`,
  - `debian.package_version.observed`,
  - `debian.release.observed`,
  - `debian.migration.observed`,
  - `debian.dependency.observed`,
  - `debian.popcon.observed`

No per-package partitioning: the package id travels in the payload
([ADR-004][ADR-004] amendment, 2026-10-23).

Echo: files read, events read, per-subject publish counts,
duplicates absorbed, stream name and servers.

### Publish the britney pairs (`normalize britney --publish`)

`normalize britney --publish` publishes the eventized pairs of an
ingested window to a second, dedicated stream — `debian-migrations`
(ADR-026) — while it writes the Parquet event lake:

```bash
debian-observer normalize britney <manifest> \
  [--events-dir DIR] [--publish] [--server nats://localhost:4222]
```

  - `--publish` is optional: without it the run stays C2's
    (lake only, nothing connected).
  - `--server` names the NATS URL(s) for `--publish` (repeatable,
    default `nats://localhost:4222`).
  - every message carries `Nats-Msg-Id: <event_id>`, so a re-run of a
    published window publishes the same ids again and the stream
    absorbs them (server-side dedup within the stream's duplicate
    window — an optimization; the guarantee stays in the consumers,
    which deduplicate by `event_id`).

Subjects are `debian.migration.excuse.<kind>` and
`debian.migration.snapshot.observed` (spec §6.1); the existing
`debian.migration.observed` stays on `debian-events`, untouched.
The stream is created if missing with its own retention: file
storage, limits retention, 400 days / 2 GB (D3), a duplicate window
of hours, and no message-count limit (age and bytes are the disk
budget). The pairs publish in order: the events of a pair only after
every event of the previous pair (ADR-027).

### Replay events

Replay events from a JSON-lines file (`--input`, required) to NATS.

Publishing happens unless `--dry-run` is given (parse only, no publish).

Other options:

  - `--nats-url` (default `nats://localhost:4222`),
  - `--create-stream` (create the JetStream stream if missing),
  - `--timeout` (default 5 s).

```bash
debian-observer replay --input events.jsonl [--dry-run] \
  [--nats-url nats://localhost:4222] [--create-stream]
```

### Serving via API

Start the FastAPI service with data loaded from Parquet:

```bash
debian-observer serve \
  --events-dir data/observed \
  --host 0.0.0.0 \
  --port 8088
```

Without `--events-dir`, the API starts with an empty store (useful for
testing), unless the `EVENTS_DIR` environment variable is set.

#### Available endpoints

| Endpoint                 | Method | Description |
|:-------------------------|--------|-------------|
| `/health`                   | GET | Service health check |
| `/metrics/`                 | GET | Available telemetry instruments |
| `/packages/{name}`          | GET | Package detail |
| `/packages/{name}/versions` | GET | Package versions |
| `/releases/`                | GET | List releases |
| `/migrations/`              | GET | List migrations |
| `/datasets/{dataset_name}`  | GET | Manifest of one dataset (404 if absent) |
| `/events/{event_id}`        | GET | Event by ID |

### Services

| Service         | Port | Purpose |
|:----------------|------|---------|
| NATS            | 4222 | JetStream event backbone |
| NATS monitoring | 8222 | Health and metrics |
| API             | 8088 | FastAPI analytical endpoints |


## Execution analytics

`ingest`, `normalize` and `benchmark` record each run (records,
events, duration, throughput) into an `execution_runs.duckdb`
analytics database (see [Pipeline Data Layout](#pipeline-data-layout)).

```bash
# What is in the event log
debian-observer status --events-dir data/observed

# A quick synthetic ingestion run (writes raw artifacts to --raw-dir)
debian-observer benchmark --records 1000 --raw-dir data/raw

# Runs of the last 7 days: counts and median duration/throughput
debian-observer execution-report --days 7 [--type ingestion] \
  [--source udd] [--format table|json|csv] [--db data/execution_runs.duckdb]
```


## Pipeline Data Layout

The ingestion pipeline integrates ParquetWriter for persistent storage.

- After running `debian-observer ingest`, raw artifacts (`.csv.xz`, dump files)
  and their run manifest sidecars (JSON, `*.manifest.json` next to each raw
  artifact) live under `--raw-dir` (default `data/raw`).
- Observation event log
  - from `ingest <source> --events-dir DIR`: ONE flat Parquet file per run,
    `DIR/<run_id>.parquet` (default `data/observed`).
  - from `normalize udddump --events-dir DIR`: one directory per event type,
    one `DIR/<event_type_snake>/<run_id>.parquet` file per run (e.g.
    `data/observed/package_version_observed/`,
    `data/observed/dependency_observed/`). Ingestion eventizes raw observations
    into the append-only observation event log as one Parquet file per
    ingestion run.
- `publish`, `query` (events view), `serve` and the prosumers'
  `--file-in` read `*.parquet` directly inside the given directory (no
  recursion): after `normalize udddump`, point them at the event-type
  directory, e.g. `--events-dir data/observed/package_version_observed`.
- Consolidated datasets are optional derived state: the
  `package_versions` table goes to
  `data/normalized/package_versions/package_versions.parquet`
  (`debian-observer-consolidate --file-out`), the default
  `query --dataset-dir`.
- Execution analytics:
  - each `ingest` run is recorded into `<raw_dir>/../execution_runs.duckdb`
    (`data/execution_runs.duckdb` with defaults — the `execution-report --db`
    default)
  - each `normalize` run into `execution_runs.duckdb` beside the directory
    of the artifacts it reads.


## Observability

OpenTelemetry tracing and metrics are wired throughout the pipeline,
messaging and the API; the API exposes `/metrics/`. The exporters are
the console ones: metrics export every 5 seconds while a process runs,
and spans and metrics flush at process exit — on **stderr**. Commands
that record telemetry (e.g. `ingest`, `benchmark`) keep their stdout
clean: it carries only the command's own output, so it is safe to
pipe or redirect. The JSON telemetry blocks appear on stderr instead.


## Troubleshooting

**No Parquet files found:** ensure `--events-dir` matches where ingestion writes.

**NATS connection refused:** verify `docker compose ps` shows nats healthy.



[ADR-002]: architecture/adr/ADR-002-raw-data-preservation.md
[ADR-003]: architecture/adr/ADR-003-parquet-duckdb.md
[ADR-004]: architecture/adr/ADR-004-nats-jetstream.md
[ADR-005]: architecture/adr/ADR-005-event-delivery-semantics.md
[ADR-006]: architecture/adr/ADR-006-no-postgresql.md
[ADR-012]: architecture/adr/ADR-012-cli-ingest-ux.md
