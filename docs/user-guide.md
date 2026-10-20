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
| API | 8000 | FastAPI analytical endpoints |


## Quick Start: Smoke Test with Real Debian Data

For a quick end-to-end test using a real public Debian data source:

```bash
debian-observer ingest \
  --source sources \
  --url hello \
  --raw-dir /tmp/smoke-raw \
  --parquet-dir /tmp/smoke-parquet
```

This fetches version history for the `hello` package from `sources.debian.org`.

**Expected output (sources.debian.org):**
```
Ingested N records
Run ID: run-xxxxxx
Checksum: ...
Events produced: N
Parquet written: /tmp/smoke-parquet/run-xxxxxx.parquet
```

**Mirror package list (Packages.gz):**

For a quick test with actual Debian Archive `Packages.gz` data:

```bash
debian-observer ingest \
  --source mirror \
  --url stable \
  --limit 5 \
  --raw-dir /tmp/mirror-raw \
  --parquet-dir /tmp/mirror-parquet
```

**Expected output (mirror):**
```
Ingested 5 records
Run ID: run-xxxxxx
Checksum: ...
Events produced: 5
Parquet written: /tmp/mirror-parquet/run-xxxxxx.parquet
```

Mirror records carry `distribution` as the archive family (`debian`,
or `ubuntu` for Ubuntu mirrors), and split the `--url` suite path:
suite roles (`oldstable`, `stable`, `testing`) land in `suite`,
codenames and pockets (`bookworm`, `sid`, `bookworm-security`) land in
`release`.

**UDD public mirror (real PostgreSQL):**

For querying the actual UDD public mirror:

```bash
debian-observer ingest \
  --source udd \
  --url packages \
  --limit 3 \
  --raw-dir /tmp/udd-raw \
  --parquet-dir /tmp/udd-parquet
```

This queries the `packages` table from `udd-mirror.debian.net:5432`.
The default table is `packages`, which includes package (binary), source, version, distribution, release (suite/pocket), architecture, component, section and priority columns.

**Expected output (udd):**
```
Ingested 3 records
Run ID: run-xxxxxx
Checksum: ...
Events produced: 3
Parquet written: /tmp/udd-parquet/run-xxxxxx.parquet
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
  debian-observer ingest \
  --source udd \
  --url packages \
  --limit 3 \
  --raw-dir /tmp/udd-raw \
  --parquet-dir /tmp/udd-parquet
```

**Verify outputs:**

Raw artifact (xz-compressed CSV):
```bash
xz -dc /tmp/smoke-raw/sources.debian.org-*.csv.xz | head -n 3
```

Parquet schema:
```bash
python3 -c "
import pyarrow.parquet as pq
table = pq.read_table('/tmp/smoke-parquet/run-*.parquet')
print(f'Rows: {table.num_rows}, Cols: {table.num_columns}')
print(f'Columns: {table.column_names}')
"
```


## Production Ingest: Full Dataset

For larger datasets (e.g. complete UDD), the acquisition layer supports configurable sources.

```bash
debian-observer ingest \
  --source udd \
  --url "https://udd.debian.org/dmd/?" \
  --raw-dir data/raw \
  --parquet-dir data/parquet
```

### Verify:

```bash
debian-observer --help
```

## Quick Start

Run a basic ingestion using a configured data source:

```bash
debian-observer ingest \
  --source sources \
  --url <package> \
  --raw-dir data/raw
```

Options: `--limit` (max records), `--parquet-dir` (Parquet output).
See `debian-observer ingest --help` for full options.

## Observability

OpenTelemetry tracing and metrics are wired throughout the pipeline,
messaging and the API; the API exposes `/metrics/`. Console telemetry
output appears only for long-lived processes: metrics export every 5
seconds while a process runs, and traces flush at process exit —
short-lived CLI commands show no telemetry output.


## Normalization

Transform raw records into canonical domain events. Raw inputs
are JSON lines (stdin or `.jsonl`) or xz-compressed CSV
artifacts (`.csv.xz`):

```bash
debian-observer normalize --input data/raw/<file>.csv.xz > events.jsonl
```

Run `debian-observer normalize --help` for all options (source, run
id, invalid-records file).


## Validate raw records

Validate raw records against the expected UDD shape before
normalization: a record needs a `version` and a `package` or
`source` name. Invalid records are reported with a reason on
stderr and the run prints valid/invalid counts; the exit code
stays 0 even when invalid records are found — validation is a
report, not a gate.

```bash
debian-observer validate --input data/raw/<file>.csv.xz
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
  --parquet-dir data/parquet
```

**Aggregations:**

```bash
debian-observer query \
  "SELECT architecture, COUNT(*) as count FROM events GROUP BY architecture ORDER BY count DESC" \
  --parquet-dir data/parquet
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
  --parquet-dir data/parquet \
  --format csv > events.csv
```

Data is stored in **Parquet** format with a **DuckDB** analytical layer for SQL queries.


## Pipeline

The ingestion pipeline integrates ParquetWriter for optional persistent storage. After running `debian-observer ingest`, raw artifacts
(xz-compressed CSV, `*.csv.xz`), the run manifest (JSON,
`*.manifest.json` next to each raw artifact) and Parquet events
are available in `data/`.

The pipeline can also project `PackageVersionObserved` events into the normalized dataset (spec §10.2, ADR-003, ADR-011):

```bash
debian-observer ingest --source udd --url packages --limit 400 \
  --dataset-dir data/normalized/package_versions
```

The dataset holds one row per observed package version (query surface); the event log remains the replay surface. Query it with DuckDB (ADR-003 example, on the dataset schema):

```sql
SELECT package_id, version, release
FROM read_parquet('data/normalized/package_versions/*.parquet')
WHERE release = 'sid'
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

The :class:`PersistingHandler` connects the consumer to Parquet storage.
Events consumed from NATS are buffered and written to Parquet files:

```python
from debian_observer.consumer_app import ConsumerApp
from debian_observer.storage.handlers import PersistingHandler

app = ConsumerApp()
handler = PersistingHandler("data/parquet", flush_threshold=100)
app.register_handler("PackageVersionObserved", handler)
```

Or via the `consume` CLI (writes to `data/parquet` by default):

```bash
debian-observer consume \
  --stream debian.events \
  --subject "debian.package_version.observed" \
  --durable storage-consumer
```


## API Endpoints

### Serve the API

Start the FastAPI service with data loaded from Parquet:

```bash
debian-observer serve \
  --parquet-dir data/parquet \
  --host 0.0.0.0 \
  --port 8000
```

Without `--parquet-dir`, the API starts with an empty store (useful for testing), unless the `PARQUET_DIR` environment variable is set.

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

The standalone event consumer can be exercised via CLI:

```bash
debian-observer consume --stream EVENTS --subject "debian.observer.>" --durable test-consumer
```

In the devcontainer, NATS is available on `localhost:4222`.


## Troubleshooting

**No Parquet files found:** ensure `--parquet-dir` matches where ingestion writes.

**NATS connection refused:** verify `docker compose ps` shows nats healthy.

