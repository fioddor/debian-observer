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

**UDD public mirror (real PostgreSQL):**

For querying the actual UDD public mirror:

```bash
debian-observer ingest \
  --source udd \
  --url sources \
  --limit 3 \
  --raw-dir /tmp/udd-raw \
  --parquet-dir /tmp/udd-parquet
```

**Expected output (udd):**
```
Ingested 3 records
Run ID: run-xxxxxx
Checksum: ...
Events produced: 3
Parquet written: /tmp/udd-parquet/run-xxxxxx.parquet
```

**Verify outputs:**

Raw JSONL artifact:
```bash
cat /tmp/smoke-raw/sources.debian.org-*.jsonl | head -n 3
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

Transform raw JSONL records into canonical domain events:

```bash
cat records.jsonl | debian-observer normalize > events.jsonl
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
debian-observer validate --input data/raw/<file>.jsonl
```

Mock artifacts (`mock-debian-*.jsonl`) hold synthetic rows that
mirror the UDD `packages` shape, so `validate` counts them valid —
they are test rows, not real UDD data.

Run `debian-observer validate --help` for all options.

## Query stored data

```bash
debian-observer query \
  "SELECT package_id, version, distribution FROM events WHERE distribution = 'sid'" \
  --parquet-dir data/parquet
```

Data is stored in **Parquet** format with a **DuckDB** analytical layer for SQL queries.


## Pipeline

The ingestion pipeline integrates ParquetWriter for optional persistent storage. After running `debian-observer ingest`, raw artifacts and Parquet events are available in `data/`.


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


## API Endpoints

When the API service is running:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Service health check |
| `/metrics/` | GET | Available telemetry instruments |
| `/packages/` | GET | List packages |
| `/packages/{entity_id}` | GET | Package detail |
| `/releases/` | GET | List releases |
| `/migrations/` | GET | List migrations |
| `/datasets/` | GET | List dataset manifests |
| `/events/` | GET | List domain events |


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

