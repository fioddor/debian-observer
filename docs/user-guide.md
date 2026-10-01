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

### Verify:

```bash
debian-observer --help
```

## Quick Start

Run a basic ingestion using a configured data source:

```bash
debian-observer ingest \
  --source mock \
  --raw-dir data/raw
```

See `debian-observer ingest --help` for full options.

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

## Testing

All features include tests. The test suite uses `pytest` with coverage reporting.

### Local virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest tests/
```

