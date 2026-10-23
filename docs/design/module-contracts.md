# Module Contracts

What each top-level module exposes, expects, and guarantees. Signatures
live in the code; this page records the *promise* — the part a reader
cannot reconstruct without reading everything.

## `ingestion`

- **Exposes**: data-source adapters (UDD PostgreSQL
  <https://udd.debian.org/>, Debian mirror, sources.debian.org,
  mock) and the `ingest` pipeline: raw artifacts as xz-compressed CSV
  + JSON manifest persisted beside each artifact + provenance record
  + the udddump download (`download_dump`: URL → byte-preserving
  dump file).
- **Expects**: a source subcommand (`ingest sources | mirror | udd |
  udddump`) with its mandatory filter flags (ADR-012), optional row
  `--limit` window; UDD requires reachable PostgreSQL credentials;
  `udddump` requires the **URL** of a `pg_dump` file (`--url`,
  mandatory scope selector) and performs no parsing — **no
  pg_restore here** (the reading lives in `normalization`, see
  below).
- **Guarantees**: raw data is preserved verbatim (ADR-002) before any
  transformation — as RFC 4180 CSV compressed with LZMA
  (<https://www.rfc-editor.org/rfc/rfc4180>, suffix `.csv.xz`);
  `None` is written as an empty field and read back lossless (sources
  emit no empty strings); manifests are persisted beside every
  artifact; identity fields (incl. `component`, `release`, `section`,
  `priority`) survive acquisition; the `ingest udddump` dump is stored
  byte-preserving as downloaded (suffix `.dump`, source name
  `udddump`) with its manifest sidecar (`record_count` null until
  normalization) and a provenance record.

## `normalization`

- **Exposes**: transformer from raw source rows to canonical
  `PackageVersion` records and typed event payloads; validators; the
  UDD `pg_dump` reader (`read_dump_records` + the TOC gate
  `list_dump_tables` and the shape gate
  `peek_dump_copy_columns`/`assert_supported_table_shape` — see
  `normalization/udddump.py`).
- **Expects**: raw rows in any of the known source layouts; the
  family/suite/release semantics contract (see data model);
  **pg_restore on PATH** (`postgresql-client`; shipped by the dev and
  deploy images — the host stays clean) for `normalize udddump`,
  whose `--table` is mandatory (validated against the dump's table of
  contents at startup — fail-fast, lists the restorable tables) and
  whose table's COPY header must match a registered shape — today
  `packages` (required COPY column `version` plus any of
  `package`/`source`); a manifest sidecar on the dump artifact
  (written by `ingest udddump`).
- **Guarantees**: rows stream from `pg_restore --data-only --table`
  — no database is created or restored (ADR-006); the shape gate
  fails fast naming the cause and the supported shapes; the same raw
  dump artifact can be re-eventized with other filters (`--table`,
  `--limit`) without re-downloading — each run re-reads the manifest
  sidecar for the source name and run id; `--limit` unset normalizes
  the whole table with a stderr warning; a record that fails
  canonical transformation is skipped with a warning naming the
  cause.
- **Guarantees**: `distribution` is always the archive family
  (`debian`/`ubuntu`) — never a suite or codename; suite *roles*
  (stable, testing, unstable…) go to `suite`; codenames and pockets
  (bullseye, sid-security…) go to `release`; a source that has no role
  information emits `suite=None`.

## `storage`

- **Exposes**: the event log writer (one Parquet file per ingestion
  run; writer-owned schema, ADR-011) + analytical queries (DuckDB).
- **Expects**: normalized rows from the transformer; dataset layout as
  written by the writer.
- **Guarantees**: writer-owned serialization (ADR-011) — the event-log
  shape is the writer's decision, loaders must not reshape it. The
  API loader still reads Parquet event files written by earlier R1
  iterations (13- and 16-column layouts, before `release`, `component`,
  `section`, `priority`, `suite` existed); fields the old files lack
  load as `None`. The per-ingestion-run `package_versions` dataset
  writer is gone (ADR-003 amendment 2026-10-23): the consolidated table
  is the `consumer`'s output, not a pipeline projection.

## `consumer`

- **Exposes**: the `consolidate` fold — the local consumer that turns
  the append-only event log into the consolidated `package_versions`
  table (`fold_package_versions`, `write_consolidated_table`,
  `build_consolidated_packages`; CLI `consolidate --events-dir`,
  `--dataset-dir` default `data/normalized/package_versions`).
- **Expects**: an events dir of Parquet event-log files as written by
  `storage` (one file per ingestion run; the consumer reads the
  flattened envelope columns, `union_by_name` over the files —
  legacy layouts load missing fields as `None`).
- **Guarantees**: one output row per full observation PK
  `(package_id, version, distribution, release, suite, architecture)`;
  attribute columns last-observed-wins (ordered by `observed_at`, then
  `event_id`); `observed_first_at`/`observed_last_at` are the min/max
  `observed_at` of the group. The fold is idempotent by construction
  (spec §15.1): duplicated log files collapse, folding twice yields
  the same table. The output is ONE file — `package_versions.parquet`,
  fixed name — written atomically (tmp file + `os.replace`), so a
  concurrent reader never sees a partial file. Clear errors, never
  tracebacks: an events dir without Parquet files, or a log without
  `PackageVersionObserved` events, fails naming the cause.

## `events`

- **Exposes**: `EventEnvelope` + strongly-typed payload models
  (`PackageObserved`, `PackageVersionObserved`, `ReleaseObserved`,
  `MigrationObserved`).
- **Guarantees**: every payload carries `ObservationMeta` provenance;
  payloads serialize to JSON and flatten to Parquet-compatible rows.

## `messaging`

- **Exposes**: NATS JetStream publisher/consumer/client
  (`nats://…`, stream `EVENTS`, subject `debian.observer.>`).
- **Guarantees**: delivery semantics per ADR-005; JetStream persistence
  per ADR-004.

## `api`

- **Exposes**: FastAPI application; Parquet-backed store via DuckDB
  (ADR-003/010).
- **Expects**: datasets on disk as written by `storage`.
- **Guarantees**: read-only access to normalized data; no ingest path.

## `cli`

- **Exposes**: `click` commands; the operator's entry point into
  ingestion and the API service. `ingest` is a command group with one
  subcommand per source (`sources`, `mirror`, `udd`, `udddump`;
  mandatory filter flags, ADR-012 — for `udddump` the scope is the
  URL and the reading lives in `normalize`); `consolidate` folds the
  event log into the consolidated `package_versions` table
  (`--events-dir` required, `--dataset-dir` default
  `data/normalized/package_versions`); `validate` takes raw
  artifacts (positional, one or more) and reads each artifact's
  manifest sidecar for source and run id; `normalize` is a command
  group with one subcommand per raw surface (`artifacts`: pipeline
  `.csv.xz` artifacts, positional; `udddump`: dump downloads with
  mandatory `--table` and optional `--limit`, ADR-012 amendment
  2026-10-23 — a breaking rename of the old `normalize <paths>`
  surface).

## `consumer_app`

- **Exposes**: standalone async consumer entrypoint — handler
  registration, idempotent processing, graceful lifecycle shutdown.
- **Expects**: registered handlers by event type; JetStream endpoints.

## `idempotency`

- **Exposes**: in-memory `IdempotencyStore` (seen event IDs).
- **Guarantees**: R1 scope only — in-memory, per-process; a persistent
  backend is deliberately deferred to R2.

## `domain`

- **Exposes**: the canonical frozen entities (see
  [data model](data-model.md)).

## `common`

- **Exposes**: shared types and validators — Debian version-string
  syntax (Debian Policy Manual §5.6.12
  <https://www.debian.org/doc/debian-policy/ch-controlfields.html#s-f-version>,
  intentionally permissive regex for R1), package-name validation.

## `observability`

- **Exposes**: OpenTelemetry tracing/metrics wiring (ADR-008) used
  across all components.
