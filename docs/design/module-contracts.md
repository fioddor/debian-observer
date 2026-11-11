# Module Contracts

What each top-level module exposes, expects, and guarantees. Signatures
live in the code; this page records the *promise* — the part a reader
cannot reconstruct without reading everything.

## `ingestion`

- **Exposes**:
   Data-source adapters (UDD PostgreSQL <https://udd.debian.org/>,
   Debian mirror, sources.debian.org, mock) and the `ingest` pipeline:
   - Raw artifacts as xz-compressed CSV
   - JSON manifest persisted beside each artifact + provenance record
   - The udddump download
     (`download_dump`: URL → byte-preserving dump file).
- **Expects**:
  - A source subcommand (`ingest sources | mirror | udd | udddump`)
    with its mandatory filter flags (ADR-012),
  - Optional row `--limit` window
  - UDD requires reachable PostgreSQL credentials.
  - `udddump` requires the **URL** of a `pg_dump` file (`--url`,
    mandatory scope selector).
- **Guarantees**:
  - Raw data is preserved verbatim (ADR-002) before any transformation
    with its manifest sidecar and a provenance record beside every
    artifact.
  - In particular, the `ingest udddump` dump is stored byte-preserving
    as downloaded (unread, suffix `.dump`, source name `udddump`,
    `record_count` null until normalization)
  - Read raw data is stored
    -  as RFC 4180 CSV compressed with LZMA
      (<https://www.rfc-editor.org/rfc/rfc4180>, suffix `.csv.xz`).
    - `None` is written as an empty field and read back lossless
       (sources emit no empty strings)

## `normalization`

- **Exposes**:
  - Transformer from raw source rows to canonical `PackageVersion`
    records and typed event payloads
  - Validators
  - The UDD `pg_dump` reader (see `normalization/udddump.py`):
    - `read_dump_records`
    - the TOC gate `list_dump_tables` and
    - the shape gate
      `peek_dump_copy_columns`/`assert_supported_table_shape`
  - The row iterator `iter_dump_rows` (ADR-015):
    - Full rows stream 1 by 1 through `subprocess.Popen`.
    - The consumer sees rows before the producer finishes.
  - The **eventizer registry** `normalization/eventizers` (ADR-014):
    `EVENTIZERS[source][name]`, `get_eventizer(source, name=None)`
    - Each eventizer declares `name`, `event_type`, `shape`
      (its own `TableShape`, checked by the shape gate) and
      `eventize(row)` over the **full row**.
    - Default `package-versions` (retro-compatible behaviour).
- **Expects**:
  - Raw rows in any of the known source layouts.
  - The family/suite/release semantics contract (see data model).
  - For `normalize udddump`, **pg_restore on PATH**.
    - `postgresql-client` shipped by the dev and deploy images.
    - `--table` is mandatory and validated against the dump's table
      of contents at startup (failure lists the restorable tables).
    - The table's COPY header must match a registered shape
      Today `packages` (required COPY column `version` plus any of
      `package`/`source`).
  - A manifest sidecar on the dump artifact
    (written by `ingest udddump`).
- **Guarantees**:
  - Usage:
    - Rows stream from `pg_restore --data-only --table`
      No database is created or restored (ADR-006).
    - The same raw dump artifact can be re-eventized with other
      filters (`--table`, `--limit`) without re-downloading.
      Each run re-reads the manifest sidecar for the source name
      and run id.
    - `--limit` unset normalizes the whole table
       with a stderr warning.
  - Errors:
    - The shape gate fails fast naming the cause and
      the supported shapes.
    - A record that fails canonical transformation is skipped
      with a warning naming the cause.
  - Payload auto-corrections:
    - `distribution` is always the archive family (`debian`/`ubuntu`)
      Never a suite or codename.
    - suite *roles* (stable, testing, unstable…) go to `suite`.
      A source that has no role information emits `suite=None`.
    - Codenames and pockets (bullseye, sid-security…) go to `release`

## `storage`

- **Exposes**:
  - writer-owned schema (ADR-011).
  - The event log writer.
  - The **per-event-type event log** (ADR-014/015).
  Details:
  - `EVENT_TYPE_SCHEMAS`
    - registers 1 Parquet schema per event type.
    - `event_type_directory` maps a type to its snake_case directory.
    - `_DEFAULT_SCHEMA` IS the `PackageVersionObserved` schema.
  - `writer_for_event_type(type, events_dir)`
    - writes `<events_dir>/<type_snake>/<run_id>.parquet`.
    - **one directory per event type**
    - **one Parquet file per ingestion run**.
    - the consolidation fold IS a writer of the consolidated table.
  - Two feeders, one semantics:
    - a) batch feeder
      - behind the `consolidate` CLI.
      - `fold_package_versions` / `build_consolidated_packages`
      - read the whole log, feed every event, write once
        pure regeneration, `merge_existing=False`
    - b) streaming feeder
      - behind the external example.
      - `ConsolidationMerge.feed` + `.write(dataset_dir)`.
      - one observation at a time, RMW of the existing table +
        atomic rename.
    - tested mandatory contract:
      Reads with a plain `read_parquet` glob, never `union_by_name`.
- **Expects**: normalized rows from the transformer; dataset layout
  as written by the writer; log rows read via `read_event_log`
  (sorted glob + DuckDB `union_by_name`) or envelope-derived rows
  via `row_from_envelope`.
- **Guarantees**: writer-owned serialization (ADR-011) — the event-log
  shape is the writer's decision, loaders must not reshape it. The
  API loader still reads Parquet event files written by earlier
  iterations (13- and 16-column layouts, before `release`, `component`,
  `section`, `priority`, `suite` existed); fields the old files lack
  load as `None`. The per-ingestion-run `package_versions` dataset
  writer is gone (ADR-003 amendment 2026-10-23): the consolidated table
  is the consumer's output, not a pipeline projection. The fold:
  one output row per full observation PK
  `(package_id, version, distribution, release, suite, architecture)`;
  attribute columns last-observed-wins (ordered by `observed_at`,
  then `event_id`); `observed_first_at`/`observed_last_at` are the
  min/max `observed_at` of the group; dedup by `event_id`; idempotent
  by construction (spec §15.1: A×3 == A) —
  `fold(log parquet) == fold(stream)`. The output is ONE file —
  `package_versions.parquet`, fixed name — written atomically (tmp
  file + `os.replace`), so a concurrent reader never sees a partial
  file. Clear errors, never tracebacks.

## `events`

- **Exposes**: `EventEnvelope` + strongly-typed payload models
  (`PackageObserved`, `PackageVersionObserved`, `ReleaseObserved`,
  `MigrationObserved`).
- **Guarantees**: every payload carries `ObservationMeta` provenance.
  Payloads serialize to JSON and flatten to Parquet-compatible rows.

## `messaging`

- **Exposes**:
  - NATS JetStream publisher/consumer/client + stream topology
    (`StreamConfig`; stream `debian-events`, flat subjects
    `debian.package.observed`, `debian.package_version.observed`,
    `debian.release.observed`, `debian.migration.observed`
     — per event type, no per-package partitioning)
  - `ensure_stream` (the stream created if missing).
  Notes:
  - `EventPublisher.publish` passes `headers` through
  - The publish CLI sends `Nats-Msg-Id: <event_id>` for server-side
    dedup (ADR-004 amendment, 2026-10-23).
  - The most recent `PubAck` is kept on `last_ack`
    (carries `duplicate` when the server reports it).
- **Guarantees**:
  - Delivery semantics (ADR-005).
  - JetStream persistence (ADR-004).

## `consumer_app`

- **Exposes**: standalone async consumer entrypoint
  - handler registration (the handler receives only the
    `EventEnvelope`),
  - idempotent processing (`IdempotencyStore`),
  - graceful lifecycle shutdown,
  - ack strictly after the handler returns without
    exception (ADR-005 amendment, 2026-10-23).
- **Expects**:
  - Registered handlers by event type.
  - JetStream endpoints
  - A handler that makes its effect durable before
    returning (the example's `merge.feed` + `merge.write`).

## `consumer`

- **Dead** (2026-10-23): the consolidation fold moved to
  `storage.consolidation` (the fold is a writer of the consolidated
  table — ADR-011) and the queue consumer lives outside the CLI
  (`examples/consume_packages.py`, over `consumer_app` +
  `messaging`). Nothing is exposed here anymore.

## `api`

- **Exposes**:
  - FastAPI application
  - Parquet-backed store via DuckDB (ADR-003/010).
- **Expects**: datasets on disk as written by `storage`.
- **Guarantees**: read-only access to normalized data; no ingest path.

## `cli`

- **Exposes**: `click` commands; the operator's entry point into
  ingestion and the API service.
  - `ingest` is a command group with one subcommand per source:
    - `mirror`,
    - `udd`,
    - `udddump`: mandatory filter flags, ADR-012, URL-scoped.
  - `consolidate` is the batch feeder of `storage.consolidation`.
    It folds the event log into the consolidated `package_versions`
    table.
    - `--events-dir` is required,
    - `--dataset-dir` defaults to `data/normalized/package_versions`.
  - `publish` projects the event log onto NATS JetStream
    - `--events-dir` is required,
    - `--stream` defaults to `debian-events`,
    - `--server` defaults to `nats://localhost:4222`
    - Rebuildable via `Nats-Msg-Id` dedup (See ADR-004 amendment).
  - `validate` takes raw artifacts (positional, one or more) and
     reads each artifact's manifest sidecar for source and run id.
  - `normalize` is a command group with one subcommand per raw surface
    - (positional) `artifacts`: pipeline `.csv.xz` artifacts.
    - `udddump`: dump downloads with mandatory `--table` and
       optional `--limit`,
     - ADR-012 amendment 2026-10-23 introduced a breaking rename
       of the old `normalize <paths>` surface).
- **Guarantees**:
  - no `consume` subcommand (2026-10-23) — a queue
    consumer is a pipeline the researcher owns, not a command.

## `consumer_app`

- **Exposes**: standalone async consumer entrypoint
  - handler registration,
  - idempotent processing,
  - graceful lifecycle shutdown.
- **Expects**:
  - registered handlers by event type
  - JetStream endpoints.

## `idempotency`

- **Exposes**: in-memory `IdempotencyStore` (seen event IDs).
- **Guarantees**:
  - In-memory, per-process scope
  - a persistent backend is deliberately deferred to R2.

## `domain`

- **Exposes**: the canonical frozen entities (see
  [data model](data-model.md)).

## `common`

- **Exposes**: shared types and validators
  - Debian version-string syntax (Debian Policy Manual §5.6.12
    <https://www.debian.org/doc/debian-policy/ch-controlfields.html#s-f-version>,
    intentionally permissive regex), package-name validation.

## `observability`

- **Exposes**: OpenTelemetry tracing/metrics wiring (ADR-008) used
  across all components.


## `analytics`

- **Exposes**:
  - `ExecutionRecord` Pydantic model,
  - `ExecutionStore` DuckDB persistence,
  -  `MicroReporter` stdout summary.
- **Expects**: every instrumented pipeline run to supply an
  `ExecutionRecord` with run identifier, timing, throughput, and
  provenance paths.
- **Guarantees**:
  - runs are persisted to `execution_runs.duckdb` beside the raw data directory,
  - the table is self-documenting with `COMMENT ON COLUMN` for every field,
  - duplicate run IDs are replaced (idempotent writes),
  - the `execution-report` CLI command provides historical aggregate views with median percentiles.

## `prosumers`

- **Exposes**: The shipped example prosumer executables
    - Decoupled from the `debian-observer` package,
    - `consolidate.py`, `filter.py` as Click scripts.
    - Installed as the `debian-observer-consolidate` /
      `debian-observer-filter` commands.
    - They import the libraries from `debian_observer.prosumers`
      (`stream`, `cli`, `packages`) exactly as a user-built prosumer
      would
    - `src/prosumers/` collects example CLIs only, no importable
      library code.
- **Expects**: input records shaped by the prosumer contract —
  `--file-in`/`--stream-in` carrying `Record` envelopes (ND-JSON)
  or flat event-log rows (Parquet).
- **Guarantees**: examples stay self-contained — a user can copy one
  to start their own prosumer without reaching into
  `debian_observer` internals beyond the public libraries.
