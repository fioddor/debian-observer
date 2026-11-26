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
    - The table's COPY header must match the shape declared by the
      selected eventizer: `package-versions` expects the `packages`
      shape (required COPY column `version` plus `package`/`source`),
      `dependencies` expects `package`, `version`, `architecture`,
      `distribution`, `release`, `depends`, `pre_depends`, `popcon`
      expects `package`, `insts` (see `normalization/eventizers/`).
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
      - behind the example prosumer `debian-observer-consolidate`
        (batch: `--file-in <event log> --file-out
        data/normalized/package_versions/package_versions.parquet`).
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
  `fold(log parquet) == fold(stream)`. Note the two event-log
  layouts: `ingest --events-dir` appends a flat
  `<events_dir>/<run_id>.parquet` file (ParquetWriter in
  `ingestion/pipeline.py`), while `normalize udddump --events-dir`
  writes the per-type directories above (`writer_for_event_type`).
  The output is ONE file —
  `package_versions.parquet`, fixed name — written atomically (tmp
  file + `os.replace`), so a concurrent reader never sees a partial
  file. Clear errors, never tracebacks.

## `events`

- **Exposes**: `EventEnvelope` + strongly-typed payload models
  (`PackageObserved`, `PackageVersionObserved`, `ReleaseObserved`,
  `MigrationObserved`, `DependencyObserved`, `PopconObserved`).
- **Guarantees**: every payload carries `ObservationMeta` provenance.
  Payloads serialize to JSON and flatten to Parquet-compatible rows.

## `messaging`

- **Exposes**:
  - NATS JetStream publisher/consumer/client + stream topology
    (`StreamConfig`; stream `debian-events`, flat subjects
    `debian.package.observed`, `debian.package_version.observed`,
    `debian.release.observed`, `debian.migration.observed`
     — per event type, no per-package partitioning)
  - a second stream `debian-migrations` (ADR-026): subjects
    `debian.migration.excuse.>` and `debian.migration.snapshot.>`,
    file storage, limits retention (400 days / 2 GB, D3), a
    duplicate window of hours, no message-count limit
    (`MigrationStreamConfig`; the per-event-type subject map
    stays single-sourced in the pair eventizer's `SUBJECTS`)
  - `ensure_stream` (the stream created if missing).
  - publication of the eventized pairs pair by pair
    (`messaging.migrations`: `Nats-Msg-Id = event_id`, a pair's
    events only after every event of the previous pair, ADR-027)
    and a durable replay of the migration stream from the start
    (deliver policy all, dedup by `event_id` in the consumer).
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
    - `sources` (sources.debian.org; `--package` is the source
      package name),
    - `mirror`,
    - `udd`,
    - `udddump`: mandatory filter flags, ADR-012, URL-scoped.
  - `consolidate` is not a subcommand: the batch feeder of
    `storage.consolidation` is the example prosumer
    `debian-observer-consolidate` (installed from `src/prosumers/`)
    — `--file-in <event log> --file-out
    data/normalized/package_versions/package_versions.parquet`.
  - `publish` projects the event log onto NATS JetStream
    - `--events-dir` is required,
    - `--stream` defaults to `debian-events`,
    - `--server` defaults to `nats://localhost:4222`
    - Rebuildable via `Nats-Msg-Id` dedup (See ADR-004 amendment).
  - `validate` takes raw artifacts (positional, one or more) and
     reads each artifact's manifest sidecar for source and run id.
  - `normalize` is a command group with one subcommand per raw surface
    (ADR-012 amendment 2026-10-23: a breaking rename of the old
    `normalize <paths>` surface):
    - `artifacts` (positional): pipeline `.csv.xz` artifacts,
    - `udddump`: dump downloads with mandatory `--table` and
      optional `--limit`,
      `--eventizer` selects the eventizer
      (`dependencies` | `package-versions` (default) | `popcon`);
      `--events-dir` writes the Parquet event log
      (one directory per event type, one file per run,
      default `data/observed`; always written),
      `--output FILE` adds a JSON-lines copy of the same events.
  - `serve` starts the FastAPI API service.
  - `status` shows system status and data summary.
  - `replay` replays events from a JSON-lines file to NATS.
  - `benchmark` runs a quick benchmark of the ingestion pipeline.
  - `execution-report` shows the historical execution report with
    percentiles.
  - `query` exposes the event log and the consolidated datasets
    to ad-hoc DuckDB SQL. It registers views the caller names in FROM.
    - `--events-dir` registers the `events` view over the event log
      (required only when a query looks at the event log),
    - `--dataset-dir` registers views over the consolidated datasets,
    - a dataset directory is ONE table: views are registered per
      directory, never per file — the file layout is the writer's
      business and is not a query interface,
    - view names derive from the directory name — one schema found
      carries the directory's name, N schemas found register
      `<directory>__<suffix>`, one view per schema, where `<suffix>`
      is a short stable digest of the schema (column names and types
      in file order), deterministic across runs, never a file name,
    - `--describe` lists schemas and their fields, and names tables
      with the SAME rule the views are registered with: what
      `--describe` lists is exactly what goes in FROM.
- **Guarantees**:
  - no `consume` subcommand (2026-10-23) — a queue
    consumer is a pipeline the researcher owns, not a command.
  - no view name is derived from a file name — the file layout is
    the writer's business and never a query interface.
  - `--describe` and `query` share one naming rule by
    construction — what `--describe` lists is exactly what goes
    in FROM.

## `idempotency`

- **Exposes**: in-memory `IdempotencyStore` (seen event IDs).
- **Guarantees**:
  - In-memory, per-process scope; no persistent backend.
  - Durability of effects does not come from this store but from
    deterministic output paths and idempotent folds (the
    consolidation fold): replaying the same events converges to
    the same state.

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

- **Exposes**: the prosumer *library* package
  (`src/debian_observer/prosumers`) — the reusable building blocks
  a prosumer is written from:
  - `stream` — the I/O abstraction: `Record`, `Source`, `Sink`
    over file (Parquet) or stream (ND-JSON),
  - `cli` — the shared `click` option dialect
    (`--file-in`/`--stream-in`/`--file-out`/`--stream-out`),
  - `packages` — the consolidation logic
    (fold `PackageVersionObserved` events),
  - `coordinates` — the reusable coordinate filter:
    `distribution`, `release`, `architecture`, `components`
    (a '+'-list; an absent selector filters nothing; a filter for
    an architecture also lets `all` through — a graph needs
    both) — one piece, reused by every consumer of the event log,
  - `lake` — the batched event-lake reader: one event-type
    directory read batch by batch, never whole in memory
    (ADR-015).
- **Expects**: input records shaped by the prosumer contract —
  `--file-in`/`--stream-in` carrying `Record` envelopes (ND-JSON)
  or flat event-log rows (Parquet).
- **Guarantees**: the libraries are the public surface — a user
  builds a prosumer from them without reaching into
  `debian_observer` internals. The shipped examples live in
  `src/prosumers/` (`consolidate.py`, `filter.py`, installed as
  `debian-observer-consolidate` / `debian-observer-filter`):
  example CLIs only, no importable library code, built solely on
  these libraries exactly as a user-built prosumer would be.




## `version`

- **Exposes**: `__version__` — the package version, as the single
  source of truth (read by hatchling via `[tool.hatch.version]`
  in `pyproject.toml`; `__init__` re-exports it).
- **Guarantees**: one version literal in the whole repository —
  the built wheel, the sdist and the runtime all see the same
  value without any manual synchronisation. The runtime readers
  are `debian-observer --version`, the API's OpenAPI version and
  the HTTP User-Agent of the acquisition downloads.
- **Expects**: nothing — a standalone module, no imports, so the
  devcontainer image can copy just this file before the editable
  install (a version bump invalidates only that layer).