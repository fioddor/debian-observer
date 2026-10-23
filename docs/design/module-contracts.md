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
  run; writer-owned schema, ADR-011) + analytical queries (DuckDB) +
  the consolidation fold (`consolidation`, moved here from
  `consumer` on 2026-10-23 — the fold IS a writer of the
  consolidated table). Two feeders, one semantics: the batch
  feeder (`fold_package_versions` /
  `build_consolidated_packages`, behind the `consolidate` CLI:
  read the whole log, feed every event, write once — pure
  regeneration, `merge_existing=False`) and the streaming feeder
  (`ConsolidationMerge.feed` + `.write(dataset_dir)`: one
  observation at a time, RMW of the existing table + atomic rename,
  behind the external example).
- **Expects**: normalized rows from the transformer; dataset layout
  as written by the writer; log rows read via `read_event_log`
  (sorted glob + DuckDB `union_by_name`) or envelope-derived rows
  via `row_from_envelope`.
- **Guarantees**: writer-owned serialization (ADR-011) — the event-log
  shape is the writer's decision, loaders must not reshape it. The
  API loader still reads Parquet event files written by earlier R1
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
- **Guarantees**: every payload carries `ObservationMeta` provenance;
  payloads serialize to JSON and flatten to Parquet-compatible rows.

## `messaging`

- **Exposes**: NATS JetStream publisher/consumer/client + stream
  topology (`StreamConfig`; stream `debian-events`, flat subjects
  `debian.package.observed`, `debian.package_version.observed`,
  `debian.release.observed`, `debian.migration.observed` — per
  event type, no per-package partitioning) + `ensure_stream` (the
  stream created if missing). `EventPublisher.publish` passes
  `headers` through — the publish CLI sends
  `Nats-Msg-Id: <event_id>` for server-side dedup (ADR-004
  amendment, 2026-10-23); the most recent `PubAck` is kept on
  `last_ack` (carries `duplicate` when the server reports it).
- **Guarantees**: delivery semantics per ADR-005; JetStream
  persistence per ADR-004. **Library only — no CLI**: the generic
  `consume` subcommand is dead (2026-10-23); consuming is a
  pipeline the researcher owns (`examples/consume_packages.py`).

## `consumer_app`

- **Exposes**: standalone async consumer entrypoint — handler
  registration (the handler receives only the `EventEnvelope`),
  idempotent processing (`IdempotencyStore`), graceful lifecycle
  shutdown; ack strictly after the handler returns without
  exception (ADR-005 amendment, 2026-10-23).
- **Expects**: registered handlers by event type; JetStream
  endpoints; a handler that makes its effect durable before
  returning (the example's `merge.feed` + `merge.write`).

## `consumer`

- **Dead** (2026-10-23): the consolidation fold moved to
  `storage.consolidation` (the fold is a writer of the consolidated
  table — ADR-011) and the queue consumer lives outside the CLI
  (`examples/consume_packages.py`, over `consumer_app` +
  `messaging`). Nothing is exposed here anymore.

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
  `data/normalized/package_versions` — the batch feeder of
  `storage.consolidation`); `publish` projects the event log onto
  NATS JetStream (`--events-dir` required, `--stream` default
  `debian-events`, `--server` default `nats://localhost:4222`) — a
  rebuildable view, `Nats-Msg-Id` dedup (ADR-004 amendment);
  `validate` takes raw artifacts (positional, one or more) and reads
  each artifact's manifest sidecar for source and run id;
  `normalize` is a command group with one subcommand per raw surface
  (`artifacts`: pipeline `.csv.xz` artifacts, positional; `udddump`:
  dump downloads with mandatory `--table` and optional `--limit`,
  ADR-012 amendment 2026-10-23 — a breaking rename of the old
  `normalize <paths>` surface).
- **Guarantees**: no `consume` subcommand (2026-10-23) — a queue
  consumer is a pipeline the researcher owns, not a command.

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
