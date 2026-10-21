# Module Contracts

What each top-level module exposes, expects, and guarantees. Signatures
live in the code; this page records the *promise* — the part a reader
cannot reconstruct without reading everything.

## `ingestion`

- **Exposes**: data-source adapters (UDD PostgreSQL
  <https://udd.debian.org/>, Debian mirror, sources.debian.org, mock)
  and the `ingest` pipeline: raw artifacts as xz-compressed CSV + JSON
  manifest persisted beside each artifact + provenance record.
- **Expects**: source selection (`--source`), optional row `--limit`
  window; UDD requires reachable PostgreSQL credentials.
- **Guarantees**: raw data is preserved verbatim (ADR-002) before any
  transformation — as RFC 4180 CSV compressed with LZMA
  (<https://www.rfc-editor.org/rfc/rfc4180>, suffix `.csv.xz`);
  `None` is written as an empty field and read back lossless (sources
  emit no empty strings); manifests are persisted beside every
  artifact; identity fields (incl. `component`, `release`, `section`,
  `priority`) survive acquisition.

## `normalization`

- **Exposes**: transformer from raw source rows to canonical
  `PackageVersion` records and typed event payloads; validators.
- **Expects**: raw rows in any of the known source layouts; the
  family/suite/release semantics contract (see data model).
- **Guarantees**: `distribution` is always the archive family
  (`debian`/`ubuntu`) — never a suite or codename; suite *roles*
  (stable, testing, unstable…) go to `suite`; codenames and pockets
  (bullseye, sid-security…) go to `release`; a source that has no role
  information emits `suite=None`.

## `storage`

- **Exposes**: Parquet dataset writing (`--dataset-dir`) and analytical
  queries (DuckDB).
- **Expects**: normalized rows from the transformer; dataset layout as
  written by the writer.
- **Guarantees**: writer-owned serialization (ADR-011) — the dataset
  shape is the writer's decision, loaders must not reshape it. The
  API loader still reads Parquet event files written by earlier R1
  iterations (13- and 16-column layouts, before `release`, `component`,
  `section`, `priority`, `suite` existed); fields the old files lack
  load as `None`.

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

- **Exposes**: `click` commands (`ingest`, `serve`, …); the operator's
  entry point into ingestion and the API service.

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
