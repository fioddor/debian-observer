# Key Flows

End-to-end behaviour *across* modules — the level neither architecture
nor code shows. Two flows in R1; add here only what a reader cannot
reconstruct from the module contracts.

## Flow 1 — Ingestion run (UDD → raw artifact)

1. Operator runs `ingest udd --table T [--limit N]` (CLI → ingestion;
   one subcommand per source, ADR-012). The UDD `pg_dump` file flow is
   separate: `ingest udddump --url` only downloads (see Flow 3).
2. `UDDDataSource` queries with the identity SELECT (package, source,
   version, distribution, release, architecture, section, priority,
   component).
3. **Window caveat:** `--limit` takes a nondeterministic window — fine
   for smoke runs. Reproducibility does not come from window
   selection: the raw artifact + its manifest (checksums, run id)
   make any run auditable, and re-ingesting a source re-derives the
   same facts.
4. Raw rows land verbatim in a `.csv.xz` artifact (RFC 4180 CSV,
   LZMA; `None` written as an empty field, lossless since sources emit
   no empty strings — ADR-002); a JSON manifest is persisted beside it
   (path, row count, checksums); provenance records source and run id.
5. Same rows feed normalization → storage → messaging (Flow 2, async).

## Flow 2 — Observation event end-to-end (ingest → consolidate → query)

The lifecycle of an observation in three stages; the event log is
the backbone — every later stage derives from it.

1. **Ingest:** the transformer (normalization) maps raw rows →
   canonical `PackageVersion` + `ObservationMeta`, applying
   family/suite/release semantics; the payload is wrapped in
   `EventEnvelope` and `ingest` appends it to the Parquet event log
   (`--events-dir`, one file per ingestion run). `ingest` writes
   nothing consolidated — the per-run dataset step is gone
   (ADR-003 amendment 2026-10-23); the log may also be published to
   JetStream (`debian.observer.>` subjects) for the async path.
2. **Consolidate:** the batch feeder (`consolidate --events-dir`)
   folds the whole log — `PackageVersionObserved` events only,
   dedup by `event_id`, ordered by `(observed_at, event_id)` — into
   the consolidated `package_versions` table (`storage.consolidation`,
   writer-owned ADR-011): one row per full observation PK,
   last-observed-wins attributes, min/max `observed_at` per group.
   Idempotent by construction, written as ONE atomically-replaced
   `package_versions.parquet`.
3. **Query:** the analyst queries the consolidated table through
   the FastAPI `api` (DuckDB, read-only) or ad-hoc SQL — closing the
   loop: ingest → consolidate → query, with the event log as the
   replay surface and the table as derived, re-generable state.



## Flow 3 — UDD dump file: download → eventize → consolidate

1. `ingest udddump --url <https://…>` downloads the dump and stores it
   **byte-preserving** as the raw artifact
   `data/raw/udddump-<timestamp>-<run-id>.dump` with a manifest
   sidecar (source `udddump`, checksum, size; `record_count` null)
   and a provenance record — acquisition only, no parsing
   (ADR-012 amendment, 2026-10-23).
2. `normalize udddump <artifact> --table T [--limit N]` re-reads the
   manifest sidecar (source name, run id), gates `--table` against
   the dump's table of contents and the table's COPY header against
   the registered shapes, streams the COPY block through host
   `pg_restore --data-only --table` (no database restored, ADR-006)
   and writes `PackageVersionObserved` events as JSON lines.
3. A consumer step projects the eventized records into the
   consolidated `package_versions` table (Flow 2, stage 2) — the
   per-run pipeline projection (`ingest --dataset-dir`) is dead
   (ADR-003 amendment, 2026-10-23).
4. Re-eventizing the same raw artifact with other filters
   (`--table`/`--limit`) repeats step 2 only — the raw download is
   never consumed and never re-downloaded.

## Flow 4 — Streaming consolidate (publish → external consumer)

The async path over NATS JetStream (ADR-004 amendment, 2026-10-23);
the event log remains the backbone — the stream is a view.

1. **Publish:** `publish --events-dir` reads every Parquet log file
   and projects each event onto JetStream (stream `debian-events`,
   flat per-type subjects, `Nats-Msg-Id: <event_id>` — server-side
   dedup: re-publishing the same log is a no-op). The stream is
   created if missing with the ADR-004 topology.
2. **Consume (external):** the researcher's consumer — a copy of
   `examples/consume_packages.py`, its own process; the CLI has no
   `consume` subcommand — subscribes durably (state server-side in
   JetStream); each `PackageVersionObserved` message feeds the
   consolidation merge and the durable table is rewritten BEFORE
   the ack (ADR-005 amendment).
3. **Crash/resume:** `kill -9` leaves unacked messages — JetStream
   redelivers, the merge absorbs the duplicates (idempotent fold),
   and the table converges to exactly what `consolidate
   --events-dir` regenerates from the same log:
   `fold(log parquet) == fold(stream)` (pinned by
   `scripts/crash-test-consumer.sh`).
