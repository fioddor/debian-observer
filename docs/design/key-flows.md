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

## Flow 2 — Observation event end-to-end

1. Transformer (normalization) maps raw rows → canonical
   `PackageVersion` + `ObservationMeta`, applying family/suite/release
   semantics.
2. Payload is wrapped in `EventEnvelope` and published to JetStream
   (`debian.observer.>` subjects).
3. `ConsumerApp` subscribes (durable consumer); **each delivery
   reaches the handler exactly once per process** thanks to the
   `IdempotencyStore`.
4. Handler processes the payload; `storage` writes the Parquet
   dataset (writer-owned shape, ADR-011).
5. Analyst queries the dataset through the FastAPI `api` (DuckDB,
   read-only) — closing the loop: ingest → normalize → store/publish →
   query.


## Flow 3 — UDD dump file: download → eventize → project

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
3. A consumer step projects the eventized records into the normalized
   `package_versions` dataset — **planned**: today the projection
   runs inside the pipeline (`ingest --dataset-dir`); a udddump
   consumer path (events → dataset) is the next MVC.
4. Re-eventizing the same raw artifact with other filters
   (`--table`/`--limit`) repeats step 2 only — the raw download is
   never consumed and never re-downloaded.
