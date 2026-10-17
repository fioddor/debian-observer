# ADR-011 — Writer-Owned Serialization

Status: Proposed
Date: 2026-10-17


## Context

Eleven ``to_parquet_row()`` serialization helpers spread across the
domain and events layers, with zero production callers: the Parquet
event file is actually written by ``ParquetWriter._event_to_row()``,
which owns its own 17-column schema and reads envelope/payload dicts
directly.

Four serialization dialects coexist (domain rows, payload rows,
envelope rows, writer rows). The drift is not hypothetical:
``PackageVersion.to_parquet_row()`` has emitted 8 of 12 fields since
its introduction (671ee5f6, 2026-10-01) — ``source_package``,
``release``, ``suite`` and ``section`` were never added — while the
writer carried them.

The spec (release1.project.v0.4, §7.2) separates domain events from
analytical state as independent layers, and both the spec (§10.2) and
ADR-003 reference a normalized dataset
``data/normalized/package_versions/*.parquet`` that R1 never built:
the event log currently doubles as the analytical surface.


## Decision

The storage layer owns all Parquet serialization.

1. Domain and event models are serialization-pure: they serialize to
   JSON (``model_dump_json()``) and nothing else. All
   ``to_parquet_row()`` helpers in ``domain/`` and ``events/`` are
   removed; new ones are not added.
2. The storage layer provides two writers, each owning its file schema:
   - ``ParquetWriter`` — the event log: one flat row per
     ``EventEnvelope`` (replay surface).
   - ``PackageVersionDatasetWriter`` — the normalized dataset:
     projects ``PackageVersionObserved`` events into
     ``data/normalized/package_versions/*.parquet``, one row per
     observed version (query surface), closing the spec §10.2 /
     ADR-003 dataset gap.
3. The dataset schema is pinned to ``PackageVersion.model_fields`` by
   a set-equality test, so a new domain field that is not projected
   turns the battery red.


## Consequences

 - Exactly one serialization dialect per artifact (event file,
   dataset file), both in ``storage/``.
 - Schema drift surface shrinks from four dialects to two
   writer-owned schemas.
 - Adding a domain field requires extending the dataset writer in the
   same change (enforced by tests).
 - Consumers wanting Parquet rows must go through the writers; the
   helpers will not come back.
 - The dataset is append-oriented: one immutable file per ingestion
   run; latest-state aggregation is R2 work.

Pros:

 - Kills a silent-drift trap class at the root: the writer is the
   only schema authority.
 - Makes the spec's four-layer separation real in code: events (log)
   vs analytical state (dataset), each an independent artifact.

Cons:

 - Any consumer of the removed helpers must migrate to the writers
   (none exist today).
 - Observed fields are stored twice (event log + dataset) — accepted
   for R1: the dataset is the query surface, the log is the replay
   surface.

Note: This ADR resolves the tension flagged on 2026-10-17 between
writer-owns-serialization and the spec's normalized dataset: the
events→state projection stays a first-class seam, owned by the
storage layer, and the dataset exists as its own artifact.
