# ADR-002 — Raw Data Preservation

Status: Proposed
Date: 2026-09-29


## Context

Historical analysis requires **auditability**;
the ability to answer "what was observed, when, and from where" without ambiguity.

Data **reproducibility** is a core goal of the Debian Observatory.

This demands not just the final normalized output, but also the original artifact,
the transformation that produced it, and the configuration under which that transformation ran.

A normalized dataset can only be considered reliable if it can be recreated deterministically from its source.

Data requirements will evolve. By reserving raw artifacts and provenance metadata,
later releases **won't need to repeatedly rebuild the entire** ingestion pipeline,
avoiding the related drift and uncertainty.

The project operates under a constraint of limited resources
(8 GB RAM development machine, 8 GB RAM primary server, additional 4 GB machines).
Raw data **preservation must therefore be deliberate** rather than exhaustive,
depending on available storage, besides legal feasibility.


## Decision

Raw data artifacts will be **preserved whenever licensing and storage constraints allow**.

Raw artifacts are immutable. A normalized dataset can be recreated from:

> raw artifact + transformation version + configuration

Each ingestion run produces a dataset manifest containing at minimum:

 - dataset name and source
 - retrieval timestamp
 - source version
 - checksum
 - size in bytes
 - record count
 - license information

The ingestion component **does not directly mutate ** analytical state.

It is responsible for

 1. retrieving data,
 2. validating acquisition,
 3. recording provenance,
 4. storing the raw artifact,
 5. creating the manifest,
 6. and triggering downstream processing.

Each manifest is persisted as a JSON file next to the raw
artifact (`raw/<source>-<timestamp>-<run>.manifest.json`), so the
provenance chain survives the run. The manifest format is JSON;
the YAML block in the project spec is illustrative of the fields
only.

Where the data source permits, R1 will support incremental processing,
distinguishing new, changed, unchanged, and invalid data to avoid unnecessary reprocessing.


## Consequences

Pros:

 - Immutable raw artifacts prevent accidental corruption of source data
 - Provenance chain enables debugging, auditing, and scientific verification
 - Full reproducibility: any normalized output can be reconstructed from preserved inputs

Con: Storage requirements will grow with each dataset snapshot

Limit:

 - Licensing constraints may limit which sets can be retained
 - Available storage room may limit how much can be retained
