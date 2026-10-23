# ADR-003 — Parquet and DuckDB

Status: Proposed
Date: 2026-09-29


## Context

The primary data workload for R1 is historical, analytical, **predominantly append-oriented, and file-based**.
The system must support efficient analytical scans over potentially large Debian datasets
while running on resource-constrained hardware (8 GB RAM primary server, additional 4 GB machines).

Traditional transactional databases like PostgreSQL are optimized for
mutable state, high-concurrency point queries, and row-level updates.
The Debian Observatory's R1 workload does not require these properties.

Instead, the targeted workload benefits from standard approaches such as:
**columnar scans, compression, portability, and low operational overhead**.

The architecture favors **immutable files, columnar storage, and lightweight analytical queries**.

The storage layer must support reproducibility, meaning
analytical datasets should be **portable and reconstructible**
without complex database restoration procedures.


## Decision

**Parquet** will be the primary analytical storage format,
and **DuckDB** will provide the analytical query engine.

Parquet was selected because it provides:

 - efficient columnar representation for analytical scans
 - strong compression reducing storage footprint
 - portability across tools and platforms
 - broad ecosystem compatibility
 - low operational overhead (files, not services)
 - suitability for immutable historical data

DuckDB was selected because it:

 - runs locally without a separate server process
 - queries Parquet files directly without ingestion
 - requires minimal operational infrastructure
 - provides a full SQL interface
 - performs analytical workloads efficiently
 - fits the homelab constraints of the target deployment

Example use:
```sql
SELECT package, version, release
FROM read_parquet('data/normalized/package_versions/*.parquet')
WHERE release = '...'
```

## Consequences

 - Analytical queries execute **efficiently** against compressed, columnar data
 - **No separate database service to deploy, monitor, or maintain**
 - Datasets are **portable** files, supporting reproducible research workflows

Cons:

 - DuckDB's single-node design **limits distributed query capabilities** in R1
 - **Not suitable for high-concurrency transactional** workloads (not required for R1)

Note: Future releases can complement or replace this stack if workload requirements change

## Amendment — 2026-10-23: the `package_versions` table is a consumer output

History (unchanged above): R1 projected the normalized dataset
per-ingestion-run from the pipeline (`--dataset-dir`, one Parquet file
per run, one row per event) and derived *current state* as a
query-time fold.

Amendment: the per-run projection step is **gone**. Normalization
only eventizes raw observations into the append-only event log
(`--parquet-dir`, one Parquet file per ingestion run). The
tabular state is produced by a **consumer** — `consolidate
--events-dir` — that folds the whole log: one row per full
observation PK `(package_id, version, distribution, release, suite,
architecture)`, last-observed-wins attributes, min/max
`observed_at` per group as `observed_first_at`/`observed_last_at`.
The fold is idempotent by construction (spec §15.1: duplicated log
files collapse; folding twice yields the same table), so the output
is ONE re-generable file (`package_versions.parquet`, written
atomically) — not an accumulating per-run dataset. The query example
above is unchanged: `read_parquet` over the same glob still works,
now over the consolidated table.
