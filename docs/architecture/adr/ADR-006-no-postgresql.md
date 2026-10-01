# ADR-006 — No PostgreSQL in R1

Status: Proposed
Date: 2026-09-29


## Context

PostgreSQL is a robust, proven transactional database suitable for a wide range of operational workloads.
In data platform architectures, it commonly serves as the primary operational store, materialized view cache, or metadata registry.
The decision to exclude it from R1 is therefore deliberate and requires justification.

The R1 workload has the following characteristics:

 - historical and analytical rather than transactional
 - predominantly append-oriented with minimal updates
 - file-based and reproducible
 - local and resource-constrained (8 GB RAM, home lab)
 - short implementation timeline (two weeks)

Introducing PostgreSQL would add operational complexity:

 - connection management,
 - schema migrations,
 - backup procedures,
 - replication considerations, and
 - performance tuning.

These concerns would consume engineering time without solving a demonstrated R1 problem.

Parquet files with DuckDB queries already satisfy the analytical requirements.
NATS JetStream provides durable event storage. Raw artifacts are preserved as immutable files.
No component in the R1 architecture requires

 - mutable transactional state,
 - high-concurrency point queries, or 
 - relational operational state.


## Decision

**PostgreSQL will not be used in R1.**

The architecture uses:

 - immutable raw files for source data preservation
 - NATS JetStream for durable event storage
 - Parquet files for analytical datasets
 - DuckDB for analytical query execution

These components collectively satisfy R1's storage and query requirements without introducing a transactional database.

If a later workload explicitly requires:

 - mutable transactional state
 - high-concurrency point queries
 - frequent row-level updates
 - relational operational data

then introducing PostgreSQL (or another operational database)
will become a future architectural decision with its own ADR.

## Consequences

Pros:

 - Reduced operational complexity and deployment footprint
 - No schema migration management required in R1
 - Analytical queries are file-based and portable
 - Faster iteration within the constrained implementation timeline

Cons: 
 - Limited ability to perform complex joins across normalized entities in R1 (mitigated by DuckDB's SQL capabilities)

Note: Future introduction of PostgreSQL is architecturally feasible when workload requirements justify it
