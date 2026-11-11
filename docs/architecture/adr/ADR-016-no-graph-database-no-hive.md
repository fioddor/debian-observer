# ADR-016 — No graph database, no Hive partitioning

Spec: `docs/release2.project.v0.2.md` §3.2.5, §10.1
Status: Accepted
Date: 2026-11-08


## Context

R2 derives dependency graphs per coordinate
(`(distribution, release, architecture, components)`, spec §4.3):
one coordinate is ~70 k nodes / ~315 k edges (*measured
2026-11-01*, spec §2.1) — small. The workload on them is batch:
BFS levels, Tarjan SCC, degree and concentration aggregates,
joins against `package_versions` and `popcon`. Queries are
analytics (DuckDB) and traversals (in-process algorithms), not
interactive multi-hop graph queries.

The derived tables are Parquet files under `data/derived/`.
Two layout temptations exist: a graph database (Neo4j & co.), and
Hive-style partition directories
(`distribution=…/release=…/architecture=…/`).


## Problem

How to store and compute over the derived graphs — on 8 GB
homelab hardware, with provenance-critical coordinates that must
survive file-by-file transport to workers (spec §11.2 ships
derived files over a shared directory) — without adopting
infrastructure whose cost is paid up front and whose benefit is
speculative?


## Alternatives

 1. **DuckDB SQL for joins/aggregates + in-memory algorithms
    (BFS, iterative Tarjan) in the prosumers; coordinates in
    columns, flat file layout.**
 2. Graph database: a second storage engine, a server to operate
    (against ADR-006/007's no-extra-services direction), import/
    export machinery for a dataset that is **regenerable from the
    event log by definition** (R1 principle) — while the workload
    (batch SCC/BFS/aggregates) is exactly what DuckDB + in-process
    algorithms do without a server.
 3. **Hive-partitioned Parquet**: path segments carry the
    coordinate; readers pass `hive_partitioning=true`.


## Decision

 1. **No graph database in R2.** Joins and aggregates: DuckDB
    over Parquet. Traversals: in-memory algorithms in the
    prosumer library (`prosumers/graph.py`: BFS levels, iterative
    Tarjan — no new Python dependency, spec §17). One graph is
    ~15 MB (CSR `array.array`, spec §2.2): memory is not the
    constraint at any worker count R2 can reach.
 2. **No Hive partitioning, and no disguised equivalent either.**
    Derived tables are one directory per table; the files inside
    are the writer's implementation detail (spec §10.1). **Every
    derived row carries its full coordinate in columns** —
    `distribution`, `release`, `architecture`, `components`. A
    file is self-describing; a query filters on columns, never on
    paths; nothing in R2 parses a path to recover a value.
    Measured against Hive (spec §10.1):
    - researchers work cross-coordinate by default (the headline
      is the *dynamics*: evolution across releases) — for a query
      that reads every coordinate, partition pruning buys nothing;
    - every reader must pass `hive_partitioning=true` and know
      which columns are path segments — the storage layout leaks
      into every program;
    - **a Hive partition file read on its own has lost
      `distribution`, `release`, `architecture`** (measured,
      spec §10.1) — a real provenance hazard when Work distribution
      (spec §11.2) ships files to workers over a shared
      directory: a file that has lost its coordinate is unusable
      on arrival.
 3. File names are a convenience for humans and for atomic
    replacement (`os.replace`, R1 pattern) — **not a query
    interface**. The deep-dependency cache writes flat files named
    `<distribution>-<release>-<architecture>-<components>-<package>
    -<version>.parquet`, epochs percent-encoded (`%3A`, `%25`).


## Consequences

 - Zero new infrastructure; the graph analytics run in-process
   next to the data.
 - The coordinate is queryable (`WHERE release='trixie'`) instead
   of structural — cross-coordinate queries are plain SQL.
 - Derived files remain self-describing under any transport.
 - Parallel per-coordinate computation is a process-per-
   coordinate choice, not a layout choice (E2, spec §16.3).


## Evidence

 - Spec §2.1 (graph sizes), §2.2 (CSR 15 MB), §10.1 (measured
   Hive partition file losing its coordinate columns), §3.2.5.
 - E2 (spec §16.3) measures the per-coordinate and cross-
   coordinate costs from the implementation.


## Revisit conditions

 - An interactive multi-hop graph query use case over all
   coordinates at once (not a batch fold) — e.g. an exploration
   UI over the full temporal+component matrix — would reopen the
   graph-database question (spec §23).
 - A lake too large for a plain column-filtered read
   (ADR-015 revisit) reopens the *partitioned file* question on
   its own, independently of the graph question.
