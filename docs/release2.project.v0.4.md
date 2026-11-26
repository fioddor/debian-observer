# Dependency & Release Dynamics. Sub-Project for Release R2.

> Turning the R1 observatory's package observations into a queryable,
> evolving dependency network of Debian — and measuring what it takes
> to compute it in parallel on a homelab.

Status: draft for review. Decisions still open for the owner are
listed in Open Decisions (§24).

Base line: R1 as delivered on `origin/tdd` (see
`docs/release1.project.v0.4.md`, `docs/design/*`,
`docs/architecture/adr/ADR-001..012`). Where this document says
"R1 already provides", the claim refers to that base line.

Measured claims in this document carry their evidence. Numbers
labelled *(measured 2026-11-01)* come from the local UDD restore;
numbers labelled *(measured 2026-11-03)* come from the probes under
`benchmarks/results/r2-design-probes/` and must be re-run and
committed with the implementation (Benchmarked Performance, §16).


## Table of Contents

- **Overview**
  - [1. Main **Goals** — domain purpose, release scope, technical features.](#1-main-goals)
  - [2. Technical **Challenges** — volume, memory, parallelism, semantics.](#2-technical-challenges)
  - [3. Engineering **Analysis** — related work & positioning, scope, ADRs.](#3-engineering-analysis)

- **Data Models**
  - [4. Data Sources & Datasets (with measured sizes)](#4-data-sources--datasets)
  - [5. Data Model: dependency domain](#5-data-model)
  - [6. Event Model: `DependencyObserved`, `PopconObserved`](#6-event-model-debian-observers)

- **Automated Process**
  - [7. Architecture](#7-architecture)
  - [8. Ingestion & the eventizer concept](#8-ingestion--the-eventizer-concept-debian-observer)
  - [9. Normalization: eventizers](#9-normalization-eventizers-debian-observer)
  - [10. Analytical Storage: derived dependency tables (the prosumers)](#10-analytical-storage-derived-dependency-tables)
  - [11. Messaging: events and work distribution](#11-messaging)
  - [12. CLI & API](#12-cli--api)
  - [13. Observability](#13-observability)

- **Product Quality Specifications**
  - [14. Deployment](#14-deployment)
  - [15. Reliability Properties](#15-reliability-properties)
  - [16. Benchmarked Performance (extension of R1 §16)](#16-benchmarked-performance-extension-of-r1-16)
  - [17. Security & Supply Chain](#17-security--supply-chain)

- **Product Quality Checks**
  - [18. Definition of Done](#18-definition-of-done)
  - [19. Testing Strategy](#19-testing-strategy)
  - [20. Demonstration Scenario](#20-demonstration-scenario)

- **Development Plan**
  - [21. Ten-Day Execution Plan](#21-ten-day-execution-plan)
  - [22. Stretch Goals (only if time remains)](#22-stretch-goals-only-if-time-remains)
  - [23. Future Work](#23-future-work)
  - [24. Open Decisions for the Owner](#24-open-decisions-for-the-owner)


---
## 1. Main Goals

### 1.1. (Business) Domain and Product Purpose

R1 answered *"can we build a reliable historical representation of
Debian packages?"*. R2 uses and develops that representation to
study the relationships **between** packages.

The domain question of R2 is:

> How is the deployment-dependency structure of Debian shaped, how
> does it change from one release to the next, and which packages
> concentrate the risk — i.e. how far does a change or a defect in
> one package reach?

Two audiences, one system:

 - **As a researcher** I want to study the package dependencies
   within Debian:
   - direct and transitive dependencies,
   - reverse dependencies,
   - cycles,
   - concentration, and
   - their evolution across releases.
 - **As an data infrastructure engineer** I want to assess capabilities
   for dimensioning deployments:
   - how much time, memory and storage the analyses cost?
   - how they scale with workers? and
   - what is worth caching?

Finding circular dependencies is the **first validation use case**
(it exercises the whole up/down graph machinery).

But it isn't the purpose: Debian already has precise tools
for single-snapshot checks (see Related work & positioning, §3.1).
The value is in the *dynamics* — evolution and concentration
— which those tools do not provide.



### 1.2. Release Scope

R2 introduces:

 1. a dependency **eventizer** for the `udddump` source in debian-observer,
 2. a `popcon` eventizer (popularity, used to weight impact) in debian-observer,
 3. a family of external dependency **prosumers** that build analytical tables
    (direct, reverse, cycles, statistics, deep dependencies). This will be a
    project of its own "Debian Observer Plug-In: Packages Deploy-Dependency
    Analysis (DOPI-PDDA)",
 4. use of debian-observer's **benchmark** (R1 §16) to evaluate parallelization
    and to dimension the cache limit for deep-dependency tables,
 5. a **parallel worker in Go** in DOPI-PDDA, as the experimental arm of that
    debian-observer's benchmark, processing in DOPI-PDDA work distributed over
    debian-observer's NATS JetStream.

R2 is useful by itself (dependency analytics over real data) while
preparing R3 (operating many workers reliably: SLOs, Kubernetes,
GitOps).


### 1.3. Technical Product Features of Release 2

Domain:
 - **real** dependency data from UDD (no synthetic data),
 - explicit **dependency observaton event** contract,
 - cross-release comparison (temporal dimension),
 - **Coordinates** (§4.3): package sets are defined by
   - distribution: ['Debian', 'Ubuntu, ...]
   - release: [..., 'bullseye', 'bookworm', 'trixie', 'forky', ...]
   - component: ['main', 'main/debian installer', 'contrib', 'non-free-firmware', 'non-free']
   - architecture: [...]
   Technical graphs usually set distribution and release
   (harder frontiers), and lists of components and architectures
   (softer boundaries).
   Administrative and other miscellaneous analysis may cut
   their scope after other patterns.

Technical:
 - pipeable analytical pieces que reads and produce streams of data,
 - idempotent, regenerable derived tables,
 - graph algorithms (reachability, strongly connected components),
 - parallel processing: Python baseline vs Go, measured,
   Python parallelism is limited to one thread at any instant.
   Others wait for the Global Interpreter Lock (GIL),
   the mutex in CPython, to turn off.
 - work distribution over JetStream (work-queue semantics),
 - evidence-based engineering decisions: parallelism and cache size
   chosen from measured product behaviour, kept separate from the
   domain analytics above,
 - observability of workers (OpenTelemetry, as R1).


---
## 2. Technical Challenges

### 2.1. Volume (measured)

Measured on 2026-11-01 against the local UDD restore
(`udd-postgresql` container on ghostell, restored from
`udd.dump`, 2.86 GB, UDD export of 2026-10-02):

| Measure                                                   |         Value |
|:----------------------------------------------------------|--------------:|
| `packages` rows, all distributions                        |     1,497,724 |
| `packages` rows, `debian` bullseye                        |       149,016 |
| `packages` rows, `debian` bookworm                        |       313,253 |
| `packages` rows, `debian` trixie                          |       303,030 |
| `packages` rows, `debian` forky                           |       310,137 |
| `packages` rows, `debian` sid                             |       333,759 |
| Distinct `(release, architecture)` pairs in `debian` (incl. `all`) |  133 |
| … excluding `all`                                                  |  117 |
| … base releases only (bullseye 4, bookworm 9, trixie 8, forky 8, sid 8 architectures; Coordinates, §4.3) | 37 |
| trixie, binaries in `amd64` + `all` (one graph)           |        70,341 |
| **all distributions, `Depends` clauses** (event volume)   | **8,376,616** |
| **all distributions, `Pre-Depends` clauses**              |    **28,607** |
| trixie amd64+all, `Depends` clauses                       |       313,761 |
| … of which complex (`a \| b`)                             | 6,929 (2.2 %) |
| trixie amd64+all, `Pre-Depends` clauses                   |         1,338 |
| mean `packages` row width (chars, columns used)           |           219 |
| mean `depends` field width (chars)                        |         144.7 |
| trixie amd64+all rows per component:main                  |        69,251 |
| trixie amd64+all rows per component:contrib               |           304 |
| trixie amd64+all rows per component:nff                   |            44 |
| trixie amd64+all rows per component:non-free              |           742 |
| `main` clauses, trixie amd64+all                          |       309,082 |
| … satisfiable **inside** `main`                           |       252,252 |
| … needing a package **outside** `main`                    |         **0** |
| … unresolved (virtual/`Provides`, Debian dependency semantics, §2.3)                   |    56,830 |
| `main` clauses rescued by a **non-first** alternative     |        **72** |
| `main`→`non-main` edges when taking the first alternative |**2** (both false, Coordinates, §4.3) |
| `(component, architecture)` pairs, trixie                 |            31 |
| trixie amd64+all, packages with `Provides`                |        13,378 |
| Longest single `Depends` field (chars)                    |         9,501 |
| `testing_autoremovals` rows                               |         3,900 |
| `popcon` rows                                             |       224,197 |

Consequences:

 - **One coordinate is small** (~70 k nodes, ~315 k edges): it fits
   in memory and a single-threaded SCC computation is fast. Parallelism
   does *not* pay off for one direct graph.
 - **The volume that justifies parallelism** is the product of
   graph coordinates (37 release×architecture, ×2 components
   , Coordinates, §4.3) × per-package deep closures
   (~70 k per coordinate, i.e. ~2.6 M closures per component list).
   The benchmark must make this volume an explicit parameter
   (Benchmarked Performance, §16).

### 2.2. Memory

**The row side is not the problem; the event side is.** Measured
cost of R1's non-streaming reader
(`normalization/udddump.py::read_dump_records`, which captures the
whole `pg_restore` stdout *and* returns a list of record dicts, both
alive at once in `_parse_copy_block`), extrapolated to the full
`packages` table (*measured 2026-11-03*):

| What | Full `packages` table |
|:-----------------------------------------|---:|
| captured `pg_restore` stdout (219 chars/row mean, *measured 2026-11-01*) | ~0.33 GB |
| list of 1,497,724 record dicts (675 B/record) | ~0.94 GB |
| **R1 peak, both alive** | **~1.3 GB** |

So reading the full table with R1's reader **fits** an 8 GB machine.
Reading it is also neither slow nor frequent (one artifact = one
observation date), so it needs no parallelism.

**Eventization does not need streaming either.** Reading UDD and
writing events is *naturally* bounded: one COPY row goes in, a handful
of events come out, and they go straight to the Parquet writer. Only
the current row group is ever resident, so nothing accumulates. The
dump holds **8,376,616** `Depends` + **28,607** `Pre-Depends` clauses
across all distributions (*measured 2026-11-01*), i.e. 8.4 M events —
and peak Resident Set Size (RSS) does not care (fresh process per run,
`VmHWM`, *measured 2026-11-04*):

| Events eventized | Peak RSS (whole process) | Output |
|--------------:|-------:|--------:|
|       200,000 | 259 MB |  4.5 MB |
|     1,000,000 | 267 MB | 22.5 MB |
|     4,000,000 | 270 MB | 90.0 MB |
| **8,405,223** (the full run) | **267 MB** | 189 MB |

Flat, 42× more events for the same memory.
Writing events is no problem.

#### The wall is reading the event lake

The prosumers are where the volume lands, and R1's reader loads the
whole lake before yielding anything
(`prosumers/stream.py::FileSource.__iter__`: `pq.read_table()` →
`.to_pandas()` → `iterrows()`), after which
`src/prosumers/consolidate.py:42` does `records = list(source)` —
a third full copy, as Python objects. Measured on a 2 M-event lake and
extrapolated to the real 8.4 M (*measured 2026-11-04*):

| Read path             | 2 M events | Extrapolated to 8.4 M |
|:----------------------------------|---------:|------------:|
| R1 `FileSource` + `list(source)`  | 4,707 MB | **19.3 GB** |
| streaming (`iter_batches` + fold) |   178 MB | **0.73 GB** |

**19.3 GB on an 8 GB machine: that is the wall, and it is in the
prosumers.** It is also a 26× difference, so this is the one place
where streaming is not a preference but the only way the release
works at all.

R2 therefore streams **the read side of the lake**: prosumers consume
the event log through `ParquetFile.iter_batches()` (or DuckDB, which
streams natively), folding as they go, only materializing when later
filters make it possible.

#### Worker memory: measured, and it is not the constraint

Parallel Python workers **share** one graph rather than each holding
a copy: that is the point of building it from a durable event log.
Memory per worker is consequently not the binding constraint, and it
is not the argument for Go.

Measured on a trixie-sized graph (70,341 nodes / 315,099 edges,
`fork()` + 4 children, read-only BFS, *measured 2026-11-03*):

| Graph representation                    | Parent RSS | Per-child `Private_Dirty` |
|:-------------------------------------------|--------:|-----------:|
| dict-of-lists (naive Python)               | 24.8 MB |   9.8 MB   |
| Compreessed Sparse Row (CSR) `array.array` | +1.5 MB | **0.9 MB** |

Consequential design constraints:

 - **One graph is ~15 MB, not a memory problem at any worker count
   R2 can reach.** 4 GB / 9.8 MB ≈ 400 workers.
 - **Workers share the graph; they do not copy it.**
   `fork()` copy-on-write already shares it, so the event-driven premise holds.
   The representation decides how much stays shared: with CSR
   (`array.array` edges, no per-edge `PyObject`) a child dirties
   0.9 MB, because refcount churn touches small object headers and not
   the edge payload. Therefore, **P6 (The prosumers, §10.2)
   builds the graph once in the parent and forks workers.
 - **The GIL still forces processes, not threads**: CPython executes
   bytecode under one global lock, so a thread pool would not produce a
   parallel Breadth-First-Search.
   Processes + Copy-on-Write (CoW) is the Python arm.
 - **The Go case rests on speed and deployability, not on memory.**
   "Goroutines share memory, processes don't" is not an argument here:
   Python processes already share it. ADR-017 therefore frames the
   question as raw traversal speed per core plus static-binary
   deployability (Deployment, §14) — the 4 GB laptops have no Python
   environment. E4 Parallel scaling (§16.5) still records RSS, as a
   *check* that copy-on-write holds under load rather than as the
   expected differentiator.

### 2.3. Debian dependency semantics

Debian Policy §7 (<https://www.debian.org/doc/debian-policy/ch-relationships.html>):

 - A field is a comma-separated list of **clauses**; a clause is one
   or more **alternatives** separated by `|`.
 - An alternative is `name[:archqual] [(op version)]`, with
   `op ∈ {<<, <=, =, >=, >>}`. In binary `Depends` the arch
   qualifier seen in practice is `:any` (e.g. `python3:any`).
   Architecture restrictions `[...]` and build profiles `<...>` only
   appear in `Build-Depends` (source packages) — out of R2 scope.
 - A dependency names a **package and a version constraint**, not a
   concrete version. The concrete target is resolved **within the
   coordinate** (same distribution, release, and architecture or
   `all`).
 - `Depends`: the target must be unpacked and configured before the
   depender is *configured*. `Pre-Depends`: the target must be fully
   installed before the depender is *unpacked* (its `preinst` needs
   it). Both are hard dependencies for reachability; R2 records the
   difference (`kind`) so an install-ordering analysis remains
   possible later.
 - Targets may be **virtual packages** (satisfied through another
   package's `Provides`). R2 does not resolve `Provides`; such targets
   remain *unresolved* and are counted (that count is itself a
   measurement that motivates the future `Provides` work).

### 2.4. Restrictions

Same as R1 (Memory, §2.2 of the R1 project):
 -  8 GB development machine, 8 GB server (ghostell), two 4 GB old
    laptops (pavilion, pb) available as extra workers, no permanent cloud.
 - Time: **10 working days**.
A smaller complete vertical slice beats a larger incomplete one.


---
## 3. Engineering Analysis

### 3.1. Related work & positioning

R2 must not pretend to replace mature Debian tooling.

#### dose3/ `dose-distcheck` (edos)

Exact installability with a SAT solver: alternatives, `Provides`, `Conflicts`, versions

Limit: one snapshot

#### botch (bootstrap/build ordering tools)

build-dependency cycles, cycle breaking, bootstrap order for new architectures (on top of dose3)

Limit: one snapshot, one architecture

#### `debtree`, `apt-rdepends`,

Interactive exploration of one package. Based on the system's APT cache.

Limit: release and architecture of the machine it is installed on.

#### `reverse-depends`

Queries an online web service (qa.ubuntuwire.org) to obtain reverse dependency information.
Additionally, known issues indicate that reverse-depends has defects when handling virtual packages (Provides),
which may prevent it from finding complete reverse dependency relationships.

Limit: the remote service (has known functional gaps).

#### britney

Decides testing migrations. History lives in logs/excuses.

Limit: daily, not an analytical store.

Statements of fact about these tools must be verified against their
current documentation before publication (the table is from domain
knowledge, not checked).


**What R2 does differently** (its distinctive value):

 1. **Time.** Every observation carries R1 coordinates and provenance;
    the same analysis runs per release (bullseye → bookworm → trixie →
    forky → sid are all in one UDD dump) and later per snapshot date.
    Questions such as *"how did the reverse-dependency reach of libc6
    grow between releases?"* or *"which cycles appear or disappear?"*
    are queries, not one-off scripts.
 2. **Aggregated, queryable analytics.** Percentiles, concentration
    rankings, cycle inventories per coordinate, as Parquet/SQL with
    provenance — an analytical layer, not a QA gate.
 3. **Engineering.** The graph is built from durable events by
    idempotent prosumers, computed in parallel, measured, and sized
    for deployment.

R2 deliberately simplifies semantics (first alternative only, no
`Provides`, no version satisfaction check). This is acceptable for
structure and concentration analysis and **unacceptable** for
installability verdicts — R2 must never claim the latter. Ingesting
dose3/botch outputs as an additional source is a candidate for a
later release.


### 3.2. Release (R2) Scope

#### 3.2.1. In scope

 1. Prosumers that **stream** the event lake
    (the release's real volume constraint, Memory, §2.2)
 2. Eventizer concept: a source owns a collection of eventizers.
    The `normalize <source>` subcommand selects the **source**.
    `--eventizer NAME` selects one of that source's eventizers
    (default `package-versions`, R1 behaviour).
 3. `dependencies` eventizer on table `packages` → `DependencyObserved`.
 4. `popcon` eventizer on table `popcon` → `PopconObserved`.
 5. Prosumers and their tables (Analytical Storage, §10):
    1. `packages_deploy_complex_dependencies`
    2. `packages_deploy_direct_straight_dependencies`
    3. `packages_deploy_direct_reverse_dependencies`
    4. `packages_deploy_dependency_cycles`
    5. `packages_deploy_direct_dependency_statistics`
    6. `<package>-<version>-deploy-deep_dependencies`
 6. Benchmark extension + execution records for the new runs.
    (See Benchmarked Performance, §16)
 7. Go deep-dependency worker consuming a JetStream work queue.
 8. Cross-release and cross-components comparison queries (SQL files):
    the same analysis per release, and per component list
    (See Coordinates, §4.3).
 9. ADR-013..022, design docs, user guide updates.

#### 3.2.2. Known limitations (to be documented, not hidden)

 - `Recommends`, `Suggests`, `Enhances`, `Breaks`, `Conflicts`,
   `Replaces` are ignored.
 - `Provides` / virtual packages are not resolved (unresolved targets
   are counted).
 - **No installability verdicts.** R2 evaluates each clause
   independently (which versions satisfy it, Clause resolution, §5.1.2)
   but never solves a whole dependency set: no `Conflicts`, no `Breaks`,
   no SAT search over combinations of alternatives. The
   question "can this set be installed together?" belongs to dose3
   (Related work & positioning, §3.1). What R2 answers is structural:
   which clauses are satisfiable, by how many versions, and what the
   resulting graph looks like.
 - A prosumer that wants **one** edge per clause must state its
   choice rule (the greatest satisfying version is the default).
 - One UDD dump = one observation date. The temporal axis in R2 is
   *across releases*; across dump dates needs more dumps (future).
 - `Build-Depends` (source packages) is out of scope.

#### 3.2.3. Mapping to the owner's requirements (`dependency-analytics.md`)

| Item                                                 | Where                                  | Deviation |
|:-----------------------------------------------------|:---------------------------------------|:------------------------|
| 0. source classes own ingestors + eventizers         | Ingestion & the eventizer concept (§8) | registry keyed by source name (R1 has no `udddump` class) |
| 1. dependencies eventizer, unfold simple conditions  | Event Model (§6), The `dependencies` eventizer (§9.3) | the **event** carries the target *package + constraint*, not a target version (a dependency names no version); the package-version → package-version edge is produced in P2 (accepted by the owner, 2026-11-01). Complex alternatives: each clause is its own event, and P1 keeps the **whole field** (all clauses, in order) of every field with a complex clause (owner's choice A, 2026-11-01) |
| 2. straight table, first alternative + `complexdeps` | P1 | — |
| 3. reverse "index or indexed table"                  | P2 | a separate table sorted by target (Parquet has no indexes) |
| 4. statistics, percentiles                           | P3 | per coordinate; plus cycles (P4) as a table of its own |
| 5. deep dependencies                                 | P4 | adds `direction`; "a set of coordinates" = one job per coordinate |
| Benchmarking: cache sizing, parallel scaling         | E3 Cache sizing (§16.4), E4 Parallel scaling (§16.5) | — |
| to do: prosumer registry                             | Prosumer registry (§22.2) | stretch |

#### 3.2.4. Relationship with the next release (R3)

R3 consumes: the worker model, the work-queue subjects,
the execution-record metrics, the measured resource budgets
(they become Kubernetes requests/limits and SLO baselines).

R3 adds: orchestration (k3s), GitOps, Prometheus/Grafana/Loki/Tempo,
SLOs, chaos experiments over the R2 workers.

**R2 must not become R3**: no Kubernetes, no full observability
platform, no autoscaling, no SLO machinery.

#### 3.2.5. Out of scope

 - migration dynamics and build relationships
   (see Stretch Goals, §22: migration as stretch goal),
 - installability verdicts, `Provides` resolution, version
   satisfaction,
 - graph databases (see ADR-016),
 - PostgreSQL as an operational store (ADR-006 still holds),
 - Kubernetes, GitOps, full observability stack (R3),
 - AI-assisted investigation (R4).


### 3.3. Repository structure: additions

This section documents expectations, it's NOT a requirement.

```text
debian-observer.git/src/debian_observer/          # the platform
├── normalization/
│   ├── udddump.py                      # + COPY row iterator
│   └── eventizers/                     # NEW subpackage: one module per eventizer
│       ├── __init__.py                 # registry: source -> {name: eventizer}
│       ├── package_versions.py         # R1 behaviour, moved, unchanged output
│       ├── deploy_dependencies.py
│       └── popcon.py
├── consumers/
│   └── events_reader.py                # NEW library: batched event-lake reader
│                                       #   (iter_batches + fold; never read_table)
├── prosumers/                          # the prosumer library (a plug-in's substrate)
│   ├── lake.py                         # batched lake source (Reading the lake, §10.2)
│   ├── coordinates.py                   # coordinate filter, incl. component lists
│   ├── cli.py                          # common I/O options, build_source/build_sink
│   ├── stream.py                       # Record, sinks/sources (ND-JSON, NATS)
│   └── dependencies.py                 # clause/edge table builders (fold helpers)
└── cli/commands/query.py               # + AnalyticalStorage, dataset views
                                        #   (this spec's §12 API is DOPI-PDDA's)

dopi-pdda.git/src/dopi_pdda/            # the plug-in (new executables, one per table)
├── packages_deploy_deps_net_direct.py  # P1 straight (entry point)
├── packages_deploy_deps_net_reverse.py # P2 reverse (entry point)
├── dependencies.py                     # clause parser, resolution, table writers
├── graph.py                            # adjacency, BFS levels, iterative Tarjan
├── deep_go/                            # Go module (deep-dependency worker)
│   ├── go.mod
│   └── cmd/deepdeps-worker/
└── benchmarks/
    ├── queries/                        # SQL files of the cross-release comparisons
    └── results/                        # measured outputs (CSV/MD), committed
```

The dependency analysis CLIs and their entry-points moved to the plug-in:
 - the platform keeps the prosumer **library** they import
   (`prosumers.{lake,coordinates,cli,stream,dependencies}`)
 - the plug-in ships the **executables** and their tests.

Existing R1 files expected to change (non-exhaustive):
`normalization/udddump.py`, `cli/commands/normalize.py`,
`common/types.py`, `events/envelope.py`, `events/types.py`,
`messaging/config.py`, `storage/parquet.py`, `analytics/models.py`,
`analytics/store.py`, `cli/commands/benchmark.py`,
`cli/commands/execution_report.py`, `pyproject.toml`
(entry points), `docs/design/*`, `docs/architecture/overview.md`,
`docs/user-guide.md`.

- `eventizers` is a subpackage of `normalization` (not a new top-level package).
  The `normalization` contract in `docs/design/module-contracts.md` is extended accordingly.
- `deep_go/` is not a Python package.


### 3.4. Architecture Decision Records (new)

Each ADR follows the R1 ADR format:
 1. context,
 2. problem,
 3. alternatives,
 4. decision,
 5. consequences,
 6. evidence,
 7. revisit conditions.

| ADR     | Title                                     | Binds           | Decision (summary) |
|:--------|:------------------------------------------|:----------------|:-------------------|
| ADR-013 | Dependency observation model              | Both equally    | 1 event per **alternative-bearing clause**, with every alternative parsed (no first-alternative shortcut). `kind` ∈ {depends, pre-depends}. `clause_index` groups the alternatives of one clause and is a uniqueness discriminator, not an order (Dependency clause, §5.1.1). The AND (clause) / OR (alternative) levels are both preserved; constraints are **evaluated** against the coordinate's versions (Clause resolution, §5.1.2) |
| ADR-014 | Eventizers per source                     | Debian Observer | A source owns a registry of eventizers; `normalize <source> --eventizer NAME`; R1 behaviour = default eventizer `package-versions` |
| ADR-015 | Streaming reads of the event lake         | Both equally    | Neither reading UDD nor writing events needs streaming (measured flat at 267 MB for 8.4 M events). The wall is the **prosumer read**: R1's `read_table`+`to_pandas`+`list(source)` extrapolates to **19.3 GB** for the real lake vs **0.73 GB** with `iter_batches` (Memory, §2.2). Prosumers consume the log in batches and fold as they go. They may materialize heavily filtered outputs. |
| ADR-016 | No graph database, no Hive partitioning   | Both equally    | DuckDB SQL for joins/aggregates + in-memory algorithms (BFS, Tarjan) in the prosumers. Derived tables are one directory per table, one file per `(coordinate, components)` with those values in the columns (Layout, §10.1 — researchers query cross-coordinate. Hive leaks storage layout into every reader and strips the coordinate from the files). Revisit conditions stated |
| ADR-017 | Go for parallel graph workers             | DOPI-PDDA       | Go worker is the experimental arm; Python `fork()`+CoW is the baseline, sharing the graph rather than copying it (measured 0.9 MB/child, Memory, §2.2). The decision rests on traversal speed per core and on static-binary deployability (Deployment, §14), not on memory per worker; the measured result decides |
| ADR-018 | Work distribution over JetStream          | DOPI-PDDA       | Work-queue stream for deep-dependency jobs. Explicit ack after the output file is atomically written |
| ADR-019 | Idempotency by deterministic output keys  | Both equally    | Derived tables are folds by primary key and per-job outputs have deterministic paths. Supersedes R1's "persistent IdempotencyStore deferred to R2" (`module-contracts.md` `idempotency` block and the `idempotency.py` docstring are updated to point here) |
| ADR-020 | Cache budget for deep dependencies        | DOPI-PDDA       | E3 (Cache sizing, §16.4) measures the cache curve; the ADR fixes the sample (`all` / `top-indegree:N` / `top-popcon:N` / `stratified:N`, Work distribution, §11.2) and the N that fit an explicit storage budget. The API's cached subset is whatever this budget allows — never a complete cache (CLI & API, §12) |
| ADR-021 | Component list as part of the coordinate  | DOPI-PDDA       | A coordinate is `(distribution, release, architecture, components)`; `components` is a multi-valued selector exactly like `architecture` (Coordinates, §4.3). Every derived row carries the list it was built under |
| ADR-022 | Plug-in API endpoints                      | Debian Observer | API endpoints are mounted from plug-in-discovered FastAPI routers: the platform discovers routers by entry point (`debian_observer.api_routers`) and mounts them under the shared coordinate prefix — **it never imports or depends on any plug-in**; the dependency arrow stays plug-in → platform (API Extensibility, §12.1). A path-template collision between two plug-ins **fails fast at startup** with both packages named — kept deliberately: it prevents mistakes |

ADR-017 is written in two steps:
 1. decision frame first (day 1),
 2. evidence and verdict after measurement (day 9).


---
## 4. Data Sources & Datasets

### 4.1. Primary source

UDD `pg_dump` file, as acquired by R1 (`ingest udddump --url …`,
raw artifact preserved byte-for-byte, ADR-002/012).
No new acquisition path is needed.

Tables used (all present in the dump's TOC as `TABLE DATA`):

| Table                  | Columns used | Eventizer |
|:-----------------------|-----------------------------------------------|:---|
| `packages`             | `package, version, architecture, distribution, release, component, depends, pre_depends` (`provides` kept raw for future work, see New event types, §6.1) | `dependencies` (+ R1 `package-versions`) |
| `popcon`               | `package, insts, vote, olde, recent, nofiles` | `popcon` |
| `testing_autoremovals` | `source, version, bugs, first_seen, last_checked, removal_time, rdeps, buggy_deps, bugs_deps, rdeps_popcon` | stretch (Stretch Goals, §22) |

Verified on 2026-11-01 against the local restore
(`\d packages`, `\d popcon`, `\d testing_autoremovals`).

### 4.2. Dataset manifest

Unchanged from R1. Each eventization run records in its execution
record which eventizer and which coordinate filters were used.

### 4.3. Coordinates

**A coordinate is `(distribution, release, architectures, components)`.**

All four are selectors over the same `packages` rows.

| Field          | Values | Selects |
|:---------------|:----------------------------------------------------------------|:--------|
| `distribution` | `ubuntu`, `debian`, …                                           | the archive |
| `release`      | `trixie`, `sid`, …                                              | the release |
| `architecture` | (`all` +) `amd64` | `arm64` | …                                 | the machines the packages run on |
| `component`    | a **list**: `main`, `contrib`, `non-free`, `non-free-firmware`  | the licensing scope the system draws from |

`architecture` and `components` are both *multi-valued* in practice:
an `amd64` graph always includes the `all` packages, and a component
selection is a list of `main` plus optional components.
Both are therefore written as lists.

`component`:
 - is a coordinate because it defines a scope of study.
   Restricting a system to `main`, or to `main,non-free-firmware`, is a
   normal and meaningful way to run Debian, and the *frontiers* of those
   selections are what interests the free-software movement: "can this
   system be built from free software alone?" is a first-class research
   question, and a model that cannot express the selection cannot ask it.
 - sets the archive area a package version lives in, as UDD reports it.
 - sub-components such as `*/debian-installer` normalize to their parent
   (`main/debian-installer` → `main`).
   The rule is part of the contract and tested (Unit tests, §19.1).
 - **component list** — the `component` element of a coordinate: the
   unordered, de-duplicated list of components a graph may draw from.
   Written with `+` as the separator (`main+non-free-firmware`),
   so a coordinate is always one filesystem-safe token.
   (CLI: `--components main+non-free-firmware`)

Architecture `all` is a wildcard within the same `(distribution, release)`.
The architecture `all` is not built on its own.

Resolution happens **within a coordinate region**: a resolution target that
exists only outside the coordinate region is `target_resolved = false` with
`unresolved_reason = 'outside-region'`, distinct from `'virtual'`
(a `Provides` target, Debian dependency semantics, Debian dependency semantics, §2.3)
and `'absent'`.
Counting `outside-components` *is* the frontier measurement —
"how far does this selection reach on its own?".

#### What the measurements say about component frontiers

Measured on the `main` selection, trixie amd64+all
(*measured 2026-11-03*):

| Fact                                                     | Value   |
|:---------------------------------------------------------|--------:|
| `main` clauses satisfiable **inside** `main`             | 252,252 |
| `main` clauses that **require** a package outside `main` |   **0** |
| `main` → `main` edges (first alternative)                | 252,180 |
| `main` → outside `main` edges (first alternative)        |   **2** |
| `main` clauses rescued alternatives                      |  **72** |
| rows: main / non-free / contrib / non-free-firmware      | 69,251 / 742 / 304 / 44 |

Two findings, not mechanics:

 1. **`main` is self-contained**, as Debian Policy §2.2.1 requires:
    none of its 309,082 clauses needs a package outside `main`.
 2. **The apparent exceptions are artefacts of simplification.** The
    2 `main`→non-`main` edges are
    `python3-tabnet: python3-torch-cuda | python3-torch` and
    `minigalaxy: unrar | unrar-free` — in both, the alternative
    *inside* `main` exists, so resolving only the first alternative
    **manufactures a boundary violation that does not exist**.
    First-alternative is a simplification.
    For accurate graphs, component frontiers must be computed over
    the full OR structure (Clause resolution, §5.1.2).

#### The coordinate matrix

The `debian` base releases in the dump — `bullseye`, `bookworm`,
`trixie`, `forky`, `sid` — give **37 release×architecture
coordinates** (*measured 2026-11-01*).

Nothing in the data justifies cutting it:
the whole `packages` table is 1.5 M rows, one coordinate is ~70 k nodes,
and a `main`-only graph is *slightly* smaller than the all-components one
(69,251 of 70,341 rows in trixie amd64+all).

The extra selections cost a fraction of a full pass each, so R2
builds and stores the **full matrix** of those 37 against the
coordinate regions it studies.

The cost is measured and publishedm(E2 Direct tables, §16.3)
rather than assumed, and if a selection turns out to be expensive
the evidence — not a guess — decides whether to keep it.

The component lists R2 studies are a parameter, not a hard-coded set;
these three are the ones the demonstration reports:

| Component list                                      | Meaning |
|:----------------------------------------------------|:--------|
| `main`                                              | DFSG-free only; the free-software question |
| `main+non-free-firmware`                            | free software plus the firmware most machines need |
| `main+contrib+non-free-firmware+non-free` (default) | the whole archive |

Pockets (`*-security`, `*-backports`, `*-proposed-updates`,
`experimental`) are partial overlays of a base release, not
self-contained graphs.

---
## 5. Data Model

### 5.1. Domain entities (DOPI-PDDA's)

#### 5.1.1. Dependency clause

One element of the comma-separated `Depends`/`Pre-Depends` list of
one binary package version in one coordinate.

```text
raw_clause:   the clause text, trimmed
clause_index: discriminator that makes this observation unique within
              its field. Its value is the clause's parse position
is_complex:   true when the clause has alternatives ('|')
alternatives: count of alternatives (1 for simple)
depender:     package, version
kind:         depends | pre-depends
first alternative, parsed:
  target_package,
  constraint_op (<<, <=, =, >=, >> or null),
  constraint_version (or null)
```

**Why `clause_index` exists.**

Because a field can hold two clauses with the same target:

```text
libc-bin: Depends: libc6 (>> 2.41), libc6 (<< 2.42)
```

But Debian Policy §7.1 gives a comma-separated list,
and position means nothing (Ordering, §6.3 says the same).

Therefore `clause_index` is **not** a semantic ordering field.
The count is just a simple and efficient *discriminator*.
Anything that reads it as "position means something" is a bug.

Measured (*measured 2026-11-03*):
keying on `target_package` instead of `clause_index`
**collides on 3,142 rows in trixie amd64+all alone** (3,116 fields),
each collision merging two clauses and silently losing a version bound.
Keying on `raw_clause` text also works
(0 duplicate clause texts measured)
but makes the PK a string up to 9,501 chars,
so we choose the int discriminator.

#### 5.1.2. Clause resolution: AND, OR and versions

A relationship field is **a conjunction of disjunctions**, and R2's
model keeps both levels because they mean different things:

```text
field   = clause AND clause AND clause        -- all must hold
clause  = alt   OR  alt   OR  alt             -- any one suffices
alt     = name[:archqual] [(op version)]      -- a constrained target
```

Measured on trixie amd64+all (*measured 2026-11-04*): 313,761 clauses,
of which **6,929 carry an OR** (up to 21 alternatives in one clause),
and **178,673 (57 %) carry a version constraint**. Neither level is a
rounding error, so neither may be flattened silently.

**The AND/OR distinction is a property of the edge, not a detail.**
Collapsing an OR clause to one alternative turns *a choice* into *a
fact*, which is how a false Policy violation got manufactured in the
first-alternative component measurement (Coordinates, §4.3).
R2 therefore classifies every resolved edge:

| Edge class    | Comes from | Means |
|:--------------|:-----------|:------|
| `required`    | a clause with exactly one alternative | the target cannot be avoided |
| `alternative` | one alternative of an OR clause | *a* way to satisfy the clause |

Alternatives of the same clause share a `clause_index`
(Dependency clause, §5.1.1), so a consumer can always regroup them and
ask "is this clause satisfied?" instead of "does this edge exist?".
Consumers then choose the semantics their question needs:

 - reachability / blast radius → follow `required` edges, and treat an
   OR clause as satisfied if **any** alternative resolves.
 - unavoidable structure (cycles, Policy checks) → `required` only.
 - exploration of choice → the `alternative` edges themselves.
 - estimations → required plus 1 chosen alternative. The selection
   logic depending on the estimation's purpose.

#### Version constraints are evaluated

Constraints are **not** merely recorded. The whole point of an
operator and a version is to say *which* versions satisfy the clause,
and the consumers need that answer:

 - **filter** the candidate versions to those satisfying `(op version)`.
 - **count** them (`candidates`), which is itself a measurement — a
   clause with zero candidates is an unsatisfiable dependency in that
   coordinate.
 - **pick one** when a single edge is wanted, by an explicit, stated
   rule (e.g. "the greatest satisfying version"), never by accident
   of row order.

Comparison uses Debian version semantics — epoch, upstream version,
revision, with `~` sorting before everything (Debian Policy §5.6.12).
`python-debian`'s `debian.debian_support.version_compare` implements
it and is **already present in the environment** (it appears in R1's
SBOM as a transitive dependency, verified 2026-11-04: `1:2.0` > `2.0`,
`1.0-1` > `1.0-1~bpo`, `2.41` < `2.41+deb13u4`). Promoting it from
transitive to declared is a one-line `pyproject.toml` change and the
only new dependency R2 asks for (Security & Supply Chain, §17).

What each prosumer does with the candidate set is **its own choice**,
declared in its contract — that is the point. The resolution library
offers the primitives (`satisfying_versions`, `pick_greatest`,
`is_satisfiable`); it does not impose a policy.

#### The derived edge

A directed edge `depender (package, version) → target (package,
version)` inside one coordinate, obtained by resolving an alternative's
`target_package` against the package versions present in that
coordinate — same architecture or `all`, and inside the coordinate's
component list (Coordinates, §4.3) — then keeping the versions that
satisfy the alternative's constraint.

Each edge carries `edge_class` (`required` | `alternative`),
`clause_index`, `alternative_index`, and `candidates` (how many
versions satisfied the constraint). A clause whose alternatives all
fail to resolve is kept once as a loose end with
`target_resolved = false`, `target_version = null` and an
`unresolved_reason`:

 - `'outside-components'` — the package exists in the release, but not
   in this coordinate's component list (the frontier measurement);
 - `'virtual'` — only a `Provides` target
   (Debian dependency semantics, §2.3);
 - `'unsatisfiable'` — the package exists here, but **no** version
   satisfies the constraint (only visible because constraints are
   evaluated);
 - `'absent'` — not in the archive at all.

Multi-version targets are rare but real, which is why the candidate
set is a set: 5 package names carry more than one version in trixie
amd64+all, 558 in sid, up to 3 versions each
(*measured 2026-11-04*).

#### 5.1.3. Popularity

Popcon counters per binary package name (no version, no coordinate):
`insts, vote, olde, recent, nofiles`. Used as an impact weight.

### 5.2. Invariants

 - R1 invariants hold (field semantics of distribution/suite/release,
   frozen models, writer-owned serialization).
 - Parsing is total: a clause that cannot be parsed is not dropped
   silently — it becomes an invalid record with the cause
   (R1 behaviour for transformation failures).
 - The parser is a pure function `parse_relationship_field(text) ->
   list[Clause]`, documented with Debian Policy §7.1 as the cited
   standard.


---
## 6. Event Model (Debian Observer's)

### 6.1. New event types

Envelope: unchanged R1 `EventEnvelope`
(identity, schema version, timestamps, provenance).

| Event type           | Subject         | Schema version |
|:---------------------|:-----------------------------|---|
| `DependencyObserved` | `debian.dependency.observed` | 1 |
| `PopconObserved`     | `debian.popcon.observed`     | 1 |

`DependencyObservedPayload` fields:
 - `observation` (R1 `ObservationMeta`),
 - `distribution`, `release`, `architecture`, `component`
   (the observed component of the package; the component *list* of a
   coordinate, Coordinates, §4.3, is a property of the derived graph,
   not of the observation — one eventization serves every list),
 - `package`, `version`, `kind`,
 - `clause_index`, `raw_clause`, `is_complex`, `alternatives`,
 - `target_package`, `target_archqual`, `constraint_op`, `constraint_version`.

`PopconObservedPayload` fields:
 -  `observation`,
 -  `package`,
 -  `insts`, `vote`, `olde`, `recent`, `nofiles`.

`provides`
 - is **not** eventized in R2 (it would need its own event type)
 - the raw artifact keeps it, so a later eventizer can add it
   without re-downloading (R1 property: re-eventize the same artifact).

Plumbing to touch (R1 registers event types in several places):
 - `common/types.py` (`EventType` and, if used for the new entities,
   `EntityType` Literals),
 - `events/types.py` (payload classes),
 - `events/envelope.py::get_payload_model` (payload mapping),
 - `messaging/config.py` (`DEFAULT_SUBJECTS`, `StreamConfig.subject_for`),
 - `storage/parquet.py` (per-type schema, Normalization CLI, §9.1).
 - `entity_id`: `DependencyObserved` → R1 `package_id` of the depender
   (`<package>:<architecture>`, as `PackageVersionObserved`)
 - `PopconObserved` → `<package>` (popcon has no architecture).

Decision (ADR-013): **one** event type for simple and complex
clauses, discriminated by `is_complex`.
Rationale: one subject, one schema, one parser;
the complex consumer filters.
The requirement
"complex alternatives will be observed separately" is satisfied: a
complex clause is its own observation, kept whole in `raw_clause`;
the whole field is rebuilt by P1 from `clause_index` order.

### 6.2. Identity and idempotency

 - `event_id`: as R1 (UUIDv4 per event instance; `Nats-Msg-Id` on
   publish).
 - Logical primary key of a dependency observation:
   `(distribution, release, architecture, package, version, kind,
   clause_index)`.
   - Re-eventizing the same dump yields new `event_id`s but the
     same keys, and derived tables fold by key (ADR-019), so
     `fold(A) == fold(A, A, A)` as in R1 §15.1.
   - Verified against the dump (*measured 2026-11-03*): this key has
     **0 collisions** over all 8,376,616 `Depends` and 28,607
     `Pre-Depends` clauses.
   - `component` is an observed attribute of the row and part of
     the coordinate's **component list** (Coordinates, §4.3),
     but it is not in the observation key: a package version sits in
     exactly one component, so adding it would be redundant, and
     the key must stay stable across component lists — the same
     observation feeds the `main` graph and the all-components one.
 - Random-by-design fields, excluded from every equality check in
   this document: `event_id`, `observed_at`, `effective_at`,
   `ingestion_run_id`, and any column derived from them.

### 6.3. Ordering

No ordering is required, and clause position carries **no domain
meaning** (Debian Policy §7.1: a relationship field is an unordered
comma-separated list). `clause_index` is a uniqueness discriminator,
not an order (Dependency clause, §5.1.1).

Where this document says a prosumer emits something "ordered by
`clause_index`" (P1's `field_raw` and `complex_list`),
that is a **presentation or determinism** convention so
the same input always produces byte-identical output.
Never a claim about Debian semantics.

---
## 7. Architecture

```text
                  UDD pg_dump (raw artifact, R1)
                             │
            normalize udddump --table T --eventizer E
                             │  (COPY iterator, bounded)
          ┌──────────────────┼──────────────────┐...other event types
          ▼                  ▼                  ▼
 PackageVersionObserved  DependencyObserved  PopconObserved
          └──────────────────┼──────────────────┘...other event types
                             ▼                   
                         event log ──►  publish CLI ──► JetStream/FastAPI
                      (Parquet. DuckDB)                        │
                             ├─────────────────────────────────┘
                         DOPI-PDDA
          ┌──────────────────┼───────────┐...other (DOPI) consumers
          ▼                  ▼           ▼
    R1 consolidate        popcon   straight table
   package_versions       table?         ├──► reverse table
          │                  │           ├──────────┼───► statistics
          ▼                  │           ├──────────┼───► cycles
   packages table            └───────────┼──────────┘
   (for ref check)                       ▼
                        deep-dependency jobs (work queue, JetStream)
                                 ┌───────┼────────┐
                                 ▼       ▼        ▼
     (Python baseline | Go)   worker   worker   worker
                                 └───────┼────────┘
                                         ▼
                 <package>-<version>-deploy-deep_dependencies (cache)
```

Principle (inherited from R1) regenerable state:
The event log is the backbone,
every analytic table is derived directly or indirectly from it.

---
## 8. Ingestion & the eventizer concept (Debian Observer)

Ingestion is **unchanged**: R2 reuses the raw UDD dump artifact.

Terminology (also added to the design docs):

 - **ingestor**: acquires a raw artifact from a source (R1 `ingest`).
 - **eventizer**: the source-specific function that, when the user
   runs `normalize`, reads a raw artifact and emits domain events (it
   *observes* the source). A source owns a collection of
   ingestors and a collection of eventizers.

R1 has no `udddump` source class (the raw source is just the name
`udddump` stamped in the manifest). R2 therefore materializes the
"source class" as a registry keyed by source name in
`normalization/eventizers/__init__.py`: `EVENTIZERS["udddump"] =
{"package-versions": …, "dependencies": …, "popcon": …}`. Ingestors
stay where R1 has them (`ingestion`). Each eventizer is an object
with `name`, `event_type`, `shape` (a `TableShape`) and
`eventize(row) -> Iterator[EventEnvelope | InvalidRecord]`.

 - **prosumer**: consumes events/records and produces derived records
   or tables (R1 term, unchanged).


---
## 9. Normalization: eventizers (Debian Observer)

### 9.1. CLI

```text
debian-observer normalize udddump ARTIFACT --table TABLE
    [--eventizer package-versions|dependencies|popcon]
    [--limit N] [--output FILE] [--invalid FILE] [--events-dir DIR]
```

 - `--eventizer` default `package-versions` (R1 behaviour; output
   equal to R1 except for fields that are random by design:
   `event_id`, `observed_at`).
 - Each eventizer declares its own required table shape. R1's
   `match_table_shape` returns the *first* registered shape matching
   the columns, which cannot discriminate eventizers (the
   `dependencies` shape is a superset of `packages`). R2 gates against
   **the shape declared by the selected eventizer**:
   `assert_supported_table_shape(columns, *, table, shape=None)` —
   `shape=None` keeps R1 behaviour; the CLI passes
   `eventizer.shape`.
   Shapes:
    - `package-versions` = R1 `packages` shape;
    - `dependencies` requires `package, version, architecture, distribution, release, depends, pre_depends`;
    - `popcon` requires `package, insts`.
   A mismatch fails fast naming
    - the eventizer,
    - its required columns and
    - the eventizers whose shape the table does match
      (CLI `--help` rule of CONTRIBUTING: show the perimeter).
 - `--output` (JSON lines, R1) is kept.
 - `--events-dir DIR` (new) writes the **Parquet event log**
   (one file per run, as R1 `ingest --events-dir`),
   which is the input of the prosumers.
 - At least one of `--output` / `--events-dir` is required for non-R1 eventizers.
 - Parquet layout is writer-owned (ADR-011):
   - R1's `_DEFAULT_SCHEMA` in `storage/parquet.py` is the `PackageVersionObserved` schema;
   - R2 registers one schema **per event type**
     (envelope columns + flattened payload columns),
     selected by the writer from the event type. This part is
     mandatory: writing a `DependencyObserved` through R1's schema
     would silently drop every dependency payload field.
   - **One table per event type.** A Parquet file holds one table, so
     an event type gets its own files and its own schema — there is no
     reason to union different event types into one view and pay for
     it. Measured on a deliberately mixed directory
     (*measured 2026-11-03*): the unioned schema is as wide as the sum
     of the types and the type-specific columns read NULL everywhere
     else (`insts` NULL in 10,000 of 15,000 rows), which is pure cost
     for zero gain.
   - Layout: one directory per event type, a file per run inside it.

     ```text
     data/events/<event_type>/<run-id>.parquet
     ```

     `<event_type>` uses the snake_case form of the type
     (`package_version_observed`, `dependency_observed`,
     `popcon_observed`). A read is then a plain glob of one directory
     with one schema. R1's `consolidate --events-dir` keeps working
     because its directory now contains only its own type.
   - A **multi-type eventizer is still allowed**: it writes one file
     per event type per run, each into its type's directory. The
     eventizer is free to emit several types; the storage stays
     one-table-per-type.
   - Mandatory contract: a reader of one event-type directory **must
     not** need `union_by_name` to succeed. A contract test
     (Contract tests, §19.4) asserts that each type's directory reads
     with a plain `read_parquet` glob, and that column order drift
     within one type is handled (R1 writes columns in schema order, so
     this is a guard against a future writer reordering them).

### 9.2. Reading the dump and writing events

**Normalization needs no streaming machinery.** Reading UDD and
writing events is incremental by nature, and measurement confirms it:
peak RSS is flat from 200 k to 8.4 M events (Memory, §2.2). The design
below is therefore the *simple* one, and R2 adds nothing to it beyond
the eventizer indirection.

- `read_dump_records` is complemented by an iterator over the COPY
  block read from `pg_restore` stdout through a pipe
  (`subprocess.Popen`, line iteration); `read_dump_records` keeps its
  signature (returns the list built from the iterator) so R1 callers
  and tests are untouched. The **full row** (all COPY columns) is
  passed to the eventizer.
- R1's fixed field mapping becomes the `package-versions` eventizer.
- Each row goes one at a time through the eventizer (for
  `package-versions`, R1's per-record `Normalizer.normalize`) and each
  event goes straight to the Parquet writer, which flushes row groups
  of a fixed size (e.g. 100 k rows).
  No reason to build any list of events.
- Peak RSS is recorded per run like every other figure
  (E1 Eventization, §16.2).
  It is reported as a *sanity check*, not as a constraint under threat.

The volume problem of this release lives one stage downstream, in the
prosumers that read the event lake (The prosumers, §10.2).

`normalize` records an execution run and `ExecutionType` gains
`normalization`. R2 then writes one record per `normalize` run to
that same database (`data/raw/x.dump` → `data/execution_runs.duckdb`,
which is `execution-report`'s default `--db`).

### 9.3. The `dependencies` eventizer

For each `packages` row: parse `depends` and `pre_depends`
- (either may be null/empty → no events).
- split on `,` → clauses (trim; skip empty).
- parse each alternative with the grammar
  `name (':' archqual)? ( '(' op version ')' )?` — name per Debian
  Policy §5.6.1 (R1 `common.validators` package-name rule), version
  per R1's version validator, whitespace inside the parentheses
  optional;
- unparseable clauses → invalid records (with the cause), not events.
- the deprecated operators `<` and `>` (Policy §7.1) are not accepted:
  they become invalid records with cause `deprecated operator` and are
  counted.
- emit one `DependencyObserved` per clause. 

### 9.4. The `popcon` eventizer

One `PopconObserved` per `popcon` row.


---
## 10. Analytical Storage: derived dependency tables

This describes a Debian Observer standard, mandatory for all its
analytical plug-ins.

### 10.1. Layout

One directory per derived table; the files inside it are an
implementation detail of that table's writer:

```text
data/derived/<table>/<file>.parquet
```

**Every derived row carries its full coordinate in columns** —
`distribution`, `release`, `architecture`, `components`
(Coordinates, §4.3). That is the only rule a reader needs: a file is
self-describing, and a query filters on columns, never on paths.

How a table splits itself into files is the writer's business, not a
contract:

 - the deep-dependency cache writes one file per job, named for its
   root and coordinate;
 - the popcon table has no coordinate and is a single
   `data/derived/popcon/popcon.parquet`.

Names are a convenience for humans and for atomic replacement. They
are **not** a query interface: nothing in R2 parses a path to recover
a value, and no reader is allowed to require it.

```text
data/derived/deep_dependencies/<file>.parquet
```

For the deep-dependency cache the file name is
`<distribution>-<release>-<architecture>-<components>-<package>-<version>.parquet`
— flat, one directory, no nesting. In `version`, `:` (epochs) is
percent-encoded (`%3A`) and so is `%` itself (`%25`), to stay
filesystem-safe and reversible; the encoding rule is part of the
contract.
The directory holds many files (a sampled cache, see the budget in
CLI & API, §12). That's a natural result, not a partitioning scheme.

**No Hive partitioning**
Coordinates live in columns, file names are flat.

 1. **Researchers work cross-coordinate by default.** The headline of
    R2 is the *dynamics* — evolution across releases
    (Domain and Product Purpose, §1.1; Demonstration Scenario, §20).
    For a query that reads every coordinate, Hive's pruning buys
    nothing: all partitions are read either way.
 2. **Hive makes the programs know too much about the storage.** Every
    reader has to pass `hive_partitioning=true` and know which columns
    are path segments rather than data — error-prone and painful to
    maintain, for a layout detail that should be invisible.
 3. **Hive files are not self-describing.** Measured: a partition file
    read on its own has columns
    `[package, version, target_package, target_resolved, kind,
    clause_index]` — `distribution`, `release` and `architecture` are
    *gone*, because they exist only in the path. For R2 that is a real
    provenance hazard: Work distribution (§11.2) ships derived files
    to workers over a shared directory, and a file that has lost its
    coordinate is unusable on arrival.


### 10.2. The prosumers

Each prosumer is a standalone executable built on library code under
`src/debian_observer/prosumers/`, per CONTRIBUTING's prosumer layout rule.
The `consolidate` example is the reference for entry points and `--help` style.
When provided by the platform (Debian Observer), it lives under
`src/prosumers/`.

#### I/O: R1's four options (unchanged, Debian Observer)

R2 adds **no new I/O vocabulary**. Every prosumer speaks the dialect
R1 already defines in `debian_observer.prosumers.cli::common_io_options`
and documents in the user guide:

| Flag              | Meaning                 |
|:------------------|:------------------------|
| `--file-in PATH`  | read Parquet from disk  |
| `--stream-in`     | read ND-JSON from stdin |
| `--file-out PATH` | write Parquet to disk   |
| `--stream-out`    | write ND-JSON to stdout |

`--file-in` and `--file-out` are **repeatable** in R2, and accept a
directory or a glob as well as a single file — that is the only extension,
and it keeps pipes working exactly as the user guide shows them.
An event-log directory or a derived-table directory is just a path handed to
`--file-in`.

(`--events-dir` stays what R1 made it — an option of `ingest`, `consolidate`,
and now `normalize`, naming where the log is *written*.
It is not part of the prosumer I/O dialect.)

On top of the four flags, the graph prosumers take the coordinate selectors
(§4.3):

```text
--distribution D  --release R  --architecture A
--components main+non+free-firmware       # Plus-sign-separated list
```

`--components` is a list, like any other multi-valued selector.
It is almost a filter (defaults to "all components") to be applied
**before** the graph is built. It decides which packages exist for resolution.

#### Reading the lake: batched (new, Debian Observer)

**This is where streaming becomes mandatory** (Memory, §2.2). R1's
`FileSource.__iter__` loads the whole file — `pq.read_table()` then
`.to_pandas()` then `iterrows()` — and `consolidate` adds
`records = list(source)` on top. Measured, that path needs **19.3 GB**
for the real 8.4 M-event lake against **0.73 GB** when read in
batches: a 26× difference between "does not run on the hardware" and
"runs comfortably".

R2 therefore adds a batched source to the shared prosumer library
(`prosumers/lake.py`), so every prosumer gets it from the same place R1 gets
`FileSource`: `iter_batches()` under the hood (or DuckDB, which streams
natively), folding batch by batch and keeping only the accumulator for the
coordinate in hand. R1's `FileSource` stays as it is for the small single-file
cases it already serves.

Reads use plain `read_parquet()`
— **no `hive_partitioning`** (Layout, §10.1) — 

#### P1 `packages_deploy_direct_straight_dependencies` (DOPI-PDDA)

prosumer `packages_deploy_direct_straight_dependencies`
will read the event log and 
will create a table  `packages_deploy_direct_straight_dependencies` with
all the dependency network, **indexed by depender** (first package, version).

In case of an incoming complex dependency, we'll initially simplify it by
selecting the first alternative and marking an output field `complexdeps='yes'`.

Sorted by `(package, version)`. Purpose: walk the graph **upwards**
(what does X need?).

#### P2 `packages_deploy_direct_reverse_dependencies` (DOPI-PDDA)

`packages_deploy_direct_reverse_dependencies`
will read the `packages_deploy_direct_straight_dependencies` table and
will create a table `packages_deploy_direct_reverse_dependencies` with
all the same dependency network, **indexed by dependency** (second package).

Purpose: walk the graph **downwards** (who needs X?).

#### P3 `packages_deploy_direct_dependency_statistics` (DOPI-PDDA)

Input: P1, P2. One row per coordinate:

```sql
distribution, release, architecture,   -- coordinate (columns, Layout, §10.1)
components,                            -- component list (Coordinates, §4.3)
packages, edges, complex_share,
unresolved_edges,                      -- total
unresolved_outside_components,         -- the frontier: edges this
                                       -- component list cannot satisfy
unresolved_virtual, unresolved_absent,
cross_component_edges,                 -- within the list (null for 'main')
out_degree_p50, _p80, _p90, _max,      -- direct dependencies per package
in_degree_p50,  _p80, _p90, _max,      -- direct reverse dependencies per package
sccs_hard, largest_scc_hard,           -- from P6 cycle_class='hard'
sccs_potential, largest_scc_potential  -- from P6 cycle_class='potential'
```

 - Degrees count distinct target/depender packages (not versions).
 - Packages with degree 0 are included in the percentiles (every
   package of the coordinate is a row of the degree distribution).
 - `complex_share` = complex clauses / all clauses of the coordinate.
 - Percentiles via DuckDB `quantile_cont`. Purpose: **estimate time and
   size of graph expansions** and compare coordinates across releases —
   a **domain** result about Debian's shape, standing on its own.

(The benchmark happens to consume these percentiles when it sizes the
cache, but that is the benchmark's business: this table answers a
question about Debian, not about our software.)

#### P4 `<package>-<version>-deploy-deep_dependencies` (cmd `packages-deploy-deps-deep`)

Input: coordinate + `package` + `version`
Options: `--max-level N` (default unlimited).

Crawls P2 (up) and P3 (down) of that coordinate, plus the popcon table.
It stops crawling at max-level, if provided.
One row per reached node and direction:

```text
package, version,                 -- the root (constant per file)
direction (depends | rdepends),
level (1..),                      -- shortest distance from the root
dep_package, dep_version, popcon_insts (null if unknown)
```

The Python implementation is the baseline.
The Go worker (Work distribution, §11.2) implements the
same contract and must produce equal files (order-insensitive
comparison).

#### Optional: P5 `packages_deploy_dependency_cycles` (DOPI-PDDA)

**Cycles need the unfolded graph.** P2 keeps only the
**first** alternative of a complex clause (Known limitations, §3.2.2), so a cycle that
exists only through a non-first alternative is invisible to it, and a
cycle P2 *does* report may be an artefact of having picked the first
alternative. Running Tarjan on P2 alone would therefore produce a
cycle inventory that is neither sound nor complete, and that is the
one result Domain and Product Purpose (§1.1) calls the first validation use case.

So P4's input is the **unfolded** edge set, not P2:

 - **Input**: the event log (`DependencyObserved`, all clauses) plus
   P1's `complex_list` and the `package_versions` table for
   resolution. Every alternative of every clause is expanded to an
   edge, each tagged with `alternative_index` and `is_complex`.
 - **Two edge classes**, kept apart because they answer different
   questions:
   - **hard edges** — the only alternative of a simple clause: the
     dependency cannot be satisfied any other way;
   - **optional edges** — one alternative of a complex clause: the
     dependency *may* be satisfied this way.
 - **Two verdicts per SCC**, from the same run:
   - `hard` — the SCC survives using hard edges only. This is a real,
     unavoidable cycle.
   - `potential` — the SCC needs at least one optional edge. It exists
     only under a particular choice of alternatives, and the row
     records which (`via_alternatives`).

Output, one row per member:

```sql
distribution, release, architecture,   -- coordinate (columns, Layout, §10.1)
components,                            -- component list (Coordinates, §4.3)
scc_id, scc_size, package, version, component,
cycle_class,                           -- 'hard' | 'potential'
via_alternatives                       -- list<struct{package, version,
                                       --   target_package, clause_index,
                                       --   alternative_index}>, null for 'hard'
```

Cycles depend on the component list: a cycle through `non-free` does
not exist under `main`. Running P4 on both lists answers
"does restricting to free software remove or create cycles?" — and
`component` on each member shows which cycles straddle components.

Iterative Tarjan (no recursion-limit issues), SCCs of size > 1 plus
self-loops. `scc_id` is deterministic: the `package=version` of the
lexicographically smallest member, comparing `package=version` strings
by **byte order** (not Debian version order).

Consequences recorded elsewhere in this document: P4 does not depend
on P2 (it can run as soon as the event log and P1 exist, Execution
Plan (Execution Plan, §21) day 5);
P5's `sccs`/`largest_scc` are reported per `cycle_class`; and the
first-alternative simplification is a limitation of the *direct*
tables (P2/P3) only — never of the cycle inventory.
Validation (Validation against reality, §19.3) compares the `hard` set against known Debian cycles
on a sample.


---
## 11. Messaging

### 11.1. Events (Debian Observer)

New subjects `debian.dependency.observed` and `debian.popcon.observed`
are registered (New event types, §6.1) so the stream view can be built.

**Whether to publish them is an open decision for the owner.**
Dropping it would remove a program-level capability (P2's "multiple
specialized consumers over NATS", s2 §4.2) to protect a configuration
limit, so it is not a designer's call. It is recorded in Open
Decisions (Open Decisions, §24) as decision **D1**; what is actually known:

 - The constraint is real but small: R1's `debian-events` stream has
   `retention=limits`, `max_msgs=100_000`, `max_bytes=1 GB`
   (`messaging/config.py`), while a full `dependencies` eventization
   is **8.4 M events** (Memory, §2.2) — roughly 84× the message cap, so the
   oldest events would be evicted silently.
 - It is also cheap to lift: the limits are constructor arguments of
   `StreamConfig`, and `ensure_stream` already creates the stream from
   them. A dedicated stream (`debian-observations`) with
   `max_msgs`/`max_bytes` sized for 8.4 M events, or
   `retention=workqueue` per consumer, is a configuration change plus
   a disk budget — not new machinery.
 - What it costs to keep: ~8.4 M × ~400 B ≈ **3.4 GB** of JetStream
   file storage for the full dump, on an 8 GB server that also runs
   Postgres and the derived tables. That is the real reason to think
   twice, and it is a *budget* question for the owner.

Until D1 is decided, the prosumers read the **Parquet log** (R1
principle: the log is the backbone, the stream is a view), which is
what makes publishing optional rather than load-bearing. JetStream in
R2 is used for **work distribution** (Work distribution, §11.2) in either case.

### 11.2. Work distribution (ADR-018)

A separate JetStream stream `debian-work` with **work-queue retention**,
subject `debian.work.deep_dependencies`, one durable pull consumer
`deepdeps` shared by all workers.

Both `deps-enqueue` and the workers call R1's `ensure_stream`
(idempotent create-if-missing) with the `debian-work` config;
there is no separate bootstrap.

Message body (JSON): `{job_id, distribution, release, architecture,
package, version, max_level}` (`max_level` = integer or `null`).
`job_id` = lowercase hex SHA-256 of the UTF-8 string
`d|r|a|package|version|max_level` joined with the ASCII unit separator
`\x1f` instead of `|`, with `max_level` rendered as decimal where 0
means no max level (the whole network).
Go and Python must produce the same id (contract test).
`job_id` is also sent as `Nats-Msg-Id`
server dedup only covers the stream's duplicate window (2 min default),
so **idempotent enqueueing is guaranteed by `deps-enqueue` itself**: a
job whose output file already exists is skipped (unless `--force`).

Consumer settings: `ack_wait` 60 s; workers send an in-progress ack
(`+WPI`) every 15 s while computing; `max_deliver` 5; a job that
exhausts deliveries is logged and counted in `jobs_failed_total`.
`max_ack_pending` bounds in-flight jobs = backpressure
(set to the total worker concurrency).

Worker loop:

 1. pull a job,
 2. compute the closure,
 3. write the output file atomically,
 4. append the execution record,
 5. ack.

Crash before ack → redelivery → same deterministic output path →
idempotent overwrite (ADR-019).

Commands:

 - `debian-observer-deps-enqueue --coordinate d/r/a --sample S
   [--max-level N] [--force]`, `S` ∈ `all`, `top-indegree:N`,
   `top-popcon:N`, `stratified:N`. Per-package in-degree comes from
   P3 (distinct depender packages per `(target_package,
   target_version)`). `stratified:N`: sort roots by
   `(in_degree, package, version)` ascending, cut into 10 equal-count
   bins (deciles), take `ceil(N/10)` roots per bin at evenly spaced
   positions inside the bin; deterministic, no randomness. Prints
   the number of enqueued and skipped jobs.
 - `debian-observer-deps-worker --concurrency N` (Python): one parent
   process starting `N` child processes (`multiprocessing`), each a
   pull-consumer loop; exits when the queue is empty for 5 s if
   `--drain` is given (benchmark mode), else runs until signalled.
 - `deepdeps-worker --concurrency N [--drain]` (Go): one process,
   `N` goroutines, same semantics and flags.

Workers can run on any homelab machine reachable to NATS and to the
derived data (shared directory; mechanism is a deployment detail,
documented in the user guide).


---
## 12. CLI & API

CLI additions:

  - the `--eventizer` option and coordinate filters (Normalization CLI, §9.1),
  - the prosumer commands (The prosumers, §10.2),
  - `deps-enqueue` (Work distribution, §11.2),
  - the `benchmark` extension (Benchmarked Performance, §16).

### 12.1. API extensibility (plug-in endpoints)

The API is **read-only and small**, but it must not be a closed list:
analytical plug-ins like DOPI-PDDA need to serve their own tables
without the platform shipping a router per plug-in
(ADR-022). The platform therefore owns the **mechanism**, the
plug-in owns the **endpoints**:

 1. **The platform never imports a plug-in.** A plug-in's router is
    discovered by entry point (`debian_observer.api_routers` group):
    each installed plug-in exports a FastAPI `APIRouter`, the
    platform's app mounts every discovered router at startup. A
    plug-in that is not installed adds nothing and breaks nothing,
    and the platform has no dependency on any plug-in — the
    dependency arrow stays plug-in → platform, never the reverse
    (the platform does not even know the plug-in's import name:
    it receives the router object, not a reference to import).
 2. **A standard FastAPI router, not a record format.** The plug-in
    declares its routes the way any FastAPI application does
    (`@router.get(...)`, path templates, dependencies, response
    models). There is no intermediate declaration format and no
    lazy handler strings: the entry point is a direct reference to
    a module that builds the router.
 3. **The platform mounts and wraps; the plug-in computes.** The
    platform mounts every discovered router under the shared
    coordinate prefix (rule 4), echoes the component list in the
    response (the rule of this §12 applies to plug-in endpoints
    unchanged: a result can never be read as belonging to another
    list), and applies its observability perimeter (spans/metrics,
    Observability, §13) around every handler.
 4. **The path vocabulary is shared, the resources are the plug-in's.**
    `/coordinates/{d}/{r}/{a}` is the platform's resource prefix for
    anything measured per coordinate; what follows it belongs to
    the declaring plug-in. Two plug-ins must not collide: a
    declaration whose path template is already mounted **fails fast
    at startup** with both declaring packages in the error — the
    platform does not silently shadow (deliberately kept: it
    prevents mistakes, ADR-022).

The plugin-developer documentation (router shape, shared prefix,
component-list echo, collision error) is
`dopi-pdda.git/docs/plugin-api-endpoints.md`; the platform's
`docs/design/module-contracts.md` keeps only the mechanism's
contract (entry-point group, mounting rules, collision error).

API (FastAPI, read-only, small): only endpoints backed by a demo use
case:

```text
GET /coordinates/{d}/{r}/{a}/dependency-statistics
GET /coordinates/{d}/{r}/{a}/packages/{package}/dependencies      # direct, up
GET /coordinates/{d}/{r}/{a}/packages/{package}/rdependencies     # direct, down
GET /coordinates/{d}/{r}/{a}/cycles
GET /coordinates/{d}/{r}/{a}/component-frontier                   # Coordinates, §4.3
```

Every endpoint takes `?components=main+non-free-firmware` (default `all`,
Coordinates, §4.3) and echoes the component list in the response, so a
result can never be read as belonging to another list.
`/component-frontier` reports, per component, the edges that leave the
component list (`unresolved_outside_components`) and the
cross-component edges inside it — the free-software-boundary question
as an endpoint.

The endpoints above are **DOPI-PDDA's**, declared through the
mechanism of API extensibility (§12.1, ADR-022), not platform
hard-coded routes; the platform's own API surface stays R1's.

**Deep dependencies and the cache budget.** There is **no disk space
to cache every closure**, so the API cannot assume a hit. Measured
ground: one coordinate has ~70 k packages (Volume, §2.1) and 37
release×architecture coordinates give ~2.6 M possible closures per
component list (Coordinates, §4.3) — caching all of them is not
on the table on this hardware, which is precisely what E3 Cache
sizing (E3 Cache sizing, §16.4) is for: it measures the cache curve and ADR-020 sets
the N and the ordering that fit an explicit storage budget.

Therefore, in R2:

 - deep-dependency closures are **not** served by the API, and the API
   never implies a complete cache;
 - the cached subset is whatever ADR-020's budget allows (a
   `stratified`/`top-indegree`/`top-popcon` sample, Work distribution, §11.2), and the
   cache is explicitly **partial**;
 - a compute-on-miss endpoint is deferred (Future Work, §23): it would need a job
   queue behind a request, which is R3's territory;
 - the CLI (`do-deps-deep`) remains the way to get an
   uncached closure, and it reports whether it read the cache or
   computed.


---
## 13. Observability

As R1 (ADR-008): OpenTelemetry traces and metrics. New:

 - spans: `eventize`, `prosumer.<name>`, `worker.job`;
 - metrics: `dependency_events_total`, `invalid_clauses_total`,
   `unresolved_edges_total`, `jobs_processed_total`,
   `jobs_failed_total`, `job_duration_seconds`, `worker_rss_bytes`;
 - the Go worker uses the OpenTelemetry Go SDK with the same metric
   names and, like R1, a stdout/console exporter by default (OTLP
   endpoint optional by environment variable);
 - `worker_rss_bytes` is the sampled `VmRSS` of Benchmark machinery (§16.1).

No full observability platform (R3).


---
## 14. Deployment

 - Docker Compose (dev) and OpenTofu (deploy) as R1; add the
   `debian-work` stream to the stream bootstrap (`ensure_stream`).
 - DOPI-PDDA has its own deployment.
   - The Go worker is a static binary (`CGO_ENABLED=0`) — deployable on
     the old 4 GB laptops without a Python environment; this is part of
     the Go evaluation (operational argument, not only speed).
   - Its devcontainer gains the Go toolchain
   - CI (Woodpecker?) gains a Go build/test step.
   - Resource budgets per service recorded (R1 §14.4), now including
     per-worker RSS.


---
## 15. Reliability Properties

 1. **Idempotent eventization/folds**: re-running eventization and the
    prosumers on the same artifact yields identical tables
    (order-insensitive, random fields of Identity and idempotency, §6.2 excluded).
 2. **Worker crash**: `kill -9` of a worker mid-queue → jobs
    redelivered → final cache identical to an uninterrupted run
    (extension of R1's `scripts/crash-test-consumer.sh` pattern).
 3. **Malformed relationship fields**: become invalid records with
    cause; counts are reported.
 4. **Cycles** never make a traversal loop forever (visited set;
    tested with synthetic cyclic fixtures).
 5. **Equivalence**: Python and Go deep-dependency outputs are equal
    for the same job.


---
## 16. Benchmarked Performance (extension of R1 §16)

**Analytics and benchmarking are different things and never share a
table.**
Analytical Storage (§10) answers questions about **Debian**:
how the dependency graph is shaped, how it changes across releases,
where the component frontiers are.

This section answers questions about **our product**:
how long it takes, how much memory it uses, how it scales.

Domain figures live in `data/derived/`; product figures live
in the `ExecutionStore` and in `benchmarks/results/`.

A benchmark may *read* a derived table (the cache sizing uses P5's
percentiles), but no derived table ever carries timings, and no
execution record ever carries domain facts.

R1 provides a sketch:
 - the `benchmark` command (synthetic ingestion),
 - `ExecutionRecord` / `ExecutionStore` (`execution_runs.duckdb`) and
 - `execution-report`.

R2 **extends** it to evaluate parallelization and
to dimension the deep-dependency cache. It does not build a new one.

### 16.1. Extensions to the R1 benchmark machinery

 - `ExecutionRecord.execution_type` gains `eventize`, `prosumer`,
   `worker`.
 - New nullable fields: `implementation` (`python` | `go`),
   `workers`, `peak_rss_bytes`, `cpu_seconds`, `coordinate`,
   `work_items`, `output_bytes`, `eventizer`, `prosumer`. Existing
   `execution_runs` tables get the columns added on open (backward
   compatible. R1 rows load with nulls).
   These new columns do not go in R1's free-form `metadata` JSON,
   because `execution-report` must group and aggregate by them.
   `metadata` stays for anything else.
 - The Go worker appends records with the same field names to a JSON
   lines file; `debian-observer benchmark import-records FILE` loads
   them into the `ExecutionStore` (idempotent by `run_id`, as R1).
   The JSON keys are exactly the `ExecutionRecord` field names
   (timestamps ISO 8601 UTC).
   `ExecutionRecord` has `extra="forbid"`, so an unknown key is an
   import error, reported with the line number.
 - `benchmark` becomes a command group, and the new subcommand is
   **generic, not graph-specific**: `benchmark` is an operator's
   tool for measuring *any* run, so naming a subcommand after one
   domain (`graph`) would build the wrong perimeter. R2 therefore adds
   `benchmark run`, which measures an **arbitrary command** and files
   the result as an `ExecutionRecord`:
   - `benchmark ingest` — R1 behaviour, unchanged;
   - `benchmark run` — run a command N times under measurement
     (duration, peak RSS, CPU seconds, output bytes, exit code) and
     record it with the caller-supplied `--execution-type`,
     `--implementation`, `--workers`, `--coordinate` labels;
   - `benchmark import-records FILE` — load the Go worker's JSON lines.

   The group is created with `invoke_without_command=True` and keeps
   R1's `--records`/`--raw-dir` options, so a bare `benchmark
   [--records N]` still runs the R1 ingestion benchmark.

   The R2 experiments (E1–E6) are then **invocations** of
   `benchmark run`, scripted under `benchmarks/`, not subcommands of
   the CLI. That keeps the graph vocabulary out of the generic tool
   and makes the same tool reusable in R3.

The deep-dependency experiment driver is a script
(`benchmarks/e4-parallel-scaling.sh`) that calls `benchmark run`, per
`(impl, workers, coordinate, repeat)`:

```text
benchmarks/e4-parallel-scaling.sh
    --impl python|go
    --workers 1,2,4[,8]
    --coordinates debian/trixie/amd64[,...] | --all-coordinates
    --sample all | top-indegree:N | top-popcon:N | stratified:N
    --repeat 3
```

Each measured iteration:
 1. enqueue the job set (`deps-enqueue --force`, deterministic),
 2. start the worker (`debian-observer-deps-worker` or `deepdeps-worker`) with
    `--concurrency workers --drain`,
 3. wait for it to exit,
 4. record wall time, jobs done, failed jobs, output bytes and peak RSS.
    Peak RSS is sampled every 0.5 s from `/proc/<pid>/status` (`VmRSS`)
    summed over the worker process tree (no new dependency).
    The maximum of the sum is recorded, plus the per-process maximum.
    For the Python arm the sum is the number that matters: it shows
    whether `fork()` copy-on-write holds under load (Memory, §2.2).

 - `execution-report` gains grouping by `implementation, workers,
   coordinate`.

### 16.2. E1 — Eventization

Full `packages` table, no filters, `--eventizer dependencies`:
duration, events/s, peak RSS, invalid clause count.

Peak RSS is reported at 10 %, 50 % and 100 % of the table to confirm
it stays flat (expected ~267 MB throughout, Memory, §2.2). This is a
regression guard, not an experiment: if it ever grows with the number
of events, something started accumulating.

The experiment that matters for volume is the **prosumer read**
(E2 Direct tables, §16.3), which contrasts the batched read against
R1's materializing one on the same lake.

### 16.3. E2 — Direct tables, sequential and parallel

The researcher works cross-coordinate (Layout, §10.1).

**The read-path arm comes first** — it is the release's volume
question (Memory, §2.2). One prosumer over the full event lake, read
in batches: peak RSS and duration recorded. R1's materializing path is
measured on a reduced lake and extrapolated, since 19.3 GB does not
run on this hardware; the point is to publish *why* the batched read
is mandatory, not to pretend the other is an option.

Then the scaling arm: P1–P5 over the coordinates — duration, peak RSS
and output bytes per coordinate, then all 37 release×architecture
coordinates **sequentially vs in parallel** (one process per
coordinate, N workers), for the all-components list and again for `main`
(Coordinates, §4.3). The `main` graphs are smaller (69,251 of 70,341
rows in trixie amd64+all), so the pair also measures what the
component list costs. Reported as the
cross-coordinate totals a researcher actually waits for (full-table
rebuild wall time), with the per-coordinate figures as the breakdown.
P4 is measured on the unfolded graph (The prosumers, §10.2 P4),
so its cost is reported separately from P2/P3 — it is a different graph.

### 16.4. E3 — Deep dependencies and cache sizing

For a sample of roots (`stratified`, from P5): time per closure, rows
and bytes per closure, per direction.

Outputs:

 - the distribution of closure size vs in-degree percentile, and how
   well P5's percentiles predict it (estimation error measured);
 - **cache curve**: cumulative storage vs number of cached roots when
   roots are ordered by in-degree and by popcon;
 - recompute time vs read time per closure;
 - extrapolation to all roots of all coordinates (with its error
   bars from the sample);
 - a recommendation (ADR-020): the N (and ordering) to precompute
   under explicit storage budgets (e.g. 5 GB, 20 GB), and whether
   caching everything is feasible at all on the homelab.

### 16.5. E4 — Parallel scaling (the Go question)

Same job set (deep closures of one coordinate, sample `stratified`
and, if feasible, `all`), on ghostell:

| Arm                                 | Workers    |
|:------------------------------------|------------|
| Python, multiprocessing (processes) | 1, 2, 4    |
| Go, goroutines in one process       | 1, 2, 4, 8 |

Record:
 - throughput (jobs/s),
 - speed-up vs 1 worker,
 - **peak RSS total and per worker**,
 - CPU utilization.

Decision frame (ADR-017):
Go becomes the default worker if it is faster at equal workers **or**
fits more workers into the same memory budget. Otherwise Python stays
default and the ADR records why. Either result is publishable
evidence.

### 16.6. E5 — Distribution (optional within the 10 days)

Go workers on ghostell + pavilion/pb, pulling from the same work
queue: throughput vs single machine, and the overhead of the queue.

### 16.7. E6 — Recovery

Worker killed with K jobs pending:
 - recovery time,
 - redelivered jobs,
 - final equality of the cache (Reliability Properties, §15, item 2).

All numbers are obtained experimentally and committed to
`benchmarks/results/` with the command that produced them. None are
invented.


---
## 17. Security & Supply Chain

 - New Go dependencies pinned in `go.sum`.
   Go module licenses checked for compatibility
   (REUSE, SBOM regenerated per CONTRIBUTING).
 - Python dependencies: R2 declares **one**, `python-debian`, for
   Debian version comparison (Clause resolution, §5.1.2). It is
   already present in R1's environment as a transitive dependency and
   appears in R1's SBOM (`pkg:pypi/python-debian@1.1.1`, verified
   2026-11-04); R2 promotes it to an explicit entry in
   `pyproject.toml`, which is the honest way to depend on it. No other
   additions — the graph algorithms stay in the library, no networkx.
 - Worker image/binary scanned like R1 images.
 - No secrets (NATS without credentials on the homelab network, as R1).


---
## 18. Definition of Done

### Functional
 - [ ] **R1 fix (T0)**: `normalize` writes an `ExecutionRecord`;
       `ExecutionType` gains `normalization`
       (Reading the dump and writing events, §9.2).
 - [ ] Prosumers read the event lake **in batches**, never
       materializing it: measured peak RSS of a full-lake prosumer run
       stays within the Memory (§2.2) figure, and no graph prosumer
       calls `read_table`/`to_pandas`/`list(source)` on the log.
 - [ ] `normalize udddump` peak RSS measured on the full table and
       confirmed flat in the number of events.
 - [ ] Derived tables in the flat layout of Layout (§10.1): no
       `hive_partitioning` anywhere in the code, one file per
       `(coordinate, components)`.
 - [ ] Component lists (Coordinates, §4.3): the full matrix of 37
       release×architecture coordinates × the studied component lists
       is built; `--components` honoured by every graph prosumer as a
       comma-separated list; `unresolved_reason` populated.
 - [ ] Component-frontier result reported: `main`'s self-containment
       measured from our own tables, with the rescued-by-alternative
       count (the false-frontier check of Coordinates, §4.3).
 - [ ] Every event-log read passes `union_by_name=true` (Normalization CLI, §9.1).
 - [ ] `--eventizer` with `package-versions` (R1-compatible), `dependencies`, `popcon`.
 - [ ] `DependencyObserved` / `PopconObserved` contracts with schema version 1.
 - [ ] 5 prosumer tables (P1–P5) generated from real data for at least trixie/amd64 and one more release.
 - [ ] Work queue + Go worker producing P5 files equal to the Python ones.
 - [ ] Cross-release comparison queries in `benchmarks/queries/`.
 - [ ] API endpoints of CLI & API (§12).

### Reliability
 - [ ] Idempotent re-run (tables identical).
 - [ ] Worker crash/redelivery test.
 - [ ] Malformed clause test.
 - [ ] Cyclic graph traversal test.
 - [ ] P4 reports `hard` and `potential` cycles from the unfolded
       graph (The prosumers, §10.2 P4).
 - [ ] Python workers share the graph: measured per-child
       `Private_Dirty` stays at the order of magnitude reported in
       Memory (§2.2), i.e. no per-worker copy.

### Observability
 - [ ] New spans/metrics emitted (Python and Go).

### Performance
 - [ ] E1–E4 and E6 measured and committed; E5 if time allowed.
 - [ ] ADR-017 and ADR-020 closed with evidence.

### Reproducibility
 - [ ] From the R1 raw artifact, a clean environment regenerates all R2 tables.
 - [ ] Devcontainer with Go toolchain; CI builds and tests Go.

### Portfolio
 - [ ] Architecture diagram updated.
 - [ ] ADR-013..022.
 - [ ] Benchmark results + cache recommendation.
 - [ ] Demo script.
 - [ ] R3 handoff section.


---
## 19. Testing Strategy

Per CONTRIBUTING (TDD, Gherkin-like where it fits).

### 19.1. Unit
 - relationship parser: simple, versioned, `:any`, alternatives,
   whitespace variants, empty/null fields, malformed input.
 - Tarjan and BFS on synthetic graphs (DAG, self-loop, 2-cycle,
   nested cycles, disconnected).
 - P1 field rebuild: clause order, field with only complex clauses,
   field with no complex clause (no row), `depends` and `pre-depends`
   of the same package as separate rows.
 - target resolution: `all` packages, several target versions,
   unresolved (virtual) targets, and each `unresolved_reason`
   (`outside-components` / `virtual` / `absent`) hit on purpose.
 - component lists: `*/debian-installer` normalizes to its parent
   component; the same events yield a smaller self-contained graph
   under `main` than under `all`; a clause whose first alternative is
   outside the component list but whose second is inside resolves
   **inside**
   (the 72 rescued clauses of Coordinates, §4.3 — this is the regression test for
   the false-frontier bug).
 - file-name encoding/decoding of versions with epochs;
 - `ExecutionRecord` backward compatibility (R1 rows load).

### 19.2. Integration
 - small real fixture (a few hundred `packages` rows extracted from
   the dump, committed with provenance and license note per
   CONTRIBUTING/REUSE) through eventize → P1..P6;
 - work queue with NATS (as R1 integration tests).

### 19.3. Validation against reality
 - P3 reverse dependencies of a few packages compared with
   `testing_autoremovals.rdeps` / known `reverse-depends` output on
   the same snapshot (documented differences are acceptable: virtual
   packages, alternatives);
 - P4 cycles on a sample checked against known Debian cycles
   (manual, recorded in the results).

### 19.4. Contract
 - event payload schemas; derived table column sets and sort orders;
   Python/Go P6 equality;
 - **event-log reader tolerates column-order drift**: two files with
   the same columns in different order are read correctly (the
   `union_by_name=true` contract of Normalization CLI, §9.1 — without it DuckDB raises
   `schema mismatch in glob`);
 - **derived files are self-describing**: every derived Parquet file
   contains its own `distribution`, `release`, `architecture` columns
   (Layout, §10.1), asserted by reading a file in isolation.

### 19.5. Failure
 - kill worker, malformed clause, missing coordinate file, NATS restart.


---
## 20. Demonstration Scenario

 1. `normalize udddump … --table packages --eventizer dependencies`
    (show events/s, peak RSS, invalid count).
 2. Build P1–P5 for trixie/amd64; show statistics.
 3. Query: top-10 packages by direct reverse dependencies; the
    cycles found.
 4. Cross-release: statistics and libc6 reverse reach for bullseye →
    bookworm → trixie → forky (one SQL file).
 4b. **Across component lists**: build trixie/amd64 under `main` and
    under
    `all`; show that `main` needs nothing outside itself (measured 0
    clauses, Coordinates, §4.3), that 72 clauses only stay inside `main` thanks to
    a non-first alternative, and that the two apparent
    `main`→non-`main` edges vanish once alternatives are unfolded —
    Policy §2.2.1 verified from data, and the cost of
    `main+non-free-firmware` for a machine that needs firmware.
 5. Enqueue deep jobs (stratified sample); run Python ×4 and Go ×4;
    show throughput and RSS side by side.
 6. Kill a Go worker mid-queue; restart; cache equality check.
 7. Show the cache curve and the recommended N.
 8. Close with the architecture and what R3 will operate.


---
## 21. Ten-Day Execution Plan

Each day ends with a buildable, tested state (CONTRIBUTING).

### Day 1 — Contracts & decisions
ADR-013..016, 018, 019 written; ADR-017/020 decision frames; design
docs (data model, module contracts, key flows) updated, landing as
the docs of the corresponding [R] commits. Success: a reviewer can
implement P1–P6 from the docs alone.

### Day 2 — Dump reading & eventizer registry
COPY iterator; eventizer registry; R1 behaviour moved to the
`package-versions` eventizer with unchanged output; coordinate
filters. Success: R1 tests green; peak RSS of a full-table run
measured.

### Day 3 — Dependency & popcon eventizers
Parser, `DependencyObserved`, `PopconObserved`, shape gates, `--help`
perimeter. Success: full `packages` table eventized on ghostell;
counts match Volume (§2.1) measurements for trixie amd64+all.

### Day 4 — P1, P2, P3
Complex, straight, reverse tables, partitioned. Success: trixie/amd64
tables; spot checks against UDD.

### Day 5 — P4, P5
Cycles (Tarjan on the **unfolded** graph, `hard` + `potential`
classes, The prosumers, §10.2 P4) and statistics. P4 reads the event log and P1, not
P2. Success: all 37 release×architecture coordinates computed
sequentially in the all-components list, trixie/amd64 also under `main`;
per-coordinate execution records; a sample of `hard` cycles listed for
manual validation (Validation against reality, §19.3).

### Day 6 — P6 (Python) & work queue
Deep dependencies, `deps-enqueue`, Python worker over JetStream.
Success: stratified sample of trixie/amd64 cached; crash test passes.

### Day 7 — Benchmark extension & Python baseline
`ExecutionRecord` extension, `benchmark run`, `import-records`,
E1–E3, E4 Python arm. Success: baseline numbers committed; cache
curve draft.

### Day 8 — Go worker
Go module, Parquet read of P2/P3, BFS, atomic Parquet write,
JetStream pull, OpenTelemetry, execution records. Success: Go output
equals Python output on the sample.

### Day 9 — Comparative benchmark & reliability
E4 Go arm, E6, (E5 if feasible). Close ADR-017 and ADR-020 with
evidence.

### Day 10 — Hardening & portfolio
Cross-release queries, API endpoints, user guide, architecture
diagram, demo script, limitations, R3 handoff.

Buffer: none. If behind schedule, cut in this order: E5 → API
endpoints → popcon weighting in P6 (keep the column null) → `all`
sample in E4. Never cut: E4 (the Go question), idempotency/crash
tests, documented limitations.


---
## 22. Stretch Goals (only if time remains)

Same priority level; pick at most one.

### 22.1. Migration: RC-bug blast radius (preferred)

Domain question: *how many packages leave testing because of a
release-critical bug in one of their dependencies, and is UDD's own
`rdeps` consistent with our reverse closure?* Source:
`testing_autoremovals` (eventizer `autoremovals`). Output: per bug /
source package, the autoremoval set vs P3/P6 reach, weighted by
`rdeps_popcon`. Adds the first link between dependency structure and
release dynamics (R2's title).

### 22.2. Prosumer registry

Developers register prosumers (linking each to the event/source types
it consumes, with a short description and documentation pointer);
researchers list them (`debian-observer prosumers list`).

### 22.3. Build-dependencies (deferred, not R2)

`sources.build_depends*` would reuse the eventizer and graph
machinery, but build-dependency cycles are botch's territory; only a
temporal angle (growth of rebuild reach across releases) would add
value. Recorded as future work.


---
## 23. Future Work

`Provides` resolution, version satisfaction (Debian version
comparison), multiple dump dates (true time series), ingestion of
dose3/botch outputs as a source, compute-on-miss API for deep tables,
graph database re-evaluation (ADR-016 revisit conditions), Kubernetes
operation of the workers (R3).


---
## 24. Open Decisions for the Owner

Decisions this document does **not** take, because they are the
owner's. Each one has a default so the implementer is never blocked,
but the default is marked as such and can be overruled.

### D1 — Publish R2 observation events to JetStream?

See Events (§11.1) for the constraint and the options.

 - **At stake**: the program-level capability of P2 (s2 §4.2),
   "multiple specialized consumers over NATS". Without publishing,
   R2's distributed-systems story rests only on the work queue
   (Work distribution, §11.2).
 - **Cost**: ~8.4 M events ≈ **3.4 GB** of JetStream file storage for
   the full dump, on an 8 GB server that also runs Postgres and the
   derived tables. R1's current `debian-events` limits
   (`max_msgs=100_000`, `max_bytes=1 GB`) would evict ~99 % of it
   silently.
 - **Options**: (a) publish a *filtered* subset (one release, or
   `is_complex` only) inside the current limits; (b) a dedicated
   `debian-observations` stream sized for the full set (+3.4 GB disk);
   (c) do not publish in R2 and record it as an R3 item.
 - **Default if undecided**: (a) — publish one coordinate
   (trixie/amd64) so the consumer path is exercised and demonstrable,
   without the disk bill. Definition of Done (§18) lists it as functional, not as a
   full-volume guarantee.

### D4 — Which component lists does R2 study?

**Resolved: R2 builds the full matrix** (37 release×architecture
coordinates × the studied component lists, Coordinates, §4.3). The
earlier proposal to cover only part of it had no evidence behind it: a
`main`-only graph is *smaller* than the all-components one, so the
extra lists cost a fraction of a pass each, and E2 Direct tables (§16.3) measures that cost rather than assuming it.

What remains open is only **which lists are worth reporting**, which
is an editorial choice about the demonstration, not a compute limit:

 - `main` and all-components are both needed — they are the two ends
   of the free-software question.
 - `main,non-free-firmware` is the list that matches a real machine
   needing firmware, so it is arguably the most *useful*, though it is
   the least different from `main` (44 extra rows in trixie amd64+all).
 - **Default if undecided**: report all three; add others on request,
   since the component list is a run parameter and not a hard-coded
   set.

### D3 — Go toolchain install (unchanged from the brief)

Not installed anywhere yet; adding it to the devcontainer and CI is a
dependency to flag before T8, not a silent step.

 - **Default if undecided**: ask before installing on any host; T8
   does not begin until the version and location are approved.

### Settled (recorded so they are not re-opened)

|  # | Question | Resolution |
|---:|:---------|:-----------|
|  1 | Does normalization (UDD read + event write) need streaming or parallelism? | **No** to both. The read is ~1.3 GB and infrequent; eventization is flat at ~267 MB for 8.4 M events. The streaming requirement belongs to the **prosumer read of the lake** (19.3 GB vs 0.73 GB), not to normalization (Memory, §2.2). |
|  2 | Do workers each copy the graph?                     | No. `fork()`+CoW shares it; measured 0.9 MB/child with CSR, so ADR-017 does not rest on memory (Memory, §2.2). |
|  3 | Does `normalize udddump` select the eventizer?      | No — it selects the source; `--eventizer` selects the eventizer (In scope, §3.2.1). |
|  4 | Is `clause_index` needed if clauses are unordered?  | Yes, as a *uniqueness discriminator*, not an order: dropping it collides 3,142 rows in trixie amd64+all (Dependency clause, §5.1.1, Ordering, §6.3). |
|  5 | How is the event log laid out?                      | One **directory per event type**, one file per run inside it (owner, 2026-11-04): a Parquet file is one table, so there is no reason to union different types and pay NULL padding for it. Per-type schemas are mandatory either way (Normalization CLI, §9.1). |
|  6 | Fix the missing `normalize` execution record in R1? | Yes — R1 bug, see D2 for sequencing (Reading the dump and writing events, §9.2). |
|  7 | Hive partitioning for derived tables?               | No, and no disguised equivalent (no coordinate subdirectories either). One directory per table, coordinate in the **columns**; file names are flat and are not a query interface (Layout, §10.1, ADR-016). |
|  8 | Is `component` part of the coordinate?              | **Yes** (owner, 2026-11-03/04), as a **list**, treated exactly like `architecture` — both are multi-valued selectors. A coordinate is `(distribution, release, architecture, components)` (Coordinates, §4.3, ADR-021). Measured: `main` needs **0** packages outside itself, 72 clauses stay inside only via a non-first alternative, and the 2 apparent `main`→non-`main` edges are artefacts of first-alternative simplification. |
| 10 | Are version constraints evaluated?                  | **Yes** (owner, 2026-11-04). 57 % of clauses carry one (178,673 of 313,761), so ignoring them would discard the majority of the information. Prosumers filter the satisfying versions, count them and may pick one by a stated rule; `python-debian`'s `version_compare` does the comparison and is promoted from transitive to declared dependency (Clause resolution, §5.1.2). |
| 11 | Are AND and OR dependencies distinguished?          | **Yes** (owner, 2026-11-04). A field is a conjunction of disjunctions; edges carry `edge_class` ∈ {`required`, `alternative`} and alternatives of one clause share a `clause_index`, so a consumer asks "is the clause satisfied?" rather than "does the edge exist?" (Clause resolution, §5.1.2). |
| 12 | Does the event log union different event types?     | No. One table per event type, one directory each, plain reads with no `union_by_name` and no NULL padding (Normalization CLI, §9.1). |
| 13 | Do prosumers get new I/O options?                   | Maybe, but R1's `--file-in/--file-out/--stream-in/--stream-out` are the baseline contract (verified in `prosumers/cli.py` and the user guide); `--file-in`/`--file-out` merely become repeatable and accept directories/globs (The prosumers, §10.2). |
| 14 | Does R2 build only part of the coordinate matrix?   | No — the **full** matrix (D4). The earlier 74-of-111 cut had no measurement behind it. |
|  9 | Can P4 find cycles from P2?                         | No. P2 keeps only first alternatives; P4 unfolds all of them and reports `hard` vs `potential` (The prosumers, §10.2 P4). |
