# Debian Release Observatory. Sub-Project for Release R1.

> A reproducible, event-driven observatory for studying the evolution of Debian releases from public historical data.


## Table of Contents

Overview
 1. Main **Goals**: Purpose, Product purpose, release goal, main technical features.
 2. Technical **Challenges**: Heterogenity and resource restrictions.
 3. Engineering **Analysis**: Patterns and release scope.

Data Models
 4. Data Sources & Datasets
 5. Data Model: Domain and metadata
 6. Event Model: Why?, envelope, identity, schema versioning, ordering

Automated Process
 7. Architecture
 8. Ingestion
 9. Normalization
 10. Analytical Storage
 11. Messaging
 12. Interaction Interfaces: API & CLI
 13. Observability

Product Quality Specifications
 14. IaC Deployment: scalable, reproducible/consistent, modular/isolated, dependency-managed
 15. Reliability Properties
 16. Benchmarked Performance
 17. Security & Supply Chain
 
Product Quality Checks
 18. Definition of Done
 19. Testing Strategy
 20. Demonstration Scenario

Development Plan
 21. Three-Week Execution Plan


---
## 1. Main Goals

### 1.1. (Business) Domain and Product Purpose

Debian is a large, continuously evolving software ecosystem composed of
tens of thousands of packages, dependencies, uploads, builds, bugs, migrations, and release constraints.

Understanding its evolution requires

 - combining data from multiple sources and
 - observing how those facts change over time.

The initial analytical question is:

> How does the state of Debian packages and releases evolve over time, and
> can we construct a reliable historical representation that
> later allows us to explain dependency and migration dynamics?


### 1.2. Release Scope

This release (R1) does not attempt to answer the complete dependency-analysis problem.
That is a task (in fact, the main focus) of the next release (R2).

This release (R1) builds a small, reproducible data and event-processing platform capable of

 1. ingesting public Debian data,
 2. normalizing it into a canonical model,
 3. publishing domain events,
 4. and exposing the resulting historical dataset for analytical queries.

The release (R1) is deliberately designed as the foundation for a multi-stage program of releases.
This release must therefore be useful by itself while
establishing interfaces and engineering practices that the next release (R2) can extend.


### 1.3. Technical Product Features of Release 1

The primary goal is to test and demonstrate the ability to
design and operate a small production-style data platform.

A researcher should be able to deploy on a resource-restricted home lab
for POC validation ("Considering a tiny sample of the data, will I be able to answer my questions?"),
and later deploy to the cloud for reliable research over large datasets.

These needs result into a set of technical features:

 - real public data,
 - asynchronous processing,
 - durable events,
 - explicit data contracts,
 - idempotent consumers,
 - provenance,
 - reproducible processing,
 - analytical storage,
 - API access,
 - observability,
 - failure recovery.


---
## 2. Technical Challenges

### 2.1 Data Reproducibility

Debian release data is distributed across different datasets and representations.

A useful historical analysis requires more than downloading a table.

The resulting data and matadata should allow later releases to
reason about historical state
without repeatedly rebuilding the entire ingestion pipeline.


### 2.2. Restrictions

The main engineering constraints are:

#### 2.2.1. Restricted Computing Power for Development and Testing

The system must process potentially large datasets
on machines with limited resources.

 - 8 GB RAM on the development machine,
 - 8 GB RAM on the primary server,
 - additional 4 GB machines,
 - no requirement for permanent cloud infrastructure,

#### 2.2.2 Time Constraint for the Release

The project must produce a credible demonstrator within approximately **two weeks**.

The implementation should therefore prioritize:

 1. a complete vertical slice,
 2. reproducibility,
 3. failure recovery,
 4. measurable performance,
 5. portfolio-quality documentation.

A smaller complete system is preferable to a larger incomplete platform.

#### 2.2.3 Preferences / Policy

 - reproducible deployment,
 - public data rather than synthetic domain data,
 - open-source tooling where technically appropriate.


---
## 3. Engineering Analysis:

### 3.1 Standard Approaches.

This problem case represents a broader class of known engineering systems:

 - software supply chains,
 - package repositories,
 - artifact pipelines,
 - release engineering,
 - dependency management,
 - build systems,
 - CI/CD platforms,
 - infrastructure inventories.

The technical patterns are therefore transferable beyond this case.
And the architecture therefore favors standard approaches:

 - immutable files,
 - columnar storage,
 - streaming/event-based processing,
 - lightweight analytical queries,
 - low operational overhead.


### 3.2 Release (R1) Scope

### 3.2.1. In scope

This release will provide:

1. public Debian data acquisition,
2. raw data preservation,
3. data provenance,
4. canonical domain normalization,
5. event publication,
6. durable event storage,
7. idempotent event consumers,
8. Parquet-based analytical storage,
9. Analytical queries (DuckDB),
10. a small API service (FastAPI),
11. a CLI for operational workflows,
12. telemetry instrumentation (Opentelemetry),
13. reproducible local deployment,
14. failure/recovery tests,
15. basic performance measurements,
16. documentation and ADRs.

### 3.2.2 Known Limitations

R1 will have limitations such as:

 - incomplete historical coverage,
 - differences between source datasets,
 - limited real-time capability,
 - local storage constraints,
 - limited concurrency,
 - simplified migration semantics,
 - absence of a full distributed deployment,
 - absence of a production-scale observability platform.

These limitations should be documented rather than hidden.

A limitation is acceptable when:

 1. it is understood,
 2. it is measured where possible,
 3. it is consciously scoped,
 4. the next project has a reason to address it.

### 3.2.3. Relationship with the Next release

This release deliberately creates the foundation required by the next one.

#### R3 consumes

 - normalized package data,
 - package versions,
 - release information,
 - migration events,
 - event schemas,
 - provenance,
 - historical datasets.

#### R3 adds

 - dependency graph construction,
 - reverse dependency analysis,
 - migration dynamics,
 - build relationships,
 - distributed workers,
 - concurrency,
 - event replay at larger scale,
 - dependency blast-radius analysis.

#### R1 must not become R2

R1 should not prematurely implement:

 - a complete dependency graph,
 - distributed graph computation,
 - multiple specialized workers,
 - Kubernetes,
 - complex stateful services.

The purpose is to establish reliable foundations.

### 3.2.4. Out of scope

The following features are explicitly deferred:

 - complete Debian dependency graph analysis,
 - reverse dependency analysis,
 - build failure correlation,
 - migration bottleneck detection,
 - reproducibility analysis (coverage and gaps),
 - supply-chain analysis,
 - historical pattern detection,
 - AI-assisted investigation.
 - large-scale distributed graph processing,
 - full observability (Grafana/Prometheus/Loki/Tempo),
 - complex frontend applications,
 - real-time monitoring of Debian infrastructure,
 - large scale deployments, production-grade multi-node orchestration,
 - GitOps,
 - full SRE platform: automated remediation, SLOs, chaos testing,

These are intentionally outside the R1 Definition of Done,
and may become candidates for future extension releases.


## 3.3. Repository Structure: Components

The repository should contain at least:

```text
project/
├── README.md
├── LICENSE
├── pyproject.toml
├── uv.lock
│
├── src/
│   └── debian_observer/
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── contract/
│
├── schemas/
│   └── events/
│
├── data/
│   └── .gitkeep
│
├── benchmarks/
│
├── docs/
│   ├── CONTRIBUTING.md
│   ├── architecture/
│   ├── adr/
│   └── datasets/
│
├── deploy/
│   ├── compose/
│   └── opentofu/
│
├── .devcontainer/
│
└── scripts/
```

Large external datasets should not be committed directly to Git.

The repository should contain manifests, checksums, acquisition instructions and reproducible processing steps.

### 3.4. Architecture Decision Records

Initial ADRs should include:

#### ADR-001 — Dataset Selection

Why the selected Debian data source is suitable for R1.

#### ADR-002 — Raw Data Preservation

Why raw artifacts are retained and how provenance is maintained.

#### ADR-003 — Parquet and DuckDB

Why analytical data is stored as Parquet and queried with DuckDB.

#### ADR-004 — NATS JetStream

Why durable event streaming is required and why NATS is appropriate for R1.

#### ADR-005 — Event Delivery Semantics

Why the system uses at-least-once delivery and idempotent consumers.

#### ADR-006 — No PostgreSQL in R1

Why a transactional operational database is not required by the current workload.

#### ADR-007 — No Kubernetes in R1

Why container orchestration is deferred until the distributed workload requires it.

#### ADR-008 — Observability Strategy

Why OpenTelemetry is introduced before a complete observability platform.

#### ADR-009 — Resource Constraints

How the architecture responds to the homelab hardware limits.


---
## 4. Data Sources & Datasets

### 4.1. Primary data source

The primary source will be the **Debian Ultimate Debian Database (UDD)**
and other publicly available Debian datasets where necessary.

The exact source and snapshot used for each experiment must be recorded in a dataset manifest.


### 4.2. Additional sources

Depending on availability and project scope, additional public sources may include:

 - Debian archive metadata,
 - Debian snapshot data,
 - migration information,
 - package metadata,
 - build-related information,
 - bug metadata.

Each source must be documented independently.


### 4.3. Source selection criteria

A source is suitable when it provides:

 - public accessibility,
 - sufficient historical depth,
 - stable identifiers,
 - machine-readable data,
 - reasonable acquisition cost,
 - sufficient volume to exercise the architecture.

APIs requiring large numbers of throttled requests
should not be the primary ingestion mechanism
when an equivalent bulk/public dataset exists.


### 4.4. Dataset manifest

Each ingestion run should produce a manifest containing at least:

```yaml
dataset:
  name: ...
  source: ...
  retrieved_at: ...
  source_version: ...
  checksum: ...
  size_bytes: ...
  record_count: ...
  license: ...
```

The manifest becomes part of the provenance chain.


---
## 5. Data Model

### 5.1 Domain Data Model

This project deliberately uses a small canonical model.

#### 5.1.1. (Debian) Package

Represents a Debian source or binary package entity
as required by the selected dataset.

Example attributes:

```text
package_id
name
source_package
section
architecture
```

The exact model will follow the semantics of the source data
rather than inventing unnecessary abstractions.


#### 5.1.2. (Debian) Package Version

Represents an observed version of a package.

```text
package_id
version
distribution
architecture
observed_at
source
```


#### 5.1.3. (Debian) Release

Represents a Debian release or suite relevant to the dataset.

```text
release_id
name
codename
status
```


### 5.1.4. (Debian) Migration

Represents an observed migration-related fact.

Thes release (R1) does not attempt to model the entire Britney state machine.

The canonical representation should contain enough information
to support historical analysis and provide a stable foundation for
the next release (R2).

### 5.2 Metadata model

#### 5.2.1. Need for Metadata

The system must preserve:

 - what was observed,
 - when it was observed,
 - where it came from,
 - which transformation produced the normalized representation,
 - which version of the schema was used.


### 5.2.1. Observation (Metadata)

An observation records the fact that information was obtained from an external source.

```text
observation_id
entity_id
source
observed_at
effective_at
payload_reference
```

The distinction between `observed_at` and `effective_at` is important.

A fact may have happened at one point in time and have been retrieved later.


### 5.2.2. Event

An event represents a normalized domain fact emitted by the pipeline.

Examples:

```text
PackageObserved
PackageVersionObserved
ReleaseObserved
MigrationObserved
```

Events are immutable.


---
## 6. Event Model

### 6.1. Why events?

Event-driven architectures support useful capabilities:

 - decoupling ingestion from processing,
 - durable buffering,
 - replay,
 - independent consumers,
 - failure recovery,
 - future parallel processing,
 - explicit domain contracts.

These capabilities become increasingly important in R2.


### 6.2. Event envelope

A canonical event should contain:

```json
{
  "event_id": "uuid",
  "event_type": "PackageVersionObserved",
  "schema_version": 1,
  "source": "udd",
  "observed_at": "2026-01-01T12:00:00Z",
  "effective_at": "2025-12-31T00:00:00Z",
  "entity_id": "package:example",
  "payload": {}
}
```

The precise schema is part of the project's contract.


### 6.3. Event identity

`event_id` provides event-level identity.

Consumers must be capable of detecting duplicate delivery.

The system must therefore treat:

> **at-least-once delivery + idempotent processing**

as the baseline reliability model.

Exactly-once processing is not required.


### 6.4. Event schema versioning

Events include a schema version.

Future changes must use an explicit compatibility strategy.

For example:

```text
PackageVersionObserved v1
          ↓
PackageVersionObserved v2
```

The purpose is to prevent R2 from being tightly coupled to R1's initial implementation.


### 6.5. Ordering

Global ordering is not required.

Ordering assumptions must be explicitly documented per event type.

If chronological ordering is needed for a particular analysis,
it should be reconstructed using domain timestamps
rather than relying on global message ordering.


---
## 7. Architecture

### 7.1. High-level architecture

```text
                         Public Debian Data
                                │
                                ▼
                     ┌─────────────────────┐
                     │      Ingestion      │
                     └──────────┬──────────┘
                                │
                                ▼
                     ┌─────────────────────┐
                     │    Raw Storage      │
                     │     immutable       │
                     └──────────┬──────────┘
                                │
                                ▼
                     ┌─────────────────────┐
                     │     Normalizer      │
                     └──────────┬──────────┘
                                │
                         Domain Events
                                │
                                ▼
                     ┌─────────────────────┐
                     │    NATS JetStream   │
                     └──────────┬──────────┘
                                │
                         ┌──────┴──────┐
                         ▼             ▼
                  Analytical      Validation /
                  Consumer          Metadata
                         │
                         ▼
                      Parquet
                         │
                         ▼
                      DuckDB
                         │
                    ┌────┴────┐
                    ▼         ▼
                 FastAPI     CLI
```


### 7.2. Architectural principle

The system separates:

 1. source data,
 2. domain events,
 3. analytical state,
 4. query interfaces.

This separation allows each layer to evolve independently.


---
## 8. Ingestion

### 8.1. Responsibilities

The ingestion component:

 - retrieves or receives source data,
 - validates acquisition,
 - records provenance,
 - stores the raw artifact,
 - creates a dataset manifest,
 - triggers downstream processing.

It must not directly mutate analytical state.


### 8.2. Raw data

Raw data should be preserved, whenever licensing and storage constraints allow it.

Raw artifacts are immutable.

A normalized dataset can then be recreated from

> raw artifact + transformation version + configuration


### 8.3. Incremental ingestion

This release (R1) should support incremental processing where the data source permits it.

To avoid unnecessary processing, the implementation should distinguish:

 - **new** data
 - **changed** data
 - **unchanged** data
 - **invalid** data


---
## 9. Normalization

The normalization layer converts source-specific structures into canonical domain representations.

Example:

```text
UDD row
   ↓
validation
   ↓
canonical PackageVersion
   ↓
PackageVersionObserved event
```

A normalized record should remain traceable to the source artifact from which it originated.
=> Normalization must preserve source provenance.


---
## 10. Analytical Storage

### 10.1. Parquet

Parquet is the primary analytical storage format.

Reasons:

 - columnar representation,
 - efficient analytical scans,
 - compression,
 - portability,
 - ecosystem compatibility,
 - low operational overhead,
 - suitability for immutable historical data.


### 10.2. DuckDB

DuckDB provides the analytical query engine.

It is particularly suitable because it:

 - runs locally,
 - can query Parquet directly,
 - requires little operational infrastructure,
 - provides SQL,
 - performs analytical workloads efficiently,
 - fits the homelab constraints.

Example:

```sql
SELECT
    package,
    version,
    release
FROM read_parquet('data/normalized/package_versions/*.parquet')
WHERE release = '...',
```


### 10.3. Why not PostgreSQL?

PostgreSQL is deliberately not required for this release (R1).

The primary workload for R1 is:

 - historical,
 - analytical,
 - predominantly append-oriented,
 - reproducible,
 - file-based.

There is not yet a strong requirement for:

 - mutable transactional state,
 - high-concurrency point queries,
 - frequent row-level updates,
 - relational operational state.

Introducing PostgreSQL would therefore add operational complexity
without solving a demonstrated R1 problem.

If a later workload requires those properties,
PostgreSQL may be introduced as an explicit architectural decision.


---
## 11. Messaging

### 11.1. NATS JetStream

NATS JetStream is the event backbone.

It provides:

 - durable streams,
 - consumer state,
 - message persistence,
 - replay,
 - acknowledgements,
 - retry capabilities.

It is deliberately preferred over introducing Kafka at this stage
because R1 does not require Kafka's operational footprint or ecosystem.

Kafka remains a relevant market technology and
may be evaluated later if the workload justifies it.


### 11.2. Stream design

A conceptual stream may contain:

```text
debian.events
```

with subjects such as:

```text
debian.package.observed
debian.package_version.observed
debian.release.observed
debian.migration.observed
```

The exact subject hierarchy is an implementation decision documented through an ADR.


### 11.3. Consumer behavior

Consumers must follow a standard process:

 1. receive an event,
 2. validate the schema,
 3. check idempotency,
 4. process the event,
 5. persist the result,
 6. acknowledge the message.

The ordering of these operations must ensure that a crash does not silently produce data loss.


---
## 12. API & CLI


### 12.1. Application Programming Interface (network) Service

The API service is not intended to become a complete product frontend.
Its purpose is to demonstrate an structured architecture open to extensions:

Patterns:

 - service boundaries,
 - API contracts,
 - structured responses,
 - instrumentation,
 - analytical access.

Candidate **endpoints**:

 - GET /health
 - GET /packages/{package}
 - GET /packages/{package}/versions
 - GET /releases
 - GET /migrations
 - GET /datasets/{dataset}
 - GET /events/{event_id}

Features:

 - The final API should remain **small**.
 - Endpoints should correspond to **real use cases** demonstrated by the project.


FastAPI provides a small query interface.


### 12.2. Commnd Line Interface (CLI)

As a common ground between humans and automations, the CLI is the primary engineering interface.
A CLI makes the pipeline easy to reproduce, automate and test without requiring a frontend.

Candidate commands:

```shell
debian-observer ingest
debian-observer normalize
debian-observer replay
debian-observer query
debian-observer validate
debian-observer benchmark
debian-observer status
```


---
## 13. Observability

### 13.1. OpenTelemetry

OpenTelemetry is introduced from the first release to
provide a consistent instrumentation model for later ones.

The system should emit:

 - traces,
 - metrics,
 - structured logs where appropriate.


### 13.2. Core metrics

Examples:

 - ingestion_duration_seconds
 - records_ingested_total
 - events_published_total
 - events_processed_total
 - events_failed_total
 - consumer_processing_duration_seconds
 - consumer_lag
 - bytes_ingested_total
 - query_duration_seconds


### 13.3. Distributed tracing

To provide an early foundation for the distributed tracing requirements of
subsequent releases, a trace should be able to represent a path such as:

```text
ingestion
   ↓
normalization
   ↓
event publication
   ↓
consumer
   ↓
Parquet write
```


### 13.4. What is deferred

This release (R1) doesn't require a complete observability stack.

The project may expose telemetry to a lightweight local backend during development.

A full Prometheus/Grafana/Loki/Tempo platform belongs to R3.


---
## 14. Deployment

We aim to make the deployment:

 - scalable (from modest homelab to huge cloud swarms. See "Restrictions" chapter for details),
 - reproducible/consistent,
 - modular/isolated, and
 - dependency-managed.

Patterns:

 - Containerization provides this, and Docker Compose provides the initial orchestration layer.
 - Infrastructure as Code (IaC) improoooves consistency and scalability.


The goal is not to recreate an entire cloud provider locally. Locally, the IaC layer should demonstrate:

 - declarative infrastructure,
 - reproducibility,
 - configuration separation,
 - environment provisioning.

Then, cloud equivalents may be documented as mappings rather than deployed. For example:

```text
Local
  NATS
  Docker network
  object/file storage
  service containers

AWS conceptual mapping
  MSK
  S3
  ECS/EKS
  CloudWatch/OpenTelemetry
```

The mapping must not imply that the local implementation is equivalent in operational characteristics to the cloud service.

OpenTofu will be used where infrastructure provisioning benefits from automation.


---
## 15. Reliability Properties

### 15.1. Idempotency

Processing the same event more than once must not corrupt analytical state.

A test should demonstrate:

```text
event A
event A
event A
```

producing the same logical state as:

```text
event A
```


### 15.2. Consumer restart

A consumer should be stoppable while events remain pending.

After restart:

```text
consumer stopped
      ↓
events remain durable
      ↓
consumer restarted
      ↓
events processed
```


### 15.3. Failure recovery

At minimum, this release (R1) should demonstrate recovery from:

 - consumer process failure,
 - temporary processing failure,
 - duplicate delivery.


### 15.4. Data integrity

Invalid records must not silently become valid-looking analytical data.

Thus, the system must detect:

 - malformed records,
 - invalid schemas,
 - missing required identifiers,
 - inconsistent source data.


---
## 16. Benchmarked Performance

Benchmarking enables quantitative awarness on the system's performance.

It enables:

 - incident response,
 - incident prevention,
 - suply planning,
 - analysis (trends, correlation, ...),
 - and quantitative estimations (dimensioning, budgeting, forecasting...).


### 16.1. Dataset Size

Record:

 - source size,
 - normalized size,
 - number of records,
 - number of events,
 - number of packages,
 - time range.


### 16.2. Queries

For representative queries:

 - package history,
 - release package counts,
 - migration history,
 - version distribution,
 - time-based aggregations.

Record:

```text
query
dataset size
execution time
peak memory
```

### 16.3. Other Processing

Measure:

 - ingestion duration,
 - normalization throughput,
 - events/second,
 - peak memory,
 - CPU utilization,
 - storage amplification.


### 16.4. Recovery

Measure:

 - events pending at failure,

 - recovery time,
 - processing throughput after restart,
 - duplicate events,
 - final consistency.

For instance:

```text
1,000,000 events to be processed
       ↓
consumer failure: 876423 events pending
       ↓
restart
       ↓
122 seconds elapsed until recovery of consumer failure
0 lost events
0 inconsistent records
```

### 16.5. Efficiency

Efficiency is not explicitly requested, but observing it is an engineering best practice
that allows to detect and tackle problems. The project should record:

 - peak RAM,
 - CPU usage,
 - storage,
 - dataset size,
 - processing time.

With this, the system operator may set thresholds to trigger alarms and react accordingly.
In SRE practice, this will be automated (in coming releases). For the release, awareness
of the system's load and response is enough.


---
## 17. Security & Supply Chain

Even though R1 is primarily a data-processing project, its implementation should follow basic supply-chain practices.

### 17.1. Dependencies

Use:

 - pinned or constrained dependencies,
 - lockfiles where appropriate,
 - automated dependency updates where practical.


### 17.2. Containers

Container images should be:

 - minimal,
 - versioned,
 - scanned,
 - built reproducibly where practical.


### 17.3. SBOM

Generate an SBOM for project artifacts if tooling can be integrated within the two-week scope.


### 17.4. Secrets

No secrets should be committed.

Configuration should distinguish:

 - public dataset configuration,
 - local environment configuration,
 - credentials, if ever required.

R1 should ideally require no secrets at all.


---
## 18. Definition of Done

R1 is complete when all of the following are true.

### Functional

 - [ ] Public Debian dataset acquired.
 - [ ] Raw data preserved.
 - [ ] Dataset manifest generated.
 - [ ] Canonical model implemented.
 - [ ] Domain events generated.
 - [ ] Events persisted in NATS JetStream.
 - [ ] Consumer implemented.
 - [ ] Idempotency demonstrated.
 - [ ] Parquet datasets generated.
 - [ ] DuckDB queries implemented.
 - [ ] FastAPI interface available.
 - [ ] CLI available.

### Reliability

 - [ ] Consumer restart tested.
 - [ ] Duplicate delivery tested.
 - [ ] Malformed input tested.
 - [ ] Interrupted processing tested.
 - [ ] Replay demonstrated.

### Observability

 - [ ] OpenTelemetry instrumentation implemented.
 - [ ] Core metrics available.
 - [ ] Representative trace available.
 - [ ] Processing failures observable.

### Performance

 - [ ] Dataset size measured.
 - [ ] Processing throughput measured.
 - [ ] Peak memory measured.
 - [ ] Representative query latency measured.
 - [ ] Recovery time measured.

### Reproducibility

 - [ ] Devcontainer works.
 - [ ] Local deployment documented.
 - [ ] Dataset provenance documented.
 - [ ] Processing can be repeated.
 - [ ] Results can be independently verified.

### Portfolio

 - [ ] Architecture diagram.
 - [ ] ADRs.
 - [ ] Benchmark results.
 - [ ] Failure demonstration.
 - [ ] Short project pitch.
 - [ ] R2 integration documented.


---
## 19. Testing Strategy


### 19.0. Development Container

A Devcontainer should provide the development environment.

A new contributor should be able to obtain:

 - language tooling,
 - dependencies,
 - CLI,
 - tests and their coverage,
 - formatting/linting,
 - local service configuration.


### 19.1. Unit Tests

Cover:

 - parsers,
 - validators,
 - normalization,
 - event creation,
 - schema validation,
 - idempotency logic.


### 19.2. Integration Tests

Exercise:

```text
source
 ↓
ingestion
 ↓
NATS
 ↓
consumer
 ↓
Parquet
 ↓
DuckDB
```


### 19.3. Contract Tests

Validate:

 - event schemas,
 - API responses,
 - dataset manifests.

These contracts are particularly important because R2 will consume R1's outputs.


### 19.4. Failure Tests

At minimum:

 1. kill consumer during processing,
 2. restart consumer,
 3. replay an event,
 4. inject malformed input,
 5. interrupt ingestion,
 6. rerun the pipeline.


### 19.5. Data Reproducibility Test

A clean environment should produce equivalent analytical results
from the same dataset manifest and transformation version.


---
## 20. Demonstration Scenario

The final demonstration should tell a coherent engineering story.

### Step 1 — Acquire real data

```text
$ debian-observer ingest ...
```

Show:

 - source,
 - dataset version,
 - size,
 - records,
 - checksum.


### Step 2 — Normalize

```text
$ debian-observer normalize ...
```

Show:

```text
raw records
    ↓
canonical records
    ↓
domain events
```


### Step 3 — Process through NATS

Show the stream and consumer processing.

```text
published: ...
processed: ...
pending: ...
failed: ...
```


### Step 4 — Query historical data

Example:

```text
$ debian-observer query package ...
```

Show package versions and historical observations.


### Step 5 — Kill the consumer

Terminate the processing process while events remain pending.

Show:

```text
pending > 0
```


### Step 6 — Restart

Start the consumer again.

Show:

```text
pending → 0
```

and demonstrate that the resulting dataset remains consistent.


### Step 7 — Analyze

Run a representative DuckDB query.

Show:

 - execution time,
 - dataset size,
 - result,
 - resource consumption.


### Step 8 — Explain the architecture

Conclude with:

```text
Real data
   ↓
reproducible ingestion
   ↓
event-driven processing
   ↓
durable events
   ↓
analytical storage
   ↓
observable service
```

Then explain how R2 will use these outputs to study dependency and migration dynamics.


---
## 21. Three-Week Execution Plan

### Days 1–2 — Data & Architecture

Deliverables:

 - repository skeleton,
 - Devcontainer,
 - dataset manifest,
 - initial source acquisition,
 - domain model,
 - architecture diagram,
 - first ADRs.

Success criterion:

> A clean environment can obtain and inspect the chosen dataset.


### Days 3–4 — Ingestion

Deliverables:

 - ingestion CLI,
 - raw artifact storage,
 - checksums,
 - provenance metadata,
 - validation,
 - basic metrics.

Success criterion:

> A source dataset can be acquired reproducibly without corrupting the raw artifact.


### Days 5–6 — Canonical Model

Deliverables:

 - normalization pipeline,
 - canonical records,
 - event envelope,
 - schema validation,
 - Parquet output.

Success criterion:

> Raw Debian data can be transformed into a reproducible canonical dataset.


### Days 7–8 — NATS JetStream

Deliverables:

 - NATS deployment,
 - event subjects,
 - publisher,
 - durable stream,
 - consumer,
 - acknowledgement,
 - idempotency.

Success criterion:

> A consumer can be stopped and restarted without losing durable events.


### Days 9–10 — DuckDB & Query Layer

Deliverables:

 - analytical datasets,
 - representative SQL queries,
 - CLI query commands,
 - FastAPI service.

Success criterion:

> Historical Debian data can be queried through SQL, CLI and API interfaces.


### Days 11–12 — Observability & Reliability

Deliverables:

 - OpenTelemetry instrumentation,
 - metrics,
 - traces,
 - failure tests,
 - replay test,
 - recovery measurements.

Success criterion:

> The system can demonstrate and measure failure recovery.


### Days 13–14 — Benchmarking & Portfolio Hardening

Deliverables:

 - benchmark suite,
 - resource measurements,
 - architecture documentation,
 - ADRs,
 - demonstration script,
 - final README,
 - limitations,
 - R2 handoff documentation.

Success criterion:

> A third party can reproduce the project and understand both its engineering decisions and its measured results.



