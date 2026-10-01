# ADR-009 — Resource Constraints

Status: Proposed
Date: 2026-09-29


## Context

The Debian Observatory must be deployable on resource-restricted hardware.
The development environment has 8 GB RAM; the primary deployment target has 8 GB RAM with additional 4 GB machines available.

There is a requirement for no permanent cloud infrastructure.

This ough to **enable any researcher to deploy on a home lab for proof-of-concept validation before scaling to cloud resources**.

These constraints affect every architectural choice:

 - Data processing must not assume unlimited memory
 - Services must not require dedicated servers per component
 - Storage formats must be space-efficient
 - Message brokers must not consume excessive RAM
 - Query engines must run locally without cluster infrastructure
 - The entire stack must run comfortably alongside the host operating system

Additionally, the two-week implementation timeline means that
resource-intensive integrations (Kubernetes, Kafka clusters, distributed databases)
would waste too much effort relative to their R1 value.


## Decision

The architecture will explicitly respond to resource constraints through the following strategies:

 1. **Columnar, compressed storage**: Parquet provides efficient on-disk representation, reducing I/O and memory pressure during analytical scans.
 2. **Embedded query engine**: DuckDB runs in-process without a separate server, eliminating inter-process communication overhead and service management.
 3. **Lightweight messaging**: NATS JetStream provides durability and replay with significantly lower memory footprint than Kafka.
 4. **File-based reproducibility**: Immutable files replace mutable database state where possible, reducing operational complexity and memory usage.
 5. **Containerized local deployment**: Docker Compose orchestrates services on a single host; multi-node orchestration is deferred.
 6. **Incremental processing**: Where data sources permit, the system distinguishes new, changed, unchanged, and invalid data to avoid redundant work.
 7. **Explicit scope limitation**: R1 does not implement distributed graph computation, multiple specialized workers, or complex stateful services that would demand horizontal scaling.

## Consequences

Pros:

 - The system is **deployable on modest home lab** hardware without cloud dependency
 - Engineering **effort focuses on business value** (data platform correctness rather than infrastructure complexity)
 - Memory-efficient formats and embedded engines **fit within** 8 GB **constraints**
 - Single-node limitations are acceptable for R1; distributed **scaling is planned** for future releases
 - Dataset size will eventually outgrow local storage; **cloud migration is anticipated** but not required
 - **Performance benchmarking** under these constraints becomes a asset for demonstration and efficiency analysis
