# ADR-004 — NATS JetStream

Status: Proposed
Date: 2026-09-29


## Context

Event-driven architectures support essential capabilities for this platform:
 - decoupling ingestion from processing,
 - durable buffering,
 - replay,
 - independent consumers,
 - failure recovery,
 - and explicit domain contracts.

These capabilities become increasingly important as the project progresses toward R2 and beyond.

The messaging backbone must provide:

 - durable streams that survive consumer restarts
 - message persistence for replay and recovery
 - consumer state tracking and acknowledgements
 - retry capabilities for transient failures
 - low operational overhead suitable for homelab deployment

The project operates under resource constraints (8 GB RAM, home lab deployment)
and a two-week implementation timeline.
Message brokers with heavy operational footprints would waste too much engineering effort.


## Decision

**NATS JetStream** will be the event backbone for R1.

NATS JetStream provides:

 - durable streams with configurable retention
 - consumer state and acknowledgement tracking
 - message persistence and replay capabilities
 - at-least-once delivery semantics
 - retry and dead-letter mechanisms
 - significantly lower operational footprint than alternatives

A conceptual stream `debian.events` will carry subjects such as:

 - `debian.package.observed`
 - `debian.package_version.observed`
 - `debian.release.observed`
 - `debian.migration.observed`

The exact subject hierarchy will be refined in implementation and documented separately.


Note: Kafka is explicitly deferred. While Kafka remains a relevant and valuable market technology,
R1 does not require its operational footprint or ecosystem complexity.
Kafka may be evaluated in a future release if the workload justifies it.


## Consequences

Pros:

 - Event producers and consumers are decoupled, enabling **independent evolution**
 - Durable streams **protect** against data loss during consumer failures
 - Replay capability **supports recovery, testing, and reprocessing** scenarios
 - **Lower operational overhead** aligns with resource constraints
 - Clustering and **multi-node deployments are possible** although not required for R1
 - If **scaling** demands increase, future migration to Kafka **remains architecturally feasible**

Cons:

 - NATS ecosystem is smaller than Kafka's; **advanced operational tooling is limited**
