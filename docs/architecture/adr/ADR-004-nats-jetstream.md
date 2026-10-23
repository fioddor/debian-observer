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

The stream is named `debian-events` (dots are invalid in NATS stream names). It carries subjects such as:

 - `debian.package.observed`
 - `debian.package_version.observed`
 - `debian.release.observed`
 - `debian.migration.observed`

The exact subject hierarchy will be refined in implementation and documented separately.

> **Implementation note (2026-10-02):** The messaging layer has been verified against a live NATS v2.10 JetStream server. The `nats-py` v2.16 API expects `num_replicas` (not `replicas`) and does not accept `max_deliver` in `js.subscribe()`. Message acknowledgment is wrapped to handle `MsgAlreadyAckdError` gracefully.


Note: Kafka is explicitly deferred. While Kafka remains a relevant and valuable market technology,
R1 does not require its operational footprint or ecosystem complexity.
Kafka may be evaluated in a future release if the workload justifies it.


## Amendment (2026-10-23) — the publish pipeline

R1 lands the producer side: `debian-observer publish --events-dir <dir>
[--stream debian-events] [--server nats://…]` projects the append-only
Parquet event log onto the stream. Decisions:

- **The stream is a view, not a source of truth.** The Parquet event log
  remains the backbone (ADR-003 amendment, 2026-10-23); the stream is
  fully rebuildable from `--events-dir`, and `consolidate --events-dir`
  regenerates the same consolidated table the streaming path produces
  (`fold(log parquet) == fold(stream)`).
- **Subjects are flat per event type — confirmed:**
  `debian.package.observed`, `debian.package_version.observed`,
  `debian.release.observed`, `debian.migration.observed`. A
  `.<package_id>` suffix (per-package partitioning) was considered and
  **rejected**: the partition adds subject cardinality with no consumer
  that needs it — the package id already travels in the payload.
- **`Nats-Msg-Id: <event_id>`** on every published message: JetStream
  deduplicates server-side within the stream's duplicate window, so
  re-publishing the same log is a no-op (idempotent publish).
- **No `consume` CLI.** Consuming is a pipeline the researcher owns —
  a copy of `examples/consume_packages.py` (durable name, subject,
  dataset dir via environment) run as its own process. The supported
  tool surface is the library (`messaging`, `consumer_app`) plus
  `publish` and the batch `consolidate`.

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
