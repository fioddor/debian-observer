# ADR-005 — Event Delivery Semantics

Status: Proposed
Date: 2026-09-29


## Context

The Debian Observatory publishes domain events representing normalized facts about the Debian ecosystem
(PackageObserved, PackageVersionObserved, ReleaseObserved, MigrationObserved).
These events traverse an asynchronous pipeline from normalizer to consumers via NATS JetStream.

In distributed messaging systems, failures are inevitable:
network partitions, consumer crashes, processing errors, and broker restarts can all interrupt message flow.
The system must define how it handles these failures and what guarantees it provides to consumers.

Event ordering represents a standard consideration.
The system does not require global ordering across all event types;
chronological relationships can be reconstructed from domain timestamps within payloads
rather than relying on broker-enforced sequence.

Exactly-once delivery and exactly-once processing are theoretically desirable
but practically complex to implement correctly,
often requiring distributed transactions, idempotent message brokers, and complex state coordination.


## Decision

The system will use **at-least-once delivery with idempotent consumers** as the baseline reliability model.

**Exactly-once processing is not required** for R1.

Consumers must follow a standard process:

 1. receive an event
 2. validate the schema
 3. check idempotency (using `event_id`)
 4. process the event
 5. persist the result
 6. acknowledge the message

The ordering of these operations must ensure that a crash does not silently produce data loss.
Specifically, the result must be durably persisted before the message is acknowledged.

Event identity is provided by `event_id` at the event level.
Consumers are responsible for detecting and handling duplicate deliveries.

Global ordering is not required. Ordering assumptions will be explicitly documented per event type.
Where chronological ordering is needed for analysis,
it will be reconstructed using `observed_at` and `effective_at` domain timestamps
rather than relying on global message ordering.


## Consequences

Pros:

 - **Duplicate events are harmless** and do not corrupt analytical state
 - **System remains correct** under crashes, restarts, and network failures without complex coordination
 - **Simpler to implement and validate** within the two-week release timeline

Cons:

 - Consumer logic must include idempotency checks, adding **modest implementation complexity**
 - Consumers may process the same event multiple times; **downstream storage must handle this**
 - **Lack of global ordering** means consumers cannot assume causal sequence from delivery order
