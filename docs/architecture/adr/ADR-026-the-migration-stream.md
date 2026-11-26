# ADR-026 — The migration stream

Spec: `docs/release3.project.v0.1.md` §11.1
Status: Accepted
Date: 2026-11-23

Relates to ADR-004 (NATS JetStream) and ADR-005 (event delivery
semantics).


## Context

All events share one stream, `debian-events`, with limits sized for a
small deployment: 100,000 messages, 30 days, 1 GB.

Migration events have a different profile: a continuous flow
(≈4,200 events a day, *probe 2026-11-23*) that consumers replay from
the start, plus backfills of whole archive windows.


## Problem

Where do migration events live so that their retention neither evicts
the existing events nor is evicted by them?


## Alternatives

 1. **A dedicated stream, `debian-migrations`**, with its own subjects
    and retention.
 2. Raise the limits of `debian-events` for everything. Couples the
    retention of unrelated event types and their disk budget.
 3. Keep migration events only in the Parquet event lake, without a
    stream. Loses the live consumers and the replay to new consumers
    that the release exists for.


## Decision

**Migration events go to a dedicated stream, `debian-migrations`**:
subjects `debian.migration.excuse.>` and
`debian.migration.snapshot.>`, file storage, limits retention, a
duplicate window of hours, maximum age and bytes sized from the
measured volume. The Parquet event lake stays the durable record.


## Consequences

 - The stream configuration gains a second stream; `debian-events` and
   its subjects are untouched, including `debian.migration.observed`.
 - Retention is a deployment parameter, sized and documented, not a
   constant.


## Evidence

*Probe 2026-11-23*: ≈4,200 events a day of ~607 B (≈2.5 MB a day,
≈1 GB a year); at that rate the 100,000-message limit of
`debian-events` would be reached in under a month.


## Revisit when

 - the measured volume differs from the probe by an order of magnitude;
 - more state sources join and a shared stream becomes simpler.
