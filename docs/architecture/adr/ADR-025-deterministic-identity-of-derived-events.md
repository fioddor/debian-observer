# ADR-025 — Deterministic identity of derived events

Spec: `docs/release3.project.v0.1.md` §6.2
Status: Accepted
Date: 2026-11-23

Departs, for snapshot-derived events only, from the random
`event_id` of the existing events; builds on ADR-019.


## Context

Every existing event carries a random UUIDv4 `event_id`; idempotency
of derived tables rests on logical keys (ADR-019), and `event_id` is
excluded from equality checks.

Events derived from snapshot pairs (ADR-024) are re-derived whenever a
window is eventized again: after a crash, for a replay, for a new
consumer, for a parser fix. With random ids, every re-derivation
publishes a different set of messages for the same facts.


## Problem

How can re-eventizing the same pair produce messages that the stream
and the consumers recognise as the same?


## Alternatives

 1. **`event_id = uuid5(namespace, canonical(logical key + payload))`.**
    Same pair and same content → same id; changed content (a parser
    fix) → a new id.
 2. Random ids plus logical keys in every consumer (ADR-019 as is). It
    works for folds but gives the stream nothing to deduplicate on, and
    every consumer must rebuild the key.
 3. `uuid5` of the logical key alone. A parser fix would then reuse the
    old ids for different content, and the corrected events would be
    deduplicated away as if they were the old ones.


## Decision

**Snapshot-derived events carry
`event_id = uuid5(namespace, canonical(logical key + payload))`**, with
the logical key `(event_type, entity, prev_snapshot, next_snapshot)`
(`(ExcuseObserved, entity, seed_snapshot)` for the seed). The id is
published as `Nats-Msg-Id`. The envelope already accepts an explicit
id (`EventEnvelope.from_payload(event_id=…)`).

JetStream's deduplication is an optimization: its window is
time-bounded. **The guarantee is the consumers'**: they deduplicate by
`event_id`.


## Consequences

 - Fixed test vectors pin the derivation; changing the namespace or the
   canonical form is a breaking change.
 - These events may enter equality checks by id; the existing events
   keep their random ids.


## Evidence

*Probe 2026-11-23*: re-publishing the full 48-hour window added 0
messages within the duplicate window; a consumer killed with 200
messages in flight ended with 8,344 of 8,344 ids, 0 lost and 0
duplicated after deduplication by id, identical to a clean run.


## Revisit when

 - an event type needs identity that survives a content change (then
   the payload leaves the hash for that type).
