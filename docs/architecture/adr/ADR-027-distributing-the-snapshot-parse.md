# ADR-027 — Distributing the snapshot parse

Spec: `docs/release3.project.v0.1.md` §2.1, §7.2, §11.2, §11.3
Status: Accepted
Date: 2026-11-23

Narrows the scope of ADR-018 (work distribution over JetStream):
distribution applies where the unit of work is expensive.


## Context

ADR-018 distributes work over a JetStream work queue. Measurement since
showed that per-root dependency closures are too cheap to pay for it
(0.1 ms of work per root against ~0.3 ms per queue message; a whole
coordinate in seconds in memory).

The britney pipeline has two steps of very different cost:

 - the **parse** of one snapshot: ~10 s and ~900 MB, independent per
   snapshot;
 - the **pair diff**: a comparison of two compact tables, which must
   see snapshots in order (ADR-024).

Live following parses one snapshot per britney run; rebuilding one
archive year parses thousands (~21.5 h serial for 2026).


## Problem

What, if anything, is distributed, and over what?


## Alternatives

 1. **Distribute the parse over a JetStream work queue; keep the diff
    sequential.**
 2. Distribute nothing; parse serially. Fine for live following,
    ~21.5 h for one archive year.
 3. An in-process pool on one machine. Same speed as the queue on one
    machine, but a killed child hangs the parent and loses its work
    silently, and it cannot use other machines.
 4. Distribute the diff as well. Nothing to gain (it is cheap) and it
    breaks the per-pair publication order.


## Decision

**The snapshot parse is a JetStream work queue (`britney-parse`), one
job per snapshot, pulled by workers on any machine that reaches the
NATS server; the pair eventizer stays a single sequential process.**
`ack_wait` exceeds the worst-case parse under contention, workers send
in-progress acknowledgements while parsing, `max_ack_pending` equals
the worker slots (backpressure), and `max_deliver` bounds poisoned
jobs. Results are written atomically, keyed by snapshot.


## Consequences

 - Remote workers need the raw files without fetching from the source
   and without a shared filesystem: the raw transport is decided in
   the spec (§7.3, NATS Object Store by default).
 - Remote workers need NATS reachable beyond localhost, with
   per-worker credentials (spec §17).
 - Live following does not need the queue, but uses it, so archive and
   live share one path.


## Evidence

*Probe 2026-11-23*, 24 snapshots:

| Configuration | Wall |
|:--|--:|
| serial | 249.6 s |
| process pool, 2 / 4 | 133.9 / 117.4 s |
| queue workers, 2 / 4 (to last result) | 134.7 / 126.8 s |

Killing one of two workers: the pool hung with 17 of 24 results; the
queue redelivered and finished 24 of 24, identical. One parse exceeded
a 20 s `ack_wait` under 4 workers and was redelivered spuriously (no
loss, idempotent writes). Queue message cycle: 0.293 ms against a
10.1 s unit.


## Revisit when

 - the parse becomes cheap enough that one machine rebuilds a year in
   minutes;
 - the source publishes the history in a pre-parsed form.
