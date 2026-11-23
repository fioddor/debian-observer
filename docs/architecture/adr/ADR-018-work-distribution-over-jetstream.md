# ADR-018 — Work distribution over JetStream

Spec: `docs/release2.project.v0.4.md` §11.2
Status: Accepted
Date: 2026-11-08


## Context

R1 uses NATS JetStream (`messaging/config.py`,
`ensure_stream` create-if-missing; ADR-004/005) for its
`debian-events` stream (limits retention,
`max_msgs=100_000`, `max_bytes=1 GB`).

R2's parallel workload is deep-dependency closures:
~70 k roots per coordinate × 37 release×architecture coordinates
× component lists — each job independent, each output a
deterministic file (the cache, spec §12). The work queue is
where R2's distributed-systems story lives: workers may run on
different machines (spec §2.4) able to read from NATS and write
the derived data over a shared directory.

Whether R2 observation events are *published* to JetStream at
all is the owner's open decision **D1** (spec §24): the prosumers
read the Parquet log, so publishing is optional, not load-bearing.
(R1 principle: the log is the backbone, the stream is a view)


## Problem

How to distribute independent per-root jobs to N workers on
2–3 homelab machines — with no lost and no duplicated work after
crashes, no second queueing infrastructure, and semantics that
survive `kill -9` (spec §15.2)?


## Alternatives

 1. **A dedicated JetStream work-queue stream**
    (`debian-work`, `retention=workqueue`, one durable pull
    consumer `deepdeps` shared by all workers),
    created idempotently by the existing `ensure_stream`.
 2. Reuse the `debian-events` stream as the work queue: it is a
    *view* stream (limits retention) whose subject names are
    observations (`debian.*.observed`), not jobs — mixing job
    subjects into it couples two retention policies and makes
    the events stream un-erasable from the job machinery.
 3. A task queue system (Celery/RQ + Redis broker): a new
    service, a new dependency, a new failure domain — against
    ADR-006/007's direction and the R1 principle of reusing the
    JetStream that is already deployed and observed.
 4. Plain NATS core (no JetStream): no durability — a crash
    between pull and processing loses the job; violates
    spec §15.2.


## Decision

 1. **Stream `debian-work` with work-queue retention**, subject
    `debian.work.deep_dependencies`, one durable pull consumer
    `deepdeps` shared by all workers. Both `deps-enqueue` and
    the workers call R1's `ensure_stream` (idempotent
    create-if-missing) with the `debian-work` config; there is no
    separate bootstrap.
 2. **Message body** (JSON):
    `{job_id, distribution, release, architecture, package,
    version, max_level}`. `job_id` = lowercase hex SHA-256 of the
    UTF-8 string `d|r|a|package|version|max_level` joined with the
    ASCII unit separator `\x1f` instead of `|`, `max_level`
    rendered as decimal where **0 means no max level** (the whole
    network). Python and Go produce the same id (contract test,
    spec §19.4). `job_id` is also sent as `Nats-Msg-Id`.
 3. **Idempotency is guaranteed by `deps-enqueue`, not by server
    dedup**: server dedup only covers the stream's 2-minute
    duplicate window, so a job whose output file already exists
    is skipped (unless `--force`). Crash-before-ack →
    redelivery → same deterministic output path → idempotent
    overwrite (ADR-019).
 4. **Consumer settings**: `ack_wait` 60 s; workers send an
    in-progress ack (`+WPI`) every 15 s while computing;
    `max_deliver` 5; a job that exhausts deliveries is logged
    and counted in `jobs_failed_total`; `max_ack_pending` =
    total worker concurrency (backpressure).
 5. **Worker loop**: pull job → compute closure → write output
    file atomically → append execution record → ack.
 6. Workers may run on any homelab machine reachable to NATS and
    to the derived data (shared directory; the mechanism is a
    deployment detail, documented in the user guide — spec §11.2).


## Consequences

 - One queue, no new infrastructure: the homelab's existing NATS
   carries R2's work distribution; `ensure_stream` bootstrap
   stays single-source.
 - Jobs are redelivered, never lost: `kill -9` mid-queue ends in
   a cache identical to an uninterrupted run (spec §15.2, crash
   test modelled on `scripts/crash-test-consumer.sh`).
 - The work queue is R2's JetStream use regardless of D1 —
   publishing observations is separable from distributing work.
 - E5 (spec §16.6) measures multi-machine distribution overhead
   (pavilion/pb) if the owner makes them available.


## Evidence

 - R1 ADR-004/005 (JetStream delivery semantics);
   `messaging/config.py` baseline.
 - E4 (spec §16.5) measures Python vs Go worker arms through
   this queue; E6 (spec §16.7) measures recovery.


## Revisit conditions

 - R3 (Kubernetes operation, spec §3.2.4) re-evaluates consumer
   placement and fairness, not the stream's existence.
 - If D1 is decided "publish", the *observations* stream's limits
   are sized separately (spec §11.1) — never merged into
   `debian-work`.


## Amendment (2026-11-23): where distribution applies — ADR-027

Measurement narrowed this decision's premise. The deep-dependency
closures turned out too cheap to pay for a work queue: 0.1 ms of work
per root against ~0.3 ms per queue message, and a whole coordinate
computes in seconds in memory. The queue described above stays as
built, but it is not where distribution pays off.

ADR-027 (`docs/release3.project.v0.1.md` §7.2, §11.2) records where
it does: the parse of britney's excuses snapshots, ~10 s and ~900 MB
per independent unit, distributed over `britney-parse` while the
pair eventizer stays sequential. The rule both share: distribute a
unit of work only when its cost dwarfs the cost of a message, or when
its failure semantics are needed.

The revisit condition above names "R3" as the release that operates
the workers on Kubernetes; that release is now R4. R3 is migration
dynamics.
