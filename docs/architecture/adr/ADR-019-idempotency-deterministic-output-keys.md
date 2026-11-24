# ADR-019 — Idempotency by deterministic output keys

Spec: `docs/release2.project.v0.4.md` §6.2, §10.1, §11.2
Status: Accepted
Date: 2026-11-08

Supersedes the R1 note "persistent IdempotencyStore deferred to R2"
(`docs/design/module-contracts.md`, `idempotency` block, and the
`idempotency.py` docstring).

## Context

R1 defines idempotency the same way R2 needs it:
`fold(A) == fold(A, A, A)` — re-running a fold over the same
input yields an identical table (order-insensitive, random-by-
design fields excluded: `event_id`, `observed_at`,
`effective_at`, `ingestion_run_id`, spec §6.2). R1 deferred a
persistent `IdempotencyStore` to R2 without a concrete design.

R2's idempotency surfaces:

 - **derived tables**: folds keyed by their primary key
   (e.g. the straight table's key `(coordinate, package, version,
   kind, clause_index)` group, spec §10.2 P1);
 - **per-job outputs**: the deep-dependency cache, one file per
   job, **redelivered after worker crashes**
   (ADR-018: crash before ack → redelivery);
 - **enqueueing**: the same job set enqueued repeatedly by
   `benchmark run` iterations
   (spec §16.1: `deps-enqueue --force`, deterministic).


## Problem

What mechanism makes re-running eventization, prosumers,
enqueueing and jobs safe — without a persistent store of
"already done" facts, which would itself be state to operate,
back up and restore?


## Alternatives

 1. **Idempotency as a property of the outputs**:
    Deterministic keys and paths, atomic replacement.
    "already done" is *checkable from the filesystem/table itself*.
 2. A persistent IdempotencyStore (R1's deferral):
    A registry of processed inputs/outputs.
    New state, new consistency questions
    (what if the store survives but the output file was lost?),
    new backup/restore surface, for a release whose
    outputs are already deterministic.
 3. Server-side JetStream dedup: covers only the 2-minute
    duplicate window (ADR-018) — not a job-safety mechanism.


## Decision

**No persistent IdempotencyStore in R2. Idempotency is
deterministic output keys plus atomic writes; every re-run
safely overwrites:**

 1. **Derived tables are folds by primary key**:
    `fold(A) == fold(A, A, A)` holds as in R1 §15.1.
    The logical key of a dependency observation makes
    re-eventizing the same dump produce new `event_id`s but the
    **same keys**: `(distribution, release, architecture,
    package, version, kind, clause_index)`
    (measured: **0 collisions** over all 8.4 M clauses,
    *measured 2026-11-03*, spec §6.2).
 2. **Per-job outputs have deterministic paths**:
    The cache file name is derived from the job's own parameters
    (`<distribution>-<release>-<architecture>-<components>-
    <package>-<version>.parquet`, epochs percent-encoded,
    spec §10.1), so a redelivered job writes the same file it
    wrote before — **idempotent overwrite**.
 3. **Writes are atomic**
    (tmp + `os.replace`, R1's consolidated-table pattern):
    A reader never sees a half-written file, and
    a crashed write leaves the previous file intact.
 4. **`deps-enqueue` skips existing outputs** (unless `--force`):
    idempotent enqueueing is the enqueuer's job, not the
    server's (ADR-018).
 5. This ADR supersedes the deferral note.
    The R1 `idempotency` contract block and the `idempotency.py`
    docstring are updated to point here


## Consequences

 - No new state to operate:
   Files and tables are their own idempotency record.
 - Crash-redelivery (ADR-018) needs no coordination:
   same job → same path → overwrite; the crash test (spec §15.2)
   asserts final cache equality with an uninterrupted run.
 - Re-running eventization doubles nothing:
   Folds collapse duplicates by key. random-by-design fields
   are excluded from every equality check by contract.
 - `--force` enqueue is explicit and reserved for benchmark
   iterations that must re-run identical job sets (spec §16.1).


## Evidence

 - Spec §6.2 (key collision measurement), §10.1 (paths), §11.2
   (enqueue skip), §15.1 (fold idempotency as an R1 property).
 - E6 (spec §16.7): recovery from a killed worker with K jobs
   pending ends in cache equality — the operational proof.


## Revisit conditions

 - Jobs whose output depends on *external mutable state* at
   compute time (e.g. a live upstream query) would break the
   deterministic-path argument and reopen the store question —
   no such job exists in R2
   (all inputs are the frozen event log and derived tables).
 - R3's Kubernetes operation (spec §3.2.4) may add job
   completion *tracking* for SLOs, but that's observability,
   not idempotency.
