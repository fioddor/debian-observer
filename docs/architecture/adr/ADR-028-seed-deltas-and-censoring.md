# ADR-028 — Seed, deltas and censoring

Spec: `docs/release3.project.v0.1.md` §6.1, §9.4
Status: Accepted
Date: 2026-11-23

Companion of ADR-024.


## Context

A consumer that rebuilds state from change events alone cannot see an
item that was already in some state before its window and did not
change during it. Durations measured from the first event in a window
are truncated at the window's start.


## Problem

What must the stream carry so that consumers can build complete state
and honest durations?


## Alternatives

 1. **A full-state seed at the start of each window, and one frame
    event per snapshot** (`SnapshotObserved`: date, previous snapshot,
    elapsed time, whether a gap precedes it).
 2. Deltas only, and leave completeness to each consumer (each would
    need its own access to the raw snapshots).
 3. Emit the full state of every snapshot (ADR-024, alternative 2).


## Decision

**Every eventized window starts with one `ExcuseObserved` per item of
its first snapshot, and every snapshot emits a `SnapshotObserved`
frame.** A duration whose start precedes the seed is a lower bound;
the frame lets a consumer mark it as left-censored. How far before the
reported period a window starts is a run parameter.


## Consequences

 - A window is self-sufficient: replaying it from the start rebuilds
   the complete state.
 - The seed costs one full state per window, not per snapshot.
 - Consumers report censored durations as such; the stream gives them
   the means, the consumers own the reporting.


## Evidence

*Probe 2026-11-23*: a delta-only consumer over 48 hours saw 4,173
entities against ~5,600–6,100 items per snapshot; its longest
"blocked for" (46.1 h) equalled the window and meant only "at least
since the window started".


## Revisit when

 - consumers need durations longer than any practical window (then a
   periodic re-seed, or a state table published beside the stream).
