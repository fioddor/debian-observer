# ADR-024 — Snapshot-pair eventization

Spec: `docs/release3.project.v0.1.md` §6.1, §9.3
Status: Accepted
Date: 2026-11-23

Extends ADR-014 (eventizers per source).


## Context

ADR-014 defines an eventizer as `eventize(row)`: every row of a source
table becomes events on its own. That fits sources whose rows ARE the
observations (a dump's packages, dependencies, popcon).

britney's excuses are different: each publication is the **complete
state** of the unstable → testing migration at one instant. A single
state says what is blocked now; only two consecutive states say what
changed — an item entered, left, changed verdict, gained or lost a
blocker.


## Problem

How does a state source become events without inventing history it
does not have, and without one eventizer per row losing the context of
the previous state?


## Alternatives

 1. **Pairs of observed snapshots plus a full-state seed.** The
    eventizer compares consecutive observed snapshots and emits the
    differences; the first snapshot of a window is emitted in full.
 2. Per-row eventization of every snapshot (one event per item per
    snapshot). Correct but redundant: ~6,000 events per snapshot when a
    quiet hour changes a handful (*probe 2026-11-23*: 5 verdict changes
    and 1 blocker change between two consecutive hours), and every
    consumer has to diff again.
 3. Deltas only, no seed. Consumers never see items that were already
    blocked and did not change in their window (ADR-028).


## Decision

**A state source is eventized from pairs of consecutive observed
snapshots, starting from a full-state seed.** The eventizer contract
gains a pair form beside ADR-014's row form:

 - `eventize_seed(state, snapshot)` — the full state of every item and
   a frame event;
 - `eventize_pair(prev_state, next_state, prev, next)` — the
   differences and a frame event.

A pair spans consecutive **observed** snapshots, however far apart: a
period with no published snapshot is a gap, recorded and framed, never
an error and never an assumed step.


## Consequences

 - The eventizer is stateful across snapshots and therefore sequential
   (ADR-027 keeps it so); it records its last published pair and
   resumes after it.
 - Set-valued fields (`blocked-by`, `migrate-after`) compare as sets.
 - ADR-014's registry keeps working: the pair eventizer is one more
   entry for the `britney` source.


## Evidence

*Probe 2026-11-23* over 47 archive snapshots (46 pairs): 8,344 events
in 48 hours, bursty (up to 2,607 in one pair); two periods without a
published snapshot in the window.


## Revisit when

 - the source starts publishing its own change log;
 - a second state source appears whose pairs need a different frame.
