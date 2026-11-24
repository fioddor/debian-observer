# ADR-014 — Eventizers per source

Spec: `docs/release2.project.v0.4.md` §8, §9
Status: Accepted
Date: 2026-11-08


## Context

R1's `normalize udddump ARTIFACT --table TABLE` maps one fixed set
of columns (`packages`) to one event type
(`PackageVersionObserved`): the field mapping is hard-wired in
the command.

R2 adds two more eventizations of the **same raw artifact**:
  - `dependencies` (table `packages`, payload `DependencyObserved`)
  - and `popcon` (table `popcon`, payload `PopconObserved`).

The same dump feeds all three, and a future `autoremovals`
eventizer (stretch, spec §22.1) would follow.

R1 has no `udddump` *source class*: `ingest udddump` stamps the
source name in the manifest, and normalization reads the columns.
Multiple eventizations of one source cannot live in a fixed
command mapping.


## Problem

Where do the eventizations of one source live, and how does the
operator pick one?
(the artifact is the unit of provenance, ADR-002/012)


## Alternatives

 1. **A registry of eventizers per source** keyed by source name
    (`normalization/eventizers/__init__.py:
    EVENTIZERS["udddump"] = {"package-versions": …,
    "dependencies": …, "popcon": …}`), the CLI selects with
    `normalize udddump … --eventizer NAME`.
 2. One subcommand per eventization (`normalize-udddump-deps …`):
    The CLI surface grows linearly with eventizations, and each
    verb re-implements artifact reading, shape gating and output.
 3. One multi-purpose eventizer emitting all types at once:
    Easy, but not flexible.


## Decision

 1. **A source owns a registry of eventizers.** `EVENTIZERS` maps
    the source name to `{eventizer-name: eventizer}`. Ingestors
    stay where R1 has them (`ingestion`); only normalization gains
    the indirection (spec §8).
 2. Each eventizer is an object with `name`, `event_type`,
    `shape` (a `TableShape`) and
    `eventize(row) -> Iterator[EventEnvelope | InvalidRecord]`.
 3. `normalize udddump ARTIFACT --table TABLE
    [--eventizer package-versions|dependencies|popcon]`; the
    default is `package-versions` (R1 behaviour, unchanged output
    except random-by-design fields).
 4. **Shape gating uses the selected eventizer's shape**
    R1's `match_table_shape` cannot discriminate
    (the `dependencies` shape is a superset of `packages`).
    `assert_supported_table_shape(columns, *, table, shape=None)`.
    `shape=None` keeps R1 behaviour,
    the CLI passes `eventizer.shape` (spec §9.1).
    A mismatch fails fast naming the eventizer, its required
    columns and the eventizers the table does match (CONTRIBUTING
    `--help` perimeter rule).
 5. `--output` (JSONL) stays; `--events-dir DIR` writes the Parquet
    event log; at least one of the two is required for non-R1
    eventizers.


## Consequences

 - R1 behaviour moves to the `package-versions` eventizer with
   unchanged output. R1 tests stay green (spec §21 day 2).
 - A new eventization is one registry entry + one module under
   `normalization/eventizers/` — no CLI change (the `autoremovals`
   stretch goal fits without touching the command).
 - The `eventizers` subpackage lives under `normalization`
   (not a top-level package). The `normalization` contract in
   `module-contracts.md` is extended.
 - Per-event-type Parquet schemas (spec §9.1, ADR-011
   writer-owned serialization) are selected by the writer from
   the eventizer's `event_type`.


## Evidence

 - Spec §8, §9.1; R1 baseline `docs/release1.project.v0.4.md`.
 - E1 (spec §16.2) measures the `dependencies` eventization on
   the full table; counts re-measured in T3 (spec §2.1).


## Revisit conditions

 - A second *source class* (e.g. dose3 output ingestion, spec
   §23) re-uses the registry keying as-is. Only a real second
   source class can tell whether the key needs to be a richer
   object than the source name.
