# ADR-013 — Dependency observation model

Spec: `docs/release2.project.v0.2.md` §5.1, §6
Status: Accepted
Date: 2026-11-08


## Context

R1 observes package versions. R2 studies the relationships
**between** packages: Debian Policy §7
(<https://www.debian.org/doc/debian-policy/ch-relationships.html>)
defines:
  - a relationship field as a comma-separated list of **clauses**,
  - each clause one or more **alternatives** separated by `|`,
  - each alternative `name[:archqual] [(op version)]`.

A relationship field is therefore **a conjunction of disjunctions**:

```text
field   = clause AND clause AND clause        -- all must hold
clause  = alt   OR  alt   OR  alt             -- any one suffices
alt     = name[:archqual] [(op version)]      -- a constrained target
```

Measured on trixie amd64+all (*measured 2026-11-04*, spec §5.1.2):
313,761 clauses, of which **6,929 carry an OR** (up to 21
alternatives) and **178,673 (57 %) carry a version constraint**.

The design-time probes (spec §4.3) showed the cost of simplifying:
Taking only the first alternative of each clause manufactures
**2 false `main`→`non-main` boundary crossings**, whose second
alternative lives inside `main` (`python3-tabnet`, `minigalaxy`).


## Problem

What is the unit of observation of a dependency, and what does an
observation carry — so that derived tables can answer reach,
concentration and cycle questions without silently losing either
the AND or the OR level, and without inventing facts the source
does not state (a dependency names a *package + constraint*,
never a concrete version)?


## Alternatives

 1. **One event per alternative-bearing clause**
    (all alternatives parsed, none privileged):
    The event carries the whole clause.
    Resolution (spec §5.1.2) happens in the prosumers, inside a
    defined region (a component list within a single coordinate).
 2. One event per clause (for example: first alternative only)
    Loses the OR level. Generates false region-frontier crossings.
 3. One event per field:
    - The event payload would need a nested clause list.
    - Every consumer re-parses.
 4. One event per resolved package-version → package-version edge:
    Resolution inside the eventizer bakes the coordinate's
    component list into the log. One eventization per component
    list, and the log stops being one observation serving every
    list (spec §6.2).


## Decision

### Observation (descriptive)
 1. **One `DependencyObserved` event per unresolved clause**
    A clause is the comma-separated element of a `depends`/
    `pre_depends` field.
 2. Add `kind` ∈ {`depends`, `pre-depends`}.
 3. Unparseable clauses become **invalid records with the cause**
    (parsing is total, spec §5.2); deprecated operators `<` and
    `>` (Policy §7.1) are invalid with cause `deprecated operator`,
    counted.
 4. One event type for simple and complex clauses, discriminated by
    `is_complex`: one subject, one schema, one parser (spec §6.1).

### Resolution (analytic)

Resoltion means selectinig one alternative.

For several purposes, almost-correct is enough.
Taking the first alternative works for `main` with scant errors.
This is good-enough to be cached.

Other use-cases will need to process all observations.

## Consequences

 - Nothing needs re-reading the raw dump. The log keeps every
   alternative: component frontiers, cycles (`hard` vs
   `potential`, spec §10.2) are all computable from the same
   eventization.
 - The parser is a pure function
   `parse_relationship_field(text) -> list[Clause]` with Debian
   Policy §7.1 as the cited standard (spec §5.2).
 - Resolution (coordinate- and component-list-dependent) lives
   **downstream**, in the prosumers: one eventization serves every
   component list, and the observation key stays stable across
   lists (spec §6.2).
 - `clause_index` read as "position means something" is a bug —
   unit-tested to stay a discriminator (spec §19.1).


## Evidence

 - Spec §2.1, §4.3, §5.1, §6 (*measured 2026-11-01/03/04*); the
   design-time probes and their commands are committed under
   `benchmarks/results/r2-design-probes/` (spec §16).
 - E1 (spec §16.2) re-measures the full eventization from the
   implementation and commits the counts.


## Revisit conditions

 - `Provides` resolution (future work, spec §23) adds an event
   type and moves the `virtual` unresolved-reason to a resolved
   edge — this ADR's clause model is unaffected.
 - Multiple dump dates (true time series) extend the provenance,
   not the clause model.
