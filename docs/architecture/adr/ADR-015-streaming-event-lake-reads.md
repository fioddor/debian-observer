# ADR-015 — Streaming reads of the event lake

Spec: `docs/release2.project.v0.4.md` §2.2, §9.2, §10.2
Status: Accepted
Date: 2026-11-08


## Context

The full `dependencies` eventization is **8,376,616 Depends +
28,607 Pre-Depends clauses ≈ 8.4 M events** across all
distributions (*measured 2026-11-01*, spec §2.1). The event log
is Parquet, ~189 MB.

R1's read paths, both materializing:

 - normalization: `read_dump_records` captures the whole
   `pg_restore` stdout **and** returns a list of record dicts,
   both alive at once.
 - prosumers: `prosumers/stream.py::FileSource.__iter__` does
   `pq.read_table()` → `.to_pandas()` → `iterrows()`, and
   `src/prosumers/consolidate.py` adds `records = list(source)` —
   a third full copy as Python objects.

Measured and extrapolated to the real 8.4 M-event lake
(*measured 2026-11-04*, spec §2.2):

| Read path              | Extrapolated to 8.4 M |
|:---------------------------------|------------:|
| R1 `FileSource` + `list(source)` | **19.3 GB** |
| batched (`iter_batches` + fold)  | **0.73 GB** |

The machine budget is 8 GB.


## Problem

Where does R2 actually need bounded reads — given that
measurement says normalization is *not* the wall (peak RSS is
flat at ~267 MB from 200 k to 8.4 M events), and the wall is
**19.3 GB** in the prosumer read of the lake, on 8 GB hardware?


## Alternatives

 1. **Stream everything** (dump read, event write, prosumer
    reads): treats a non-problem as a constraint and adds
    machinery (backpressure, incremental parsers) where
    measurement says none is needed.
 2. **Materialize and rely on swap/compression**: 19.3 GB does
    not run on the hardware; not an option.
 3. **Stream only where the volume lands: the prosumer read of
    the event lake.**
    Normalization stays the simple incremental design
    (row in → events out → Parquet writer, row groups of ~100 k).
    Peak RSS there is a *sanity check*
    (E1 regression  guard, spec §16.2),
    not a constraint under threat.


## Decision

 1. **Normalization needs no streaming machinery** (spec §9.2):
    the COPY block is read line-by-line from `pg_restore` stdout
    (`subprocess.Popen`, line iteration) — a *pipe iterator*, so
    the captured stdout does not sit in memory — and
    `read_dump_records` keeps its signature (returns the list
    built from the iterator) so R1 callers and tests are
    untouched. Rows go one at a a time through the eventizer and
    events go straight to the Parquet writer. No event list is
    built.
 2. **The prosumer read of the event lake is batched, and this is
    the one place streaming is mandatory** (spec §10.2): a batched
    source in the shared prosumer library
    (`prosumers/lake.py`) reads with `ParquetFile.iter_batches()`
    (or DuckDB, which streams natively), folding batch by batch,
    keeping only the accumulator for the coordinate in hand.
    R1's `FileSource` stays as-is for the small single-file cases
    it already serves.
 3. Prosumers may materialize **heavily filtered** outputs:
    the point is that the *lake* is never fully resident,
    not that nothing ever is.
 4. Peak RSS is recorded per run like every figure (E1, E2);
    E1 additionally reports it at 10 %, 50 %, 100 % of the table
    as the regression guard that it stays flat in the number of
    events.


## Consequences

 - The release's real volume problem (19.3 GB → 0.73 GB, a 26×
   difference between "does not run" and "runs comfortably") is
   solved exactly where it appers.
 - Graph prosumers that call `read_table`/`to_pandas`/
   `list(source)` on the **lake** fail the Definition of Done
   review (spec §18 functional, item 2); the small-file cases are
   unaffected.
 - A shared `lake.py` means every prosumer gets the batched read
   from one place — no per-prosumer re-implementation.
 - Python workers share the graph via `fork()`+CoW (0.9 MB/child
   measured, spec §2.2).


## Evidence

 - Spec §2.2 tables (*measured 2026-11-03/04*): flat 259→267 MB
   eventization RSS across 200 k→8.4 M events; 4,707 MB vs 178 MB
   on the 2 M-event lake probe; the probes' commands committed
   under `benchmarks/results/r2-design-probes/`.
 - E1 and E2 (spec §16.2, §16.3) re-measure both read paths from
   the implementation: full-lake batched prosumer run, and the
   R1 materializing path on a reduced lake (extrapolated, since
   19.3 GB does not run on this hardware).


## Revisit conditions

 - If a future lake grows two orders of magnitude (multiple dump
   dates × all tables, spec §23), per-coordinate partitioned reads
   may need predicate pushdown into the Parquet reader — decided
   then, with the benchmark of that release.
