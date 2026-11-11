# ADR-017 — Go for parallel graph workers

Status: **Proposed — decision frame; evidence pending**
(E4, spec §16.5, closes this on day 9)
Date: 2026-11-08


## Context

R2's parallel workload is deep-dependency closures over shared,
in-memory graphs (ADR-015: the lake is read in batches; the
graph is built once, ~15 MB CSR, spec §2.2). Two arms exist:

 - **Python baseline**: `multiprocessing` — processes, because
   CPython's GIL lets one thread execute bytecode at a time, so a
   thread pool would not produce a parallel BFS.
 - **Go**: goroutines in one process.

The naive framing — "goroutines share memory, Python processes
copy it" — is **measured false for this workload** (spec §2.2):
`fork()` copy-on-write already shares the graph across Python
children (**0.9 MB private-dirty per child** with CSR
`array.array`, *measured 2026-11-03*; ~15 MB graph; 4 GB machine
÷ 9.8 MB naive ≈ 400 hypothetical workers). Memory per worker is
therefore **not** the Go argument.

What remains:

 1. **raw traversal speed per core** — Go's compiled BFS/Tarjan
    vs CPython's interpreter;
 2. **deployability** — the 4 GB laptops (pavilion, pb) have no
    Python environment; a Go worker is a static binary
    (`CGO_ENABLED=0`) that copies over (spec §14).

This ADR is deliberately a **frame**: E4 (spec §16.5) measures
both arms (Python 1/2/4, Go 1/2/4/8 workers) and the measured
result decides.


## Problem

Which implementation is R2's **default** deep-dependency worker —
decided by measurement, not by language affinity?


## Alternatives

 1. **Go becomes default** if it is faster at equal workers or
    fits more workers into the same budget.
 2. **Python stays default** if the measured speed difference is
    noise and the deployability case is not worth a second
    toolchain (Go in the devcontainer and CI, spec D3).
 3. Ship both with no default: a default is exactly what the
    evidence owes us.


## Decision

**Frame (to be closed with E4 evidence, day 9):**

 1. The **Python `multiprocessing` worker is the baseline** and
    ships first (T6): it reuses the in-repo resolution library,
    the graph builder and the execution-record machinery, and it
    is the reference the Go arm must match (P6 file equality,
    order-insensitive; spec §15.5).
 2. The **Go worker is the experimental arm** (T8): same
    `job_id` contract (ADR-018), same consumer settings, same
    output paths (ADR-019), OpenTelemetry metrics with the same
    names (spec §13). Static binary, `CGO_ENABLED=0`.
 3. **E4 records** throughput (jobs/s), speed-up vs 1 worker,
    peak RSS total and per worker (as a *check* that CoW holds
    under load, not as the expected differentiator), CPU
    utilization — per arm and worker count (spec §16.5).
 4. **The verdict rule**: Go becomes the default worker if it is
    faster at equal workers **or** fits more workers into the
    same memory budget; otherwise Python stays default and this
    ADR records why. **Either result is publishable evidence.**
 5. Prerequisite (spec D3): before any Go install, the version,
    source (Debian package vs pinned official tarball) and
    location (devcontainer image, CI) are proposed to the owner
    and approved; Go is installed nowhere today (verified
    2026-11-05).


## Consequences (of framing, not yet of verdict)

 - The baseline is guaranteed to exist regardless of E4's
   outcome; R2's distributed-systems story never depends on the
   Go arm landing on time.
 - The Go toolchain decision (D3) is decoupled: it can be approved
   late without blocking T6.
 - ADR-020's cache sizing (E3) is measured on the Python arm;
   E4's comparison is on identical job sets.


## Evidence

 - Pending: E4 (spec §16.5), day 9. Pre-evidence in spec §2.2:
   the 0.9 MB/child CoW measurement that removed memory from the
   argument.


## Revisit conditions

 - This ADR is closed by E4, not revisited before it. After the
   verdict, a change of worker default reopens it with new
   measurements only.
