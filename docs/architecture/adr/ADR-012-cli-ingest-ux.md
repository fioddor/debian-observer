# ADR-012 — CLI ingest UX: one subcommand per source

Status: Accepted
Date: 2026-10-21


## Context

The `ingest` command grew as a single command with a `--source`
choice (`sources` | `mirror` | `udd`) and a `--url` flag whose meaning
changed per source: a package name, a suite, or a table name. New
sources are coming (a UDD `pg_dump` file source is planned), and each
new source added another meaning to `--url`.

`validate` and `normalize` took raw records from any stream
(`--input`, stdin) and stamped them with a `--source` name and
`--run-id`. But every raw artifact is written by the pipeline with a
manifest sidecar (`<artifact>.manifest.json`) that already records
the source, the run id and the checksums — the flags re-asked the
operator for data the pipeline had already persisted.


## Decision

**1. `ingest` becomes a command group with one subcommand per
source.** Each subcommand takes its selector as a *mandatory filter
flag* with its own name:

    debian-observer ingest sources --package <name>
    debian-observer ingest mirror  --suite <suite> [--component main]
                                     [--architecture amd64] [--limit N]
    debian-observer ingest udd     --table <table> [--limit N]

`--source` and `--url` are removed. Common pipeline-output flags
(`--raw-dir`, `--parquet-dir`, `--dataset-dir`) stay on every
subcommand; `--limit` (mirror/udd) keeps its whole-source default
with a stderr warning.

**2. Elements vs filters.** The *element* is the thing ingested — a
database, a mirror, a service instance. A *filter* restricts which
part of the element is taken. Elements have to be recognizable, and
there are no element *detectors* yet, so every source is born with
**mandatory** filter flags that pin the element down:

- `udd`: without filters the element is the whole database; a future
  detector could ingest everything it recognizes (tables, views,
  indexes…) and generate multiple entity types per element.
- `mirror`: without filters the element is the whole mirror; a
  future detector could iterate the components and suites it finds.
- `sources`: without filters the element is the service instance; a
  future detector could ingest all packages it finds.

When detectors exist, filter flags become optional and positional
arguments may name elements. The planned dump-file source fits the
same shape: `ingest udddump --file <dump> --table <table>` — the file
is the mandatory element selector until dumps are detectable.

**3. `validate`/`normalize` take raw artifacts as positional
arguments** (one or more), and read the source name and run id from
each artifact's manifest sidecar:

    debian-observer validate <artifact>.csv.xz [<artifact>.csv.xz …]
    debian-observer normalize <artifact>.csv.xz [<artifact>.csv.xz …] [--output …] [--invalid …]

`--source`, `--run-id` and `--input` are removed; ad-hoc stdin JSONL
input is removed with them (artifacts and their manifests are the
unit of ingestion, and loose records have no provenance to stamp).
An artifact without a manifest sidecar is a usage error with a clear
message, not a traceback.


## Consequences

Pros:

 - One meaning per flag name (`--package`, `--suite`, `--table`);
   `--url`'s triple meaning disappears.
 - New sources add a subcommand instead of overloading `--url`;
   the element/filter rule says exactly which flags a new source
   needs and which become optional later.
 - `validate`/`normalize` stop asking for data the pipeline already
   persisted (manifest sidecar); several artifacts can be processed
   in one run.
 - The CLI keeps a stable migration path: flags become filters
   (optional) when detectors arrive; positional elements may come
   after that.

Cons:

 - Breaking UX change: `--source`/`--url` invocations stop working
   (documented; no compatibility shim in R1).
 - Ad-hoc stdin JSONL validation/normalization is no longer
   supported; records must come from a pipeline artifact.
 - `--output data/raw` in the old `cli/__init__.py` docstring never
   existed and is removed.


## References

 - ADR-002 — raw data preservation (`.csv.xz` artifacts, manifests).
 - ADR-006 — no PostgreSQL in R1 (the dump-file source reads a file,
   not a database connection).