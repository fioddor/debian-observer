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

**2. The ingestion scope: sources and filters.** The *ingestion
scope* consists of a data source and its filters — the source is
the thing ingested (a database, a mirror, a service instance), and
a *filter* restricts which part of the source is taken. Entities
within a source have to be recognizable/detectable, and there are no
*detectors* yet, so every source is born with **mandatory** filter
flags that pin the scope down:

- `udd`: without filters the scope is the whole database; a future
  detector could ingest everything it recognizes (tables, views,
  indexes…) and generate multiple entity types per source.
- `mirror`: without filters the scope is the whole mirror; a
  future detector could iterate the components and suites it finds.
- `sources`: without filters the scope is the service instance; a
  future detector could ingest all packages it finds.

When detectors exist, filter flags become optional and positional
arguments may name the entities. The planned dump-file source fits
the same shape: `ingest udddump --file <dump> --table <table>` — the
file is the mandatory scope selector until dumps are detectable.

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
   the source/filter rule says exactly which flags a new source
   needs and which become optional later.
 - `validate`/`normalize` stop asking for data the pipeline already
   persisted (manifest sidecar); several artifacts can be processed
   in one run.
 - The CLI keeps a stable migration path: flags become filters
   (optional) when detectors arrive; positional entity arguments
   may come after that.

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


## Amendment (2026-10-21): the dump-file source — startup gate, one table

`ingest udddump --file <dump> --table <table> [--limit N]` now exists
in the shape planned above. Two follow-up decisions:

**α — validate the table at startup; an inspection flag comes later.**
Every `udddump` run first reads the dump's table of contents
(`pg_restore --list`, read-only) and fails fast when `--table` is
not a restorable table — the usage error lists the dump's tables
instead of raising raw `pg_restore` stderr or a traceback. A dedicated
inspection flag (`--list-tables`) is deliberately deferred to a
later MVC: the startup gate covers typos and unknown-table cases
first.

**β — one table per run.** `--table` is a single name; each run
restores exactly one table into its own raw artifact + manifest.
When many tables are needed, a `--tables` variant will iterate them
one at a time — one artifact + manifest per table, never one mixed
artifact.

The file path is a **host path**. `pg_restore` runs on the host
(plain PATH dependency — install `postgresql-client` if absent;
no container indirection, no live UDD instance involved). Rows
stream from `pg_restore --data-only --table`: no database is
created or restored (ADR-006).

**Correction (2026-10-22):** the first implementation of this
amendment routed `pg_restore` through `docker exec` into the local
`udd-postgresql` container (`UDDDUMP_CONTAINER`/`UDDDUMP_USER`). That
coupled the ingest CLI to a deployment artifact of the UDD sandbox
— the container has no reason to exist for reading a dump file.
The runner is host-`pg_restore` only.

**Provider (2026-10-22):** the images ship the dependency.
`postgresql-client` is installed in the dev and deploy images —
containers own their dependencies; the host stays untouched. The
deploy base is pinned `python:3.11-slim-trixie`: the floating
`3.11-slim` tag is non-deterministic (a cached bookworm-era image
ships `pg_restore` 15, which cannot read the dump — its
custom-format header pins `vmaj` 16, and `pg_restore` rejects
newer-major dumps); trixie ships `postgresql-client` 17 — verified
reading the real 2.9 GB dump's TOC in-container.

**Perimeter gate (2026-10-22, evening):** the startup gate does not
stop at the TOC. It peeks the table's COPY header — one stdout line of
`pg_restore --data-only --table`, no data restored — and matches it
against `SUPPORTED_TABLE_SHAPES`, a registry: new shapes are data,
not gate code (today it carries one entry, `packages`, requiring
`version` plus any of `package`/`source` — mirroring the validator;
the real `ci` table carries `source`+`version` and ingests fine). The transformer is the backstop: a record
that slips past validation and fails canonical transformation is
skipped with a warning naming the cause — never a traceback. `--help`
states the perimeter (CONTRIBUTING, "CLI `--help`" rule).
