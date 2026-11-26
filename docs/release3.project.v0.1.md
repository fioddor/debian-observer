# Migration Dynamics. Release R3 — the platform part.

> Turn britney's published package migration state into a replayable stream of
> migration events, and rebuilding its history over a work queue that spans
> across machines.

Status: Approved. Decisions still open for the owner are listed in
Open Decisions (§24).

Base line: Debian Observer v1.1.1
R1 and the platform part of R2:

  - `docs/release1.project.v0.4.md`,
  - `docs/release2.project.v0.4.md`,
  - `docs/design/*`,
  - `docs/architecture/adr/ADR-001..019`

Where this document says "the platform already provides", the claim refers to
this base line.

Scope of this document: the **platform** side of R3

  - acquisition,
  - eventization,
  - the migration stream and
  - the distributed parse

The analytics that consume the stream (blocking dynamics, blast radius) live in
a plug-in and are specified in that plug-in's repository.

Nothing here depends on a plug-in
([ADR-022][ADR-022]: the arrow points from plug-ins to the platform, never back).

Measured claims carry their evidence.
Numbers labelled *(probe 2026-11-23)* come from a design-time spike against the
public service. They must be re-run and committed with the implementation under
`benchmarks/results/` (Benchmarked Performance, §16).
A result that disagrees with a probe is reported.


## Table of Contents

- **Overview**
  - [1. Main **Goals** — domain purpose, release scope, technical features.](#1-main-goals)
  - [2. Technical **Challenges** — cost, burstiness, gaps, memory.](#2-technical-challenges)
  - [3. Engineering **Analysis** — related work, scope, repository, ADRs.](#3-engineering-analysis)

- **Data Models**
  - [4. Data Sources & Datasets](#4-data-sources--datasets)
  - [5. Data Model: snapshots and excuse states](#5-data-model)
  - [6. Event Model: excuse events](#6-event-model)

- **Automated Process**
  - [7. Architecture](#7-architecture)
  - [8. Ingestion: the `britney` source](#8-ingestion-the-britney-source)
  - [9. Normalization: the pair eventizer](#9-normalization-the-pair-eventizer)
  - [10. Analytical Storage](#10-analytical-storage)
  - [11. Messaging: the migration stream and the parse work queue](#11-messaging)
  - [12. CLI](#12-cli)
  - [13. Observability](#13-observability)

- **Product Quality Specifications**
  - [14. Deployment](#14-deployment)
  - [15. Reliability Properties](#15-reliability-properties)
  - [16. Benchmarked Performance](#16-benchmarked-performance)
  - [17. Security & Supply Chain](#17-security--supply-chain)

- **Product Quality Checks**
  - [18. Definition of Done](#18-definition-of-done)
  - [19. Testing Strategy](#19-testing-strategy)
  - [20. Demonstration Scenario](#20-demonstration-scenario)

- **Development Plan**
  - [21. Execution Plan](#21-execution-plan)
  - [22. Stretch Goals](#22-stretch-goals)
  - [23. Future Work](#23-future-work)
  - [24. Open Decisions for the Owner](#24-open-decisions-for-the-owner)


---
## 1. Main Goals

### 1.1. Domain and product purpose

Debian packages reach _testing_ by migrating from _unstable_.
*Britney*, the release team's migration tool, decides each migration and
publishes its reasons as **excuses** per item:

  - a verdict,
  - the versions involved,
  - and which other items block it (`blocked-by`) or
    must migrate first (`migrate-after`).

Britney publishes a **state**, regenerated several times a day, not a history.

The platform turns the sequence of published states into *migration events*,
so that consumers can answer, over time:

> - Which packages block the migration of others?
> - For how long? and
> - How far do those blocks reach over the ecosystem?

Britney only answers "why does this item not migrate now".
It is not an analytical store over time — that is what the event stream adds.

### 1.2. Release scope (platform)

 1. a `britney` source:
    - acquisition from the live endpoint and from the public archive,
    - raw preservation with provenance ([ADR-002][ADR-002]),
    - gaps recorded as data.
 2. a **pair eventizer**:
    - consecutive observed snapshots → canonical excuse events,
      starting from a full-state seed ([ADR-024][ADR-024], [ADR-028][ADR-028]).
 3. deterministic identity for these derived events ([ADR-025][ADR-025]).
 4. a dedicated migration stream ([ADR-026][ADR-026]).
 5. a **parse work queue**: the expensive snapshot parse distributed over
    workers on any machine that reaches the NATS server ([ADR-027][ADR-027]).
 6. benchmarks of the above (§16).

### 1.3. Technical product features

 - `debian-observer ingest britney`:
    an archive window or live following as a CLI call or as a long-running
    service reading the same window from configuration.
 - `debian-observer normalize britney --eventizer excuses`:
   seed plus deltas, idempotent and resumable.
 - publication to the migration stream with deduplication.
 - a parse worker executable, runnable on several machines at once.


---
## 2. Technical Challenges

### 2.1. Cost per snapshot *(probe 2026-11-23)*

| Quantity                               | Value                         |
|:---------------------------------------|:------------------------------|
| `excuses.yaml` size                    | ~3.1–3.4 MB gzip, ~49 MB YAML |
| items per snapshot                     | 5,569–6,121                   |
| parse (`yaml.CSafeLoader`, local file) | 10.1 s mean (9.3–11.0)        |
| peak RSS of a parsing process          | ~900 MB                       |
| 2026 archive                           | 7,666 YAML snapshots          |

**Live following needs no parallelism; rebuilding the history does.**
The parse dominates. One archive year is ~21.5 h of serial parsing.
Following the live endpoint is ~10 s of work per britney run.

### 2.2. Burstiness *(probe 2026-11-23, 48 h, 46 pairs)*

 - 8,344 events: mean 181 per hour;
 - 4–5 peaks a day, up to 2,607 events in one pair, troughs of 3–50.

Consumers must absorb bursts two orders of magnitude above the mean:
backpressure and consumer lag are measurable here.

### 2.3. Gaps *(probe 2026-11-23)*

The archive is not continuous: some hours have no snapshot. A snapshot that was
never published is not a lost one. The unit of reasoning is the **pair of
observed snapshots**, of whatever duration (§9.3).

### 2.4. Memory and parallel speed-up *(probe 2026-11-23)*

 - ~900 MB per parsing process: a 4 GB machine fits about three workers.
 - on a shared 4-CPU host the speed-up saturated at ~2× with 4 workers,
   equally for a process pool and for queue workers. The cause (CPU contention vs
   memory) is not yet separated (E5, §16).

### 2.5. Restrictions

 - The source is a *public* service: retrieval must be **polite** (§4.1.3).
 - The source is external YAML: safe loading only (§17).


---
## 3. Engineering Analysis

### 3.1. Related work and positioning

| Tool                         | What it gives                      | Limit for the question |
|:-----------------------------|:-----------------------------------|:-----------------------|
| britney excuses (HTML/YAML)  | why an item does not migrate *now* | a state, not a history; no aggregation |
| britney `update_output` logs | what each run accepted             | migrations, not blocks; text |
| UDD `migration_excuses`      | the current excuses as SQL         | current only |
| UDD `testing_autoremovals`   | RC-bug driven removal set          | current only |
| tracker.debian.org           | per-package view                   | per package |

Statements of fact about these tools must be verified against their current
documentation before publication
(britney: <https://release.debian.org/doc/britney/>).

### 3.2. Scope

In scope:
the unstable → testing excuses (`excuses.yaml`) and their archive for the
current year (older years only if their format holds, §4.1.2).

Out of scope:
  - the other excuses files
    - stable updates
    - backports
    - experimental pseudo-excuses
  - britney's input state (autopkgtest, piuparts, reproducibility)
  - the analytics over the stream (plug-in).

### 3.3. Repository structure: additions

Names are indicative; the module contracts decide.

```text
src/debian_observer/
├── ingestion/
│   └── britney.py              # archive index, live fetch, raw store, gaps
├── normalization/
│   ├── britney.py              # snapshot → compact state (the parse)
│   └── eventizers/
│       └── excuses.py          # the pair eventizer (seed + deltas)
├── messaging/
│   └── config.py               # + the migration stream, + the parse queue
└── cli/commands/
    ├── ingest.py               # + `ingest britney`
    └── normalize.py            # + `normalize britney`
```

plus a parse-worker executable and its crash-test script, modelled on
`scripts/crash-test-consumer.sh`.

### 3.4. Architecture Decision Records (new)

| ADR     | Title | Decision |
|:--------|:-----------------------------------------|:--|
| [ADR-024][ADR-024] | Snapshot-pair eventization               | A source whose truth is a periodic state is eventized from pairs of observed snapshots plus a full-state seed; [ADR-014][ADR-014]'s per-row contract gains a pair contract |
| [ADR-025][ADR-025] | Deterministic identity of derived events | `event_id = uuid5(namespace, logical key + payload)` for snapshot-derived events; re-eventizing a pair yields the same ids |
| [ADR-026][ADR-026] | The migration stream                     | A dedicated stream with its own retention; `debian-events`' limits do not fit |
| [ADR-027][ADR-027] | Distributing the snapshot parse          | The parse is a work queue across machines; the pair eventizer stays sequential; distribution applies where the unit of work is expensive |
| [ADR-028][ADR-028] | Seed, deltas and censoring               | The stream starts with a full-state seed and frames every pair; consumers can tell when a duration started before their seed |


---
## 4. Data Sources & Datasets

### 4.1. Primary source: britney excuses

#### 4.1.1. Live

`https://release.debian.org/britney/excuses.yaml.gz`.

Structure:
`{generated-date, sources: [{ item-name, source, old-version,
new-version, migration-policy-verdict, is-candidate, reason, excuses,
dependencies: {blocked-by: [...], migrate-after: [...]}, policy_info,
…} ]}`.

Observed *(probe 2026-11-23)*:

  - `item-name` unique per snapshot (0 duplicates over 47 snapshots);
  - `blocked-by` is a flat list of item names;
  - `dependencies` is absent in ~33 % of items;
  - verdicts seen: `PASS`, `PASS_HINTED`, `REJECTED_PERMANENTLY`,
    `REJECTED_BLOCKED_BY_ANOTHER_ITEM`, `REJECTED_WAITING_FOR_ANOTHER_ITEM`,
    `REJECTED_TEMPORARILY`, `REJECTED_CANNOT_DETERMINE_IF_PERMANENT`,
    `REJECTED_NEEDS_APPROVAL`. The set is **open**: an unknown verdict is
    kept verbatim and counted, never rejected.

This is britney's output, not a stable interface.
Format changes must be detected rather than silently absorbed.
Therefore, the parser keeps the fields it needs and records each snapshot's
top-level keys.

#### 4.1.2. Archive

`https://release.debian.org/britney/update_excuses/<year>/`

One `<timestamp>.yaml.gz` (and `.html.gz`) per britney run.

The timestamp suffix varies (`…0001`, `…0002`): **file names come from the
directory index**, don't rely on expectations.
Earlier years are published as yearly tarballs. It remains unverified and out
of the base scope, whether they carry the YAML excuses.

#### 4.1.3. Retrieval policy

No published bulk-retrieval policy was found; `robots.txt` of
release.debian.org does not exclude `/britney/`.

So, the platform sets and documents its own polite limits:

  - one request at a time, at least 1 s apart (configurable, but never 0).
  - every file downloaded **once** into the raw store.
    Re-runs read the store.
  - live following uses conditional requests (`If-Modified-Since`, `ETag`)
    and polls no more often than every 15 min.
  - the `User-Agent` `debian-observer/<version>
    (+https://github.com/fioddor/debian-observer)` (D6).
  - parse workers on other machines **never** fetch from the source (§7.3).

### 4.2. Dataset manifest

As R1 ([ADR-002][ADR-002]): every raw file is kept byte for byte with its

  - URL,
  - retrieval time,
  - HTTP validators,
  - size and
  - SHA-256.

Per window the manifest also records:

  - the index listing it was built from.
  - **gaps**: periods with no published snapshot, as data.
  - **failures**: listed files that could not be retrieved, distinct from gaps.
  - per snapshot: `generated-date`, item count, top-level keys.

### 4.3. Coordinates

  - A snapshot belongs to `(distribution = debian, from = unstable,
    to = testing, the testing codename at generated-date)`.
  - `item-name` forms seen: `src`, `src/arch`
  - Removal items `-src` appear in `update_output`.
  - The complete inventory of forms is counted on the archive window during
    implementation, not assumed.

---
## 5. Data Model

### 5.1. Entities

 - **Snapshot** (new):
   one britney run: `snapshot_id` (the archive file stem), `generated_date`,
   `url`, `sha256`, `item_count`, `top_level_keys`.
 - **ExcuseState** (new):
   one item in one snapshot, compact:
   `item_name`, `source`, `old_version`, `new_version`, `verdict`,
   `is_candidate`, `blocked_by` (list), `migrate_after` (list),
   `reason` (list).
 - **Migration** (existing):
   the minimal `Migration` entity stays.
   The existing `MigrationObserved` stays as it is (D8):
   Its natural producer is the accepted-migrations extension (§22.1).
   It is defined but nothing produces it today.

### 5.2. Invariants

 - `item_name` is unique within a snapshot.
   A duplicate is an invalid record with its cause, never a silent overwrite.
 - Snapshots are totally ordered by `generated_date`. A tie is an error.
 - A pair joins **consecutive observed** snapshots of the window.
   Its duration is the difference of their dates, never assumed.


---
## 6. Event Model

### 6.1. New event types

Envelope: the existing `EventEnvelope`- `event_id` is passedexplicitly
([ADR-025][ADR-025]; `EventEnvelope.from_payload(event_id=…)` already accepts it). 

| Event type                  | Meaning   | Payload (besides `snapshot`, `prev_snapshot`) |
|:--------------------------- |:--------------------------------------|:------------------|
| `ExcuseObserved`            | the full state of an item at the seed | its `ExcuseState` |
| `ExcuseEntered`             | present in `next`, absent in `prev`   | its `ExcuseState` |
| `ExcuseLeft`                | present in `prev`, absent in `next`   | its last `ExcuseState` |
| `ExcuseVerdictChanged`      | the verdict differs                   | `before`, `after` |
| `ExcuseBlockedByChanged`    | `blocked-by` differs, as a set        | `before`, `after` |
| `ExcuseMigrateAfterChanged` | the migrate-after set differs         | `before`, `after` |
| `ExcuseVersionChanged`      | `new-version` differs                 | `before`, `after` |
| `SnapshotObserved`          | one per snapshot: the frame           | `generated_date`, `prev_snapshot`, `elapsed`, `gap_before`, item count |

`SnapshotObserved` makes the frame explicit:

  - a consumer knows each pair's duration and whether a gap precedes it,
  - and freshness can be measured from it.

Subjects:

  - `debian.migration.excuse.<kind>` and
  - `debian.migration.snapshot.observed`

The existing `debian.migration.observed` is untouched.

`ExcuseLeft` is ambiguous on purpose:
An item leaves the excuses when it migrates **or** when it is removed or
superseded. Consumers must not read "left" as "migrated"
(stretch §22.1 adds the distinction).

### 6.2. Identity and idempotency ([ADR-025][ADR-025])

 - Logical key: `(event_type, entity, prev_snapshot, next_snapshot)`;
   for the seed `(ExcuseObserved, entity, seed_snapshot)`.
 - `event_id = uuid5(namespace, canonical(logical key + payload))`.
   A re-parse that changes the content (a parser fix) yields new ids,
   so old and new events stay distinguishable instead of colliding.
 - `Nats-Msg-Id = event_id` on publish.
 - Fixed vectors pin the derivation (§19.4).

**JetStream deduplication is an optimization, not the guarantee.**

Its window is time-bounded: a replay later than the window publishes every
event again.

Idempotency lives in the consumers, which deduplicate by `event_id`
(the fold-by-key pattern of [ADR-019][ADR-019]).

The probe confirmed both halves:

  - re-publishing inside the window added 0 messages
  - a killed consumer produced duplicates in its raw output that only the
    dedupe by id removed *(probe 2026-11-23)*.

### 6.3. Ordering

 - Within a pair: no order.
 - Across pairs:
   the events of a pair are published only after every event of the
   previous pair ([ADR-027][ADR-027]: the pair eventizer is sequential). Redeliveries may
   still arrive out of order: consumers order by the snapshot's
   `generated_date`, never by arrival.


---
## 7. Architecture

### 7.1. Pipeline

```text
 release.debian.org (live endpoint + archive index)
            │  polite fetch, once (§4.1.3)
            ▼
 ingest britney ── raw store + manifest (ADR-002), gaps as data
            │
            ▼  one job per snapshot
 parse work queue ── workers on one or more machines
            │  snapshot → compact ExcuseState table
            ▼
 pair eventizer (sequential, resumable)
            │  seed + deltas, deterministic ids
            ├──► event lake (Parquet, one directory per event type)
            └──► migration stream (Nats-Msg-Id = event_id)
                          │
                          ▼
                 consumers (plug-ins)
```

### 7.2. Where the cut is (ADR-027)

The parse is expensive, independent per snapshot and exposed to failures over a
long run => it is the unit worth distributing.

The diff compares two compact tables and must see snapshots in order:
it stays sequential and cheap (its cost is measured, §16 E2).
=> Distributing it would buy nothing and would break §6.3.

### 7.3. Raw data for remote workers

Remote workers must not fetch from the source (§4.1.3) and share no filesystem
with the ingestor. Two options were weighed (D2):

 - **NATS Object Store** (chosen):
   1. the ingestor puts each raw file in a bucket.
   2. a job names the object.
   3. the worker reads it from the same NATS it already uses and
      puts the compact state back. No new service, no shared filesystem.
      Raw files (~3 MB) exceed the default message size,
      so they travel as objects, not as messages.
 - an HTTP file server over the raw store:
   one more service to run and secure.

### 7.4. Live following

The service polls the live endpoint (§4.1.3). A new `generated-date` is one
more snapshot: the same raw store, one parse job, one pair.
Archive and live share every step after acquisition.


---
## 8. Ingestion: the `britney` source

`debian-observer ingest britney` ([ADR-012][ADR-012]: one subcommand per source).

Window selection via CLI options, or the same names in the service
configuration:

 - `--from <timestamp> --to <timestamp>` (archive, inclusive, UTC);
 - `--last <N>` (the N most recent snapshots in the index);
 - `--live` (follow; with `--from`, backfill first, then follow).

Acquisition rules:

 - the index is the truth: listed files are fetched; unlisted periods are gaps
   (manifest, §4.2).
 - a listed file that fails (HTTP error, truncated gzip) is retried with
   backoff, then recorded as **failed** (distinct from a gap).
 - files already in the store are skipped by SHA-256: re-runs are idempotent.
 - `--help` lists the implemented perimeter (CONTRIBUTING, CLI `--help`).

One execution record per run (`ExecutionRecord`): files fetched, skipped,
failed, gaps.


---
## 9. Normalization: the pair eventizer

### 9.1. CLI

`debian-observer normalize britney --eventizer excuses [window]`.
Registry entry `EVENTIZERS["britney"] = {"excuses": …}` ([ADR-014][ADR-014]).

### 9.2. The parse: snapshot → compact state

  - `yaml.load(…, Loader=yaml.CSafeLoader)`; the pure-Python `SafeLoader` only
     as a measured fallback where libyaml is missing (§17).
     Never `FullLoader` or `Loader`.
  - Output: one Parquet table per snapshot with the `ExcuseState` columns,
    plus the snapshot metadata.
  - This is the job the parse workers run (§11.2).

### 9.3. The pair contract ([ADR-024][ADR-024])

[ADR-014][ADR-014]'s eventizer is `eventize(row)`. A state source adds:

 - `eventize_seed(state, snapshot)`
   → one `ExcuseObserved` per item and a`SnapshotObserved`.
 - `eventize_pair(prev_state, next_state, prev, next)`
   → the deltas of §6.1 and a `SnapshotObserved`.

Rules:
  - comparisons are on the compact state.
  - `blocked-by` and `migrate-after` compare as **sets**.
  - `ExcuseVersionChanged` compares `new-version` only
    (`old-version` is the testing side and changes when the item migrates).
  - a pair spans consecutive **observed** snapshots, however long.
  - `SnapshotObserved.gap_before` marks it.
 - the eventizer records the last pair it published (its checkpoint):
   - a restart resumes after it, and
   - re-running a published pair yields the same ids (§6.2).

### 9.4. The seed ([ADR-028][ADR-028])

The seed is the first snapshot of the window.

A consumer that needs durations should start its window well before the
period it reports on. How far is a run parameter.

Durations that started before the seed must be taken as lower bounds,
and the frame events let a consumer say so.


---
## 10. Analytical Storage

 - raw store and manifest (§4.2);
 - compact snapshot states: one Parquet file per snapshot.
 - event lake:
   - one directory per event type,
   - one file per run,
   - readable with a plain `read_parquet` glob without `union_by_name`
   - no `hive_partitioning` ([ADR-016][ADR-016]) until needed (and approved).


---
## 11. Messaging

### 11.1. The migration stream ([ADR-026][ADR-026])

| Setting          | Value | Why |
|:-----------------|:------|:----|
| name             | `debian-migrations` | separate from `debian-events` |
| subjects         | `debian.migration.excuse.>`, `debian.migration.snapshot.>` | §6.1 |
| storage          | file   | durable replay |
| retention        | limits | consumers replay from the start |
| max age / bytes  | 400 days / 2 GB (D3), revisited once C3 measures the real volume (≈4,200 events a day × ~607 B ≈ 2.5 MB a day, *probe 2026-11-23*) | disk budget |
| duplicate window | hours, not minutes | covers re-publishing a window; the guarantee stays in the consumers (§6.2) |

### 11.2. The parse work queue ([ADR-027][ADR-027])

| Setting                      | Value             | Why |
|:-----------------------------|:------------------|:----|
| stream                       | `britney-parse`, work-queue retention | one job = one snapshot |
| consumer                     | one durable, pull            | workers on several machines share it |
| `ack_wait`                   | above the worst-case parse under contention, measured (e.g. 120 s) | the probe saw one parse exceed 20 s with 4 workers and be redelivered spuriously |
| in-progress acknowledgements | every ~10 s while parsing    | keeps a slow job, still redelivers a dead one |
| `max_ack_pending`            | the total worker slots       | backpressure |
| `max_deliver`                | 5, then a dead-letter record | a poisoned snapshot does not loop forever |

Worker writes are atomic and keyed by snapshot:
a redelivered job overwrites with identical content.

### 11.3. Why not an in-process pool *(probe 2026-11-23)*

With `multiprocessing.Pool(2)` and one child killed (`kill -9`),
the parent hung and 7 of 24 results were lost without notice.

The queue run with the same kill finished 24 of 24, identical.
Without the kill both took the same time (134.7 s vs 133.9 s).
A pool can be hardened
(detecting the dead child, retrying, persisting the pending list),
but that rebuilds a durable queue —
and the queue is also needed for workers on other machines.


---
## 12. CLI

 - `debian-observer ingest britney` (§8).
 - `debian-observer normalize britney --eventizer excuses` (§9).
 - the parse worker:
   - `--concurrency N`,
   - `--drain S` (integer seconds, 0 = run until signalled),
   - the NATS URL as a parameter
   final names at implementation.
 - `benchmark run` invocations for E1–E6 (the generic benchmark).

Analytics endpoints over the stream are mounted by the plug-ins that own them
([ADR-022][ADR-022]).


---
## 13. Observability

 - OpenTelemetry spans per fetch, per parse job (worker host as an attribute),
   per pair; traces cross machines.
 - Metrics: parse time per snapshot and host, redeliveries, queue
   depth, events per pair, consumer lag per durable, and **freshness**
   = now − `generated_date` of the last published snapshot.


---
## 14. Deployment

 - NATS: the platform's own deployment (compose or OpenTofu, CONTRIBUTING),
   reachable by the remote workers.
 - The live-following service: one more service in the deployment definitions.
 - Parse workers on other machines:
   - the platform wheel installed unchanged,
   - or the development image where Docker exists
   - the NATS URL as a parameter.
 - Exposing NATS beyond localhost is a security change (§17).


---
## 15. Reliability Properties

| Failure                       | Expected behaviour | Checked by |
|:------------------------------|:-------------------|:-----------|
| a parse worker dies mid-job   | the job is redelivered after `ack_wait`; final output complete and identical | E4 |
| a worker machine disappears   | its jobs are redelivered to the others | E4 |
| the pair eventizer dies       | the restart resumes after its checkpoint; republished pairs deduplicate | E4 |
| a consumer dies               | the durable resumes; its output deduplicated by `event_id` equals a clean run | E4 |
| the source misses a snapshot  | a gap is recorded; the pair spans it | unit + archive run |
| the source changes its format | the top-level key change is reported; the run fails loudly | unit |
| a poisoned snapshot           | `max_deliver`, then a dead-letter record; the run continues | unit + integration |


---
## 16. Benchmarked Performance

All through the generic `benchmark run`; results and their commands
under `benchmarks/results/`.

| Id | Question |
|:---|:---------|
| E1 | parse cost per snapshot (distribution over the window); compact-state size |
| E2 | pair-eventizer cost per pair (the assumption of [ADR-027][ADR-027] that the diff is cheap) |
| E3 | backfill scaling: 1, 2 and 4 workers on one machine, then across machines |
| E4 | the failure runs of §15 |
| E5 | the parallel ceiling: CPU contention vs memory |
| E6 | consumer throughput and lag under the worst measured burst |


---
## 17. Security & Supply Chain

 - **No new Python dependency**:
   `pyyaml`, `nats-py` and `python-debian` are already declared.
   Check that the installed PyYAML provides `CSafeLoader` in every target
   environment and record it.
 - External YAML:
   Safe loading only (§9.2).
 - External gzip: size checks before decompressing.
 - HTTPS with certificate verification for the source.
 - SHA-256 of every raw file in the manifest.
 - NATS reachable from other machines:
   - bind and authenticate deliberately
    - credentials per worker,
   - least privilege
     - pull on the parse queue,
     - read on the raw bucket,
     - write on the results bucket.


---
## 18. Definition of Done

### Functional:

  - an archive window ingested, parsed, eventized and published, with its gaps
    in the manifest;
  - live following publishes new snapshots.
  - the parse runs on workers on more than one machine.

### Reliability:
Every row of §15 demonstrated with a committed script and its output.

### Observability:
The metrics of §13 visible for an E3 run.

### Performance:
E1–E6 committed.

### Reproducibility:
a third person reproduces a run from its manifest
(raw files by URL and SHA-256).


---
## 19. Testing Strategy

 - Synthetic, deterministic fixtures in the battery (`tests/`)
 - real data and NATS-backed runs in scripts outside the battery (`scripts/`).

### 19.1. Unit
 - archive index parsing (suffixes other than `0001`, missing periods).
 - pair eventizer: entered, left, each change, set semantics of
   `blocked-by`, unknown verdicts, a pair spanning a gap, the seed.
 - parser:
   - a duplicate `item-name` becomes an invalid record
   - top-level key drift from the data source is detected and reported.

### 19.2. Integration
 - publish, replay twice (0 added), durable resume (against a NATS
   started by the script, never in the battery).

### 19.3. Validation against reality
 - spot checks of compact states against the same snapshot's
   `update_excuses.html` for a handful of items (query and result in
   the hand-off).

### 19.4. Contract
 - `event_id` derivation: fixed vectors.

### 19.5. Failure
 - crash-test scripts for
   - the parse worker,
   - the pair eventizer and
   - a consumer (§15).


---
## 20. Demonstration Scenario

 1. Ingest one week of the archive; show the manifest with its gaps.
 2. Start parse workers on two machines; kill one mid-job; show the
    redelivery and the identical result.
 3. Eventize and publish; publish again; show 0 added.
 4. Attach a consumer later; it replays from the start without touching
    the producer.
 5. Switch to live following; show freshness and consumer lag after the
    next burst.


---
## 21. Execution Plan

One task = one feature branch, following CONTRIBUTING (R/G/B links on `tdd`).
each task closes with a hand-off and a review before the next builds on it.

| Task | Content | Depends on |
|:---|:--|:--|
| C0 | this document and ADR-024…028 | owner review |
| C1 | `ingest britney`: index, fetch, raw store, manifest, gaps, live polling | C0 |
| C2 | the parse and the pair eventizer: seed, checkpoint, event lake | C1 |
| C3 | the migration stream, publication, replay and consumer crash tests | C2 |
| C4 | the parse work queue, remote workers, E1–E5 | C3 |

The consumers (blocking dynamics, blast radius) and E6 follow in the plug-in's
own plan.


---
## 22. Stretch Goals

### 22.1. Accepted migrations from `update_output`
Parse britney's accepted lines so that "migrated" can be told from
"removed or superseded" behind `ExcuseLeft`.


---
## 23. Future Work

 - Service levels on freshness and consumer lag; the workers as an
   operated workload.
 - Older archive years, if their format holds.
 - Other excuses files (stable updates, backports).


---
## 24. Open Decisions for the Owner

None open.

### Settled (recorded so they are not re-opened)
 - The analytics over the stream live in a plug-in.
 - The window is a parameter: CLI options or service configuration.
 - D2: remote workers get the raw files through the NATS Object Store
   (§7.3). Revisit if C4's measurements show it does not hold.
 - D3: `debian-migrations` keeps 400 days / 2 GB, revisited once C3
   measures the real volume (§11.1).
 - D5: ADR numbering continues at ADR-024.
 - D6: the `User-Agent` is `debian-observer/<version>
   (+https://github.com/fioddor/debian-observer)` (§4.1.3).
 - D8: the existing `MigrationObserved` stays as it is (§5.1).


[ADR-002]: architecture/adr/ADR-002-raw-data-preservation.md
[ADR-012]: architecture/adr/ADR-012-cli-ingest-ux.md
[ADR-014]: architecture/adr/ADR-014-eventizers-per-source.md
[ADR-016]: architecture/adr/ADR-016-no-graph-database-no-hive.md
[ADR-019]: architecture/adr/ADR-019-idempotency-deterministic-output-keys.md
[ADR-022]: https://ghostell.local/gitea/fioddor-dev/dopi-pdda/src/branch/tdd/docs/release2.project.v0.5.md
[ADR-024]: architecture/adr/ADR-024-snapshot-pair-eventization.md
[ADR-025]: architecture/adr/ADR-025-deterministic-identity-of-derived-events.md
[ADR-026]: architecture/adr/ADR-026-the-migration-stream.md
[ADR-027]: architecture/adr/ADR-027-distributing-the-snapshot-parse.md
[ADR-028]: architecture/adr/ADR-028-seed-deltas-and-censoring.md
