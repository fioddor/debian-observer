# Data Model

Intent and invariants of the canonical data. Field-by-field schemas live
in the code (`src/debian_observer/domain/models.py`); this page records
what the code does not make obvious.

## Entities

| Entity | Intent | Key relations |
|---|---|---|
| `Package` | A Debian source or binary package. `package_id` is deterministic from source data (`name`, or `name:architecture` for binaries). | `source_package` → source `Package` |
| `PackageVersion` | An observed version of a package, with archive coordinates (`distribution`=family, `suite`=role, `release`=codename/pocket). | belongs to `Package`; observed via `ObservationMeta` |
| `Release` | A Debian release/suite lifecycle fact. | referenced by version coordinates |
| `Migration` | A package's movement between releases (the minimal model). | from/to `Release` |
| `Observation` (`ObservationMeta`) | Provenance: *when* seen (`observed_at`) vs *when true* (`effective_at`), source, run id. | embedded in every event payload |

## Invariants

- **Field semantics are fixed**: `distribution` = archive family (`debian`/`ubuntu`); `suite` = suite *role* only (stable, testing, unstable…); codenames and pockets (bullseye, sid-security…) go to `release`. Sources emit `suite=None` when the source has no role.
- **`observed_at` ≠ `effective_at`**: timestamps differ by design; per-row wall-clock variance is noise, not signal.
- **All domain models are frozen** Pydantic models (`extra="forbid"`): immutable, hashable, equality on value.
- **Serialization is writer-owned** (ADR-011): domain models serialize to JSON; Parquet layout is decided by the storage layer, not the domain.

## Terminology

This heading is the glossary
(the repository has no separate glossary file).

| Term | Intent | Where it lives |
|---|---|---|
| **ingestor** | Acquires a raw artifact from a source (`ingest`). | `ingestion` |
| **eventizer** | The source-specific function that, when the user runs `normalize`, reads a raw artifact and emits domain events (it *observes* the source). A source owns a collection of ingestors and a collection of eventizers; `normalize <source> --eventizer NAME` selects one (default `package-versions`). | `normalization/eventizers/` (ADR-014) |
| **prosumer** | Consumes events/records and produces derived records or tables. | `prosumers/` |

The coordinate of a derived graph is
`(distribution, release, architecture, components)` — `components`
is a list on equal footing with `architecture` (ADR-021).