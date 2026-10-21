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
| `Migration` | A package's movement between releases (minimal R1 model). | from/to `Release` |
| `Observation` (`ObservationMeta`) | Provenance: *when* seen (`observed_at`) vs *when true* (`effective_at`), source, run id. | embedded in every event payload |

## Invariants

- **Field semantics are fixed**: `distribution` = archive family (`debian`/`ubuntu`); `suite` = suite *role* only (stable, testing, unstable…); codenames and pockets (bullseye, sid-security…) go to `release`. Sources emit `suite=None` when the source has no role.
- **`observed_at` ≠ `effective_at`**: timestamps differ by design; per-row wall-clock variance is noise, not signal.
- **All domain models are frozen** Pydantic models (`extra="forbid"`): immutable, hashable, equality on value.
- **Serialization is writer-owned** (ADR-011): domain models serialize to JSON; Parquet layout is decided by the storage layer, not the domain.
