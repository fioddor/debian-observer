"""Strongly-typed payload models for Debian Observatory domain events.

Each event type in the system has a dedicated payload model that
encapsulates the domain data carried by the event. These payloads
are designed to be embedded inside ``EventEnvelope`` instances via
``EventEnvelope.from_payload()``.

All payload models:

* Are **frozen** Pydantic models
* Reference canonical domain models (``Package``, ``PackageVersion``,
  ``Release``, ``Migration``) via their primitive fields
* Include the ``Observation`` metadata that provides provenance
* Serialize cleanly to JSON (for messaging) and can be flattened
  to Parquet-compatible rows

Event types defined here:

- ``PackageObserved``          — a package entity was seen
- ``PackageVersionObserved``   — a specific version was observed
- ``ReleaseObserved``          — a release/suite was observed
- ``MigrationObserved``        — a migration fact was observed

Event types added by the dependency analytics release:

- ``DependencyObserved``      — one dependency clause of a binary
                                 package version was observed
- ``PopconObserved``          — popcon counters of a package were
                                 observed

Event types added by the migration-dynamics release (R3, C2):

- ``ExcuseObserved``          — an item's full state at the seed
- ``ExcuseEntered``           — an item appears in the next snapshot
- ``ExcuseLeft``              — an item left (ambiguous by design)
- ``ExcuseVerdictChanged``    — an item's verdict changed
- ``ExcuseBlockedByChanged`` — an item's blocked-by set changed
- ``ExcuseVersionChanged``    — an item's new-version changed
- ``SnapshotObserved``       — one snapshot's frame (pair data)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from debian_observer.common.types import MigrationStatus
from debian_observer.common.validators import (
    validate_debian_version,
    validate_package_name,
)

# ---------------------------------------------------------------------------
# Shared config
# ---------------------------------------------------------------------------

FrozenModelConfig = ConfigDict(
    frozen=True,
    extra="forbid",
    populate_by_name=True,
    str_strip_whitespace=True,
)


def _utc_now() -> datetime:
    """Return the current UTC datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Reusable: Observation metadata embedded in payloads
# ---------------------------------------------------------------------------


class ObservationMeta(BaseModel):
    """Subset of Observation metadata embedded in event payloads.

    Rather than embedding the full ``Observation`` model (which may
    contain large reference fields), event payloads carry a minimal
    observation metadata block that preserves essential provenance.

    Attributes:
        source: Data source identifier.
        observed_at: When the fact was observed by the pipeline.
        effective_at: When the fact became true in the domain.
        schema_version: Version of the schema used for normalization.
        ingestion_run_id: Ingestion run that produced this observation.
    """

    model_config = FrozenModelConfig

    source: str = Field(..., description="Data source identifier.")
    observed_at: datetime = Field(
        default_factory=_utc_now,
        description="When the fact was observed by the pipeline (UTC).",
    )
    effective_at: datetime | None = Field(
        default=None,
        description="When the fact became true in the domain (UTC).",
    )
    schema_version: int = Field(
        default=1,
        ge=1,
        description="Schema version used for normalization.",
    )
    ingestion_run_id: str | None = Field(
        default=None,
        description="Ingestion run that produced this observation.",
    )

    @field_validator("source", mode="before")
    @classmethod
    def _validate_source(cls, value: Any) -> Any:
        if not value or not isinstance(value, str):
            raise ValueError("source must be a non-empty string")
        return value

    @field_validator("observed_at", "effective_at")
    @classmethod
    def _ensure_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @model_validator(mode="before")
    @classmethod
    def _sync_timestamps(cls, data: Any) -> Any:
        """Ensure observed_at and effective_at default to the same timestamp."""
        if isinstance(data, dict):
            has_obs = data.get("observed_at") is not None
            has_eff = data.get("effective_at") is not None
            if not has_obs and not has_eff:
                now = _utc_now()
                data["observed_at"] = now
                data["effective_at"] = now
            elif has_obs and not has_eff:
                data["effective_at"] = data["observed_at"]
            elif has_eff and not has_obs:
                data["observed_at"] = data["effective_at"]
        return data

    @field_validator("effective_at")
    @classmethod
    def _effective_not_after_observed(cls, value: datetime, info: Any) -> datetime:
        observed = info.data.get("observed_at")
        if observed is not None and value > observed:
            raise ValueError(
                "effective_at cannot be later than observed_at: "
                f"effective={value.isoformat()}, observed={observed.isoformat()}"
            )
        return value


# ---------------------------------------------------------------------------
# 6.2 Specific event type: PackageObserved
# ---------------------------------------------------------------------------


class PackageObservedPayload(BaseModel):
    """Payload for ``PackageObserved`` events.

    Emitted when a ``Package`` entity is observed in a source dataset.

    Attributes:
        package_id: Canonical package identifier.
        name: Package name.
        source_package: Source package name (for binaries).
        section: Archive section.
        architecture: Target architecture.
        maintainer: Package maintainer.
        description: Short description.
        observation: Provenance metadata for this observation.
    """

    model_config = FrozenModelConfig

    package_id: str = Field(..., description="Canonical package identifier.")
    name: str = Field(..., description="Package name.")
    source_package: str | None = Field(
        default=None, description="Source package name (for binaries)."
    )
    section: str | None = Field(default=None, description="Archive section.")
    architecture: str | None = Field(default=None, description="Target architecture.")
    maintainer: str | None = Field(default=None, description="Package maintainer.")
    description: str | None = Field(default=None, description="Short description.")
    observation: ObservationMeta = Field(
        ..., description="Provenance metadata for this observation."
    )

    @field_validator("name", mode="before")
    @classmethod
    def _validate_name(cls, value: Any) -> Any:
        if value is None:
            raise ValueError("Package name is required")
        return validate_package_name(value)

    @field_validator("package_id", mode="before")
    @classmethod
    def _validate_package_id(cls, value: Any) -> Any:
        if not value or not isinstance(value, str):
            raise ValueError("package_id must be a non-empty string")
        return value


# ---------------------------------------------------------------------------
# 6.2 Specific event type: PackageVersionObserved
# ---------------------------------------------------------------------------


class PackageVersionObservedPayload(BaseModel):
    """Payload for ``PackageVersionObserved`` events.

    Emitted when a specific version of a package is observed in a
    distribution/suite.

    Attributes:
        package_id: Canonical package identifier.
        source_package: Source package this binary belongs to.
        version: Debian version string.
        distribution: Distribution family where observed.
        release: Debian release/pocket where observed.
        suite: Suite role where observed (oldstable, stable, testing).
        architecture: Architecture this version targets.
        component: Archive component.
        section: Archive section.
        priority: Package priority.
        observation: Provenance metadata.
    """

    model_config = FrozenModelConfig

    package_id: str = Field(..., description="Canonical package identifier.")
    source_package: str | None = Field(
        default=None, description="Source package this binary belongs to."
    )
    version: str = Field(..., description="Debian version string.")
    distribution: str | None = Field(
        default=None, description="Distribution family where observed."
    )
    release: str | None = Field(
        default=None,
        description="Debian release/pocket where observed (sid, bullseye, "
        "etc.).",
    )
    suite: str | None = Field(
        default=None,
        description="Suite role where observed (oldstable, stable, testing); "
        "None when only the release pocket is known.",
    )
    architecture: str | None = Field(
        default=None, description="Architecture this version targets."
    )
    component: str | None = Field(
        default=None, description="Archive component (main, contrib, non-free)."
    )
    section: str | None = Field(
        default=None, description="Archive section (devel, net, utils)."
    )
    priority: str | None = Field(default=None, description="Package priority.")
    observation: ObservationMeta = Field(
        ..., description="Provenance metadata for this observation."
    )

    @field_validator("version", mode="before")
    @classmethod
    def _validate_version(cls, value: Any) -> Any:
        if value is None:
            raise ValueError("Version is required")
        return validate_debian_version(value)

    @field_validator("package_id", mode="before")
    @classmethod
    def _validate_package_id(cls, value: Any) -> Any:
        if not value or not isinstance(value, str):
            raise ValueError("package_id must be a non-empty string")
        return value


# ---------------------------------------------------------------------------
# 6.2 Specific event type: ReleaseObserved
# ---------------------------------------------------------------------------


class ReleaseObservedPayload(BaseModel):
    """Payload for ``ReleaseObserved`` events.

    Emitted when a Debian release or suite is observed.

    Attributes:
        release_id: Canonical release identifier.
        name: Suite name.
        codename: Release codename.
        status: Release lifecycle status.
        version: Release version number.
        description: Human-readable description.
        observation: Provenance metadata.
    """

    model_config = FrozenModelConfig

    release_id: str = Field(..., description="Canonical release identifier.")
    name: str = Field(..., description="Suite name.")
    codename: str | None = Field(default=None, description="Release codename.")
    status: str | None = Field(default=None, description="Release lifecycle status.")
    version: str | None = Field(default=None, description="Release version number.")
    description: str | None = Field(
        default=None, description="Human-readable description."
    )
    observation: ObservationMeta = Field(
        ..., description="Provenance metadata for this observation."
    )

    @field_validator("release_id", "name", mode="before")
    @classmethod
    def _validate_required_str(cls, value: Any) -> Any:
        if not value or not isinstance(value, str):
            raise ValueError("Field must be a non-empty string")
        return value


# ---------------------------------------------------------------------------
# 6.2 Specific event type: MigrationObserved
# ---------------------------------------------------------------------------


class MigrationObservedPayload(BaseModel):
    """Payload for ``MigrationObserved`` events.

    Emitted when a migration-related fact is observed.

    This payload captures the minimal migration facts needed for
    historical analysis. The migration model is deliberately
    simplified: the full Britney state machine is a downstream
    question.

    Attributes:
        migration_id: Canonical migration identifier.
        source: Source suite.
        target: Target suite.
        package: Package name.
        version: Version being migrated.
        architecture: Architecture being migrated.
        status: Simplified migration status.
        observation: Provenance metadata.
    """

    model_config = FrozenModelConfig

    migration_id: str = Field(..., description="Canonical migration identifier.")
    source: str = Field(..., description="Source suite.")
    target: str = Field(..., description="Target suite.")
    package: str = Field(..., description="Package name.")
    version: str = Field(..., description="Version being migrated.")
    architecture: str | None = Field(
        default=None, description="Architecture being migrated."
    )
    status: MigrationStatus = Field(
        default=MigrationStatus.UNKNOWN,
        description="Simplified migration status.",
    )
    observation: ObservationMeta = Field(
        ..., description="Provenance metadata for this observation."
    )

    @field_validator("package", mode="before")
    @classmethod
    def _validate_package(cls, value: Any) -> Any:
        if value is None:
            raise ValueError("Package name is required")
        return validate_package_name(value)

    @field_validator("version", mode="before")
    @classmethod
    def _validate_version(cls, value: Any) -> Any:
        if value is None:
            raise ValueError("Version is required")
        return validate_debian_version(value)

    @field_validator("migration_id", "source", "target", mode="before")
    @classmethod
    def _validate_required_str(cls, value: Any) -> Any:
        if not value or not isinstance(value, str):
            raise ValueError("Field must be a non-empty string")
        return value


# ---------------------------------------------------------------------------
# Dependency analytics payloads
# ---------------------------------------------------------------------------


class DependencyObservedPayload(BaseModel):
    """Payload for ``DependencyObserved`` events.

    One dependency clause of a binary package version, as written
    in the source field. The eventizer observes: the clause
    travels verbatim (``condition``), with its position
    discriminator (``clause_index``), whether it holds alternatives
    (``is_complex``) and how many (``alternatives``). No resolution
    fields here: which package version the clause points at is a
    downstream, coordinate-dependent question. Field meanings live
    on the ``Field`` descriptions.
    """

    model_config = FrozenModelConfig

    observation: ObservationMeta = Field(
        ..., description="Provenance metadata."
    )
    distribution: str | None = Field(
        default=None, description="Distribution family where observed."
    )
    release: str | None = Field(
        default=None, description="Release/pocket where observed."
    )
    architecture: str | None = Field(
        default=None, description="Architecture where observed."
    )
    component: str | None = Field(
        default=None, description="Archive component of the depender."
    )
    package: str = Field(
        ..., description="Binary package name of the depender."
    )
    version: str = Field(..., description="Version of the depender.")
    kind: str = Field(
        ..., description="Relationship field: depends | pre-depends."
    )
    clause_index: int = Field(
        ...,
        description=(
            "Position discriminator of the clause within its field "
            "(uniqueness, not order)."
        ),
    )
    condition: str = Field(
        ...,
        description="The clause text, verbatim (ends trimmed).",
    )
    is_complex: bool = Field(
        ...,
        description="Whether the clause holds alternatives ('|').",
    )
    alternatives: int = Field(
        ...,
        description=(
            "How many alternatives the clause holds (1 if simple)."
        ),
    )


class PopconObservedPayload(BaseModel):
    """Payload for ``PopconObserved`` events.

    Popcon counters of one binary package name (no version, no
    coordinate). Used as an impact weight. Field meanings live on
    the ``Field`` descriptions.
    """

    model_config = FrozenModelConfig

    observation: ObservationMeta = Field(
        ..., description="Provenance metadata."
    )
    package: str = Field(..., description="Binary package name.")
    insts: int = Field(
        default=0, description="Number of installations."
    )
    vote: int = Field(default=0, description="Number of votes.")
    olde: int = Field(default=0, description="Number of old reports.")
    recent: int = Field(default=0, description="Number of recent reports.")
    nofiles: int = Field(
        default=0, description="Number of nofiles reports."
    )


# ---------------------------------------------------------------------------
# Migration-dynamics payloads (R3, C2 — docs/release3.project.v0.1.md §6.1)
# ---------------------------------------------------------------------------


class _ExcuseStatePayloadFields(BaseModel):
    """The compact ExcuseState as event payload fields (§5.1).

    ``blocked_by``/``migrate_after``/``reason`` are the state's lists;
    the verdict travels verbatim — its set is open (§4.1.1).
    """

    model_config = FrozenModelConfig

    item_name: str = Field(..., description="The item's name.")
    source: str | None = Field(default=None, description="Source package.")
    old_version: str | None = Field(
        default=None, description="Version in testing."
    )
    new_version: str | None = Field(
        default=None, description="Version in unstable."
    )
    verdict: str | None = Field(
        default=None,
        description="Migration-policy verdict, verbatim (open set).",
    )
    is_candidate: bool = Field(
        default=False, description="Is a migration candidate."
    )
    blocked_by: list[str] = Field(
        default_factory=list, description="Blocking item names."
    )
    migrate_after: list[str] = Field(
        default_factory=list, description="Items to migrate first."
    )
    reason: list[str] = Field(
        default_factory=list, description="britney's reasons."
    )


class _SnapshotFields(BaseModel):
    """The snapshot pair identifiers every excuse payload carries."""

    model_config = FrozenModelConfig

    snapshot: str = Field(..., description="The eventized snapshot id.")
    prev_snapshot: str | None = Field(
        default=None,
        description="The previous observed snapshot id (None at the seed).",
    )


class ExcuseObservedPayload(_SnapshotFields, _ExcuseStatePayloadFields):
    """Payload for ``ExcuseObserved``: an item's full state at the seed."""

    observation: ObservationMeta = Field(
        ..., description="Provenance metadata."
    )


class ExcuseEnteredPayload(_SnapshotFields, _ExcuseStatePayloadFields):
    """Payload for ``ExcuseEntered``: present in next, absent in prev."""

    observation: ObservationMeta = Field(
        ..., description="Provenance metadata."
    )


class ExcuseLeftPayload(_SnapshotFields, _ExcuseStatePayloadFields):
    """Payload for ``ExcuseLeft``: present in prev, absent in next.

    Carries the item's **last** state. Ambiguous on purpose: an item
    leaves when it migrates, is removed or is superseded — consumers
    must not read "left" as "migrated" (§6.1).
    """

    observation: ObservationMeta = Field(
        ..., description="Provenance metadata."
    )


class ExcuseVerdictChangedPayload(_SnapshotFields):
    """Payload for ``ExcuseVerdictChanged``: the verdict differs."""

    model_config = FrozenModelConfig

    observation: ObservationMeta = Field(
        ..., description="Provenance metadata."
    )
    item_name: str = Field(..., description="The item's name.")
    before: str | None = Field(..., description="Verdict in prev.")
    after: str | None = Field(..., description="Verdict in next.")


class ExcuseBlockedByChangedPayload(_SnapshotFields):
    """Payload for ``ExcuseBlockedByChanged``: the set differs.

    ``before``/``after`` are the compared sets, sorted — order is not
    a change (§9.3).
    """

    model_config = FrozenModelConfig

    observation: ObservationMeta = Field(
        ..., description="Provenance metadata."
    )
    item_name: str = Field(..., description="The item's name.")
    before: list[str] = Field(..., description="Blocked-by in prev.")
    after: list[str] = Field(..., description="Blocked-by in next.")


class ExcuseVersionChangedPayload(_SnapshotFields):
    """Payload for ``ExcuseVersionChanged``: new-version differs.

    Compares ``new_version`` only: ``old_version`` is the testing side
    and changes when the item migrates (§9.3).
    """

    model_config = FrozenModelConfig

    observation: ObservationMeta = Field(
        ..., description="Provenance metadata."
    )
    item_name: str = Field(..., description="The item's name.")
    before: str | None = Field(..., description="new-version in prev.")
    after: str | None = Field(..., description="new-version in next.")


class SnapshotObservedPayload(BaseModel):
    """Payload for ``SnapshotObserved``: one snapshot's frame (§6.1).

    Makes the pair explicit: a consumer knows its duration
    (``elapsed``) and whether a gap precedes it (``gap_before``)
    without inferring either. ``gap_before`` is None at the seed (no
    previous snapshot), False for a pair within the source's
    publication rhythm, True for one wider than it.
    """

    model_config = FrozenModelConfig

    observation: ObservationMeta = Field(
        ..., description="Provenance metadata."
    )
    snapshot: str = Field(..., description="The eventized snapshot id.")
    generated_date: str = Field(
        ..., description="The snapshot's generated-date (ISO 8601 UTC)."
    )
    prev_snapshot: str | None = Field(
        default=None, description="The previous observed snapshot id."
    )
    elapsed: int | None = Field(
        default=None,
        description="Seconds between the pair's generated-dates.",
    )
    gap_before: bool | None = Field(
        default=None, description="Does a publication gap precede it."
    )
    item_count: int = Field(
        default=0, ge=0, description="Items in this snapshot."
    )