"""Canonical domain models for Debian Observatory Release 1.

This module defines the core domain entities:

- ``Package``         — a Debian source or binary package
- ``PackageVersion``  — an observed version of a package
- ``Release``         — a Debian release or suite
- ``Migration``       — a simplified migration fact (minimal R1 model)
- ``Observation``     — metadata about when/where an entity was observed

All models are designed to be:

* **Immutable**      — frozen Pydantic models (or frozen dataclasses)
* **Validatable**    — field validators enforce domain constraints
* **Serializable**   — JSON via ``model_dump_json()``, Parquet via
  ``to_arrow_record()`` / ``to_parquet_row()`` helpers
* **Comparable**     — hashable and equality-tested on value

Design notes
------------

* R1 deliberately avoids premature abstraction. Models follow the
  semantics of Debian public data rather than inventing generic
  constructs.
* ``observed_at`` vs ``effective_at``: a fact may have happened at
  one point in time and been retrieved later. ``effective_at`` is the
  timestamp the fact *applied in the domain*; ``observed_at`` is when
  the pipeline *saw* it.
* Version strings are validated against Debian Policy syntax but
  the validator is intentionally permissive enough to handle real-world
  edge cases found in historic data.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from debian_observer.common.types import DebianVersionString, MigrationStatus, ReleaseStatus
from debian_observer.common.validators import (
    validate_debian_version,
    validate_package_name,
)

# ---------------------------------------------------------------------------
# Shared configuration
# ---------------------------------------------------------------------------

# All domain models use this config. ``frozen=True`` enforces immutability
# at runtime: instances cannot be mutated after creation and are hashable.
FrozenModelConfig = ConfigDict(
    frozen=True,
    extra="forbid",           # Reject unexpected fields (strict contracts)
    populate_by_name=True,    # Allow construction by alias
    str_strip_whitespace=True,
    validate_assignment=False, # Not needed when frozen=True
)


def _utc_now() -> datetime:
    """Return the current UTC datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# 5.1.1 Package
# ---------------------------------------------------------------------------

class Package(BaseModel):
    """A Debian source or binary package entity.

    The ``package_id`` is a stable canonical identifier. It should be
    deterministic given the same source data (e.g. ``name`` for source
    packages, or ``name:architecture`` for binary packages).

    Attributes:
        package_id: Stable canonical identifier for this package.
        name: Debian package name (e.g. "libc6", "python3").
        source_package: For binary packages, the name of the source package
            from which this binary was built. ``None`` for source packages.
        section: Archive section (e.g. "libs", "python", "net").
        architecture: Target architecture (e.g. "amd64", "all", "source").
            ``None`` when architecture is not applicable or unknown.
        maintainer: Package maintainer name/email if available.
        description: Short package description.
    """

    model_config = FrozenModelConfig

    package_id: str = Field(
        ...,
        description="Stable canonical identifier for this package.",
        examples=["libc6:amd64", "python3"],
    )
    name: str = Field(
        ...,
        description="Debian package name.",
        examples=["libc6", "python3"],
    )
    source_package: str | None = Field(
        default=None,
        description="Source package name (for binaries); None for source packages.",
    )
    section: str | None = Field(
        default=None,
        description="Archive section (e.g. libs, python, net).",
    )
    architecture: str | None = Field(
        default=None,
        description="Target architecture (amd64, all, source, etc.).",
    )
    maintainer: str | None = Field(
        default=None,
        description="Package maintainer name or email.",
    )
    description: str | None = Field(
        default=None,
        description="Short package description.",
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

    def to_parquet_row(self) -> dict[str, Any]:
        """Flatten to a Parquet/Arrow-compatible row dict.

        All nested structures are flattened; None values are preserved
        so that Arrow can infer nullability.
        """
        return {
            "package_id": self.package_id,
            "name": self.name,
            "source_package": self.source_package,
            "section": self.section,
            "architecture": self.architecture,
            "maintainer": self.maintainer,
            "description": self.description,
        }


# ---------------------------------------------------------------------------
# 5.1.2 PackageVersion
# ---------------------------------------------------------------------------

class PackageVersion(BaseModel):
    """An observed version of a Debian package.

    This represents the fact that a particular version of a package
    was seen in a specific distribution/context. The combination of
    ``package_id``, ``version``, and ``architecture`` (when present)
    should uniquely identify the version entity within a dataset.

    Attributes:
        package_id: Canonical package identifier (references Package).
        version: Debian version string (validated per Policy).
        distribution: Suite/distribution where observed (e.g. "sid", "trixie").
        architecture: Architecture this version targets.
        component: Archive component (main, contrib, non-free).
        priority: Package priority (required, important, standard, optional,
            extra).
        observed_at: When this version was observed by the pipeline (UTC).
        source: Data source identifier (e.g. "udd", "snapshot").
    """

    model_config = FrozenModelConfig

    package_id: str = Field(
        ...,
        description="Canonical package identifier.",
    )
    version: DebianVersionString = Field(
        ...,
        description="Debian version string.",
        examples=["2.31-13+deb12u5", "1:3.11.2-1"],
    )
    distribution: str | None = Field(
        default=None,
        description="Suite/distribution where observed (sid, trixie, bookworm).",
    )
    architecture: str | None = Field(
        default=None,
        description="Architecture this version targets.",
    )
    component: str | None = Field(
        default=None,
        description="Archive component (main, contrib, non-free).",
    )
    priority: str | None = Field(
        default=None,
        description="Package priority.",
    )
    observed_at: datetime = Field(
        default_factory=_utc_now,
        description="When this version was observed by the pipeline (UTC).",
    )
    source: str = Field(
        ...,
        description="Data source identifier (udd, snapshot, etc.).",
    )

    @field_validator("version", mode="before")
    @classmethod
    def _validate_version(cls, value: Any) -> Any:
        if value is None:
            raise ValueError("Version is required")
        return validate_debian_version(value)

    @field_validator("source", mode="before")
    @classmethod
    def _validate_source(cls, value: Any) -> Any:
        if not value or not isinstance(value, str):
            raise ValueError("source must be a non-empty string")
        return value

    @field_validator("observed_at")
    @classmethod
    def _ensure_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def to_parquet_row(self) -> dict[str, Any]:
        """Flatten to a Parquet/Arrow-compatible row dict."""
        return {
            "package_id": self.package_id,
            "version": self.version,
            "distribution": self.distribution,
            "architecture": self.architecture,
            "component": self.component,
            "priority": self.priority,
            "observed_at": self.observed_at.isoformat(),
            "source": self.source,
        }


# ---------------------------------------------------------------------------
# 5.1.3 Release
# ---------------------------------------------------------------------------

class Release(BaseModel):
    """A Debian release or suite.

    Debian uses both codenames (trixie, bookworm) and suite names
    (stable, testing, unstable) to refer to releases. This model
    captures both where available.

    Attributes:
        release_id: Canonical release identifier.
        name: Suite name (e.g. "stable", "testing", "unstable").
        codename: Release codename (e.g. "trixie", "bookworm", "sid").
        status: Release lifecycle status.
        version: Release version string if available (e.g. "12.0").
        description: Human-readable description.
    """

    model_config = FrozenModelConfig

    release_id: str = Field(
        ...,
        description="Canonical release identifier.",
        examples=["stable", "trixie", "bookworm"],
    )
    name: str = Field(
        ...,
        description="Suite name (stable, testing, unstable, experimental).",
    )
    codename: str | None = Field(
        default=None,
        description="Release codename (trixie, bookworm, sid).",
    )
    status: ReleaseStatus | None = Field(
        default=None,
        description="Release lifecycle status.",
    )
    version: str | None = Field(
        default=None,
        description="Release version number (e.g. 12.0).",
    )
    description: str | None = Field(
        default=None,
        description="Human-readable release description.",
    )

    @field_validator("release_id", "name", mode="before")
    @classmethod
    def _validate_required_str(cls, value: Any) -> Any:
        if not value or not isinstance(value, str):
            raise ValueError("Field must be a non-empty string")
        return value

    def to_parquet_row(self) -> dict[str, Any]:
        """Flatten to a Parquet/Arrow-compatible row dict."""
        return {
            "release_id": self.release_id,
            "name": self.name,
            "codename": self.codename,
            "status": self.status,
            "version": self.version,
            "description": self.description,
        }


# ---------------------------------------------------------------------------
# 5.1.4 Migration (minimal for R1)
# ---------------------------------------------------------------------------

class Migration(BaseModel):
    """A simplified migration-related fact.

    R1 does not attempt to model the full Britney state machine.
    This minimal representation captures enough information to support
    historical analysis and provides a stable foundation for R2
    extension.

    A migration fact represents that a package version was observed
    in the process of moving from one suite to another (e.g. from
    ``unstable`` to ``testing``).

    Attributes:
        migration_id: Canonical migration identifier.
        source: Source suite (e.g. "unstable", "sid").
        target: Target suite (e.g. "testing", "trixie").
        package: Package name.
        version: Version string being migrated.
        architecture: Architecture being migrated.
        status: Simplified migration status.
        observed_at: When this migration fact was observed (UTC).
    """

    model_config = FrozenModelConfig

    migration_id: str = Field(
        ...,
        description="Canonical migration identifier.",
    )
    source: str = Field(
        ...,
        description="Source suite (unstable, sid, etc.).",
    )
    target: str = Field(
        ...,
        description="Target suite (testing, trixie, stable, etc.).",
    )
    package: str = Field(
        ...,
        description="Package name.",
    )
    version: DebianVersionString = Field(
        ...,
        description="Version string being migrated.",
    )
    architecture: str | None = Field(
        default=None,
        description="Architecture being migrated.",
    )
    status: MigrationStatus = Field(
        default=MigrationStatus.UNKNOWN,
        description="Simplified migration status.",
    )
    observed_at: datetime = Field(
        default_factory=_utc_now,
        description="When this migration fact was observed (UTC).",
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

    @field_validator("observed_at")
    @classmethod
    def _ensure_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def to_parquet_row(self) -> dict[str, Any]:
        """Flatten to a Parquet/Arrow-compatible row dict."""
        return {
            "migration_id": self.migration_id,
            "source": self.source,
            "target": self.target,
            "package": self.package,
            "version": self.version,
            "architecture": self.architecture,
            "status": self.status.value,
            "observed_at": self.observed_at.isoformat(),
        }


# ---------------------------------------------------------------------------
# 5.2.1 Observation (Metadata)
# ---------------------------------------------------------------------------

class Observation(BaseModel):
    """Metadata recording that information was obtained from an external source.

    Observations are immutable metadata attached to domain facts.
    They preserve provenance: *what* was observed, *when*, *where it
    came from*, and *which artifact contains the raw evidence*.

    The distinction between ``observed_at`` and ``effective_at`` is
    important. ``effective_at`` is the domain time when the fact
    became true; ``observed_at`` is when the pipeline retrieved it.

    Attributes:
        observation_id: Unique observation identifier.
        entity_id: Canonical identifier of the observed entity.
        entity_type: Type of observed entity (package, package_version,
            release, migration).
        source: Data source (e.g. "udd", "snapshot", "archive").
        observed_at: When the pipeline observed this fact (UTC).
        effective_at: When the fact became true in the domain (UTC).
        payload_reference: Reference to the raw artifact containing
            the source evidence (URI, path, or object key).
        schema_version: Version of the schema used for normalization.
        ingestion_run_id: Identifier of the ingestion run that produced
            this observation.
    """

    model_config = FrozenModelConfig

    observation_id: str = Field(
        ...,
        description="Unique observation identifier.",
    )
    entity_id: str = Field(
        ...,
        description="Canonical identifier of the observed entity.",
    )
    entity_type: str = Field(
        ...,
        description="Type of observed entity.",
        examples=["package", "package_version", "release", "migration"],
    )
    source: str = Field(
        ...,
        description="Data source identifier.",
    )
    observed_at: datetime = Field(
        default_factory=_utc_now,
        description="When the pipeline observed this fact (UTC).",
    )
    effective_at: datetime | None = Field(
        default=None,
        description="When the fact became true in the domain (UTC).",
    )
    payload_reference: str | None = Field(
        default=None,
        description="Reference to raw artifact (URI, path, or object key).",
    )
    schema_version: int = Field(
        default=1,
        ge=1,
        description="Schema version used for normalization.",
    )
    ingestion_run_id: str | None = Field(
        default=None,
        description="Identifier of the ingestion run.",
    )

    @field_validator("observation_id", "entity_id", "entity_type", "source", mode="before")
    @classmethod
    def _validate_required_str(cls, value: Any) -> Any:
        if not value or not isinstance(value, str):
            raise ValueError("Field must be a non-empty string")
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
        """Ensure observed_at and effective_at default to the same timestamp.

        Prevents race conditions where two independent ``_utc_now()`` calls
        produce different values, which can trigger the ordering validator.
        """
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
        """Raise if effective_at is after observed_at.

        A fact cannot become effective after it was observed.
        """
        observed = info.data.get("observed_at")
        if observed is not None and value > observed:
            raise ValueError(
                "effective_at cannot be later than observed_at: "
                f"effective={value.isoformat()}, observed={observed.isoformat()}"
            )
        return value

    def to_parquet_row(self) -> dict[str, Any]:
        """Flatten to a Parquet/Arrow-compatible row dict."""
        return {
            "observation_id": self.observation_id,
            "entity_id": self.entity_id,
            "entity_type": self.entity_type,
            "source": self.source,
            "observed_at": self.observed_at.isoformat(),
            "effective_at": self.effective_at.isoformat(),
            "payload_reference": self.payload_reference,
            "schema_version": self.schema_version,
            "ingestion_run_id": self.ingestion_run_id,
        }
