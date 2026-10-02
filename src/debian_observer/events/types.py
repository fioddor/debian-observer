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

Event types defined here (R1):

- ``PackageObserved``          — a package entity was seen
- ``PackageVersionObserved``   — a specific version was observed
- ``ReleaseObserved``          — a release/suite was observed
- ``MigrationObserved``        — a migration fact was observed
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
        schema_version: Schema version used for normalization.
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

    def to_parquet_row(self) -> dict[str, Any]:
        """Flatten payload to a Parquet-compatible row dict."""
        obs = self.observation
        return {
            "package_id": self.package_id,
            "name": self.name,
            "source_package": self.source_package,
            "section": self.section,
            "architecture": self.architecture,
            "maintainer": self.maintainer,
            "description": self.description,
            "observation_source": obs.source,
            "observation_observed_at": obs.observed_at.isoformat(),
            "observation_effective_at": obs.effective_at.isoformat(),
            "observation_schema_version": obs.schema_version,
            "observation_ingestion_run_id": obs.ingestion_run_id,
        }


# ---------------------------------------------------------------------------
# 6.2 Specific event type: PackageVersionObserved
# ---------------------------------------------------------------------------


class PackageVersionObservedPayload(BaseModel):
    """Payload for ``PackageVersionObserved`` events.

    Emitted when a specific version of a package is observed in a
    distribution/suite.

    Attributes:
        package_id: Canonical package identifier.
        version: Debian version string.
        distribution: Suite where observed.
        architecture: Architecture this version targets.
        component: Archive component.
        priority: Package priority.
        observation: Provenance metadata.
    """

    model_config = FrozenModelConfig

    package_id: str = Field(..., description="Canonical package identifier.")
    version: str = Field(..., description="Debian version string.")
    distribution: str | None = Field(
        default=None, description="Suite where observed (sid, trixie, etc.)."
    )
    architecture: str | None = Field(
        default=None, description="Architecture this version targets."
    )
    component: str | None = Field(
        default=None, description="Archive component (main, contrib, non-free)."
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

    def to_parquet_row(self) -> dict[str, Any]:
        """Flatten payload to a Parquet-compatible row dict."""
        obs = self.observation
        return {
            "package_id": self.package_id,
            "version": self.version,
            "distribution": self.distribution,
            "architecture": self.architecture,
            "component": self.component,
            "priority": self.priority,
            "observation_source": obs.source,
            "observation_observed_at": obs.observed_at.isoformat(),
            "observation_effective_at": obs.effective_at.isoformat(),
            "observation_schema_version": obs.schema_version,
            "observation_ingestion_run_id": obs.ingestion_run_id,
        }


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

    def to_parquet_row(self) -> dict[str, Any]:
        """Flatten payload to a Parquet-compatible row dict."""
        obs = self.observation
        return {
            "release_id": self.release_id,
            "name": self.name,
            "codename": self.codename,
            "status": self.status,
            "release_version": self.version,
            "description": self.description,
            "observation_source": obs.source,
            "observation_observed_at": obs.observed_at.isoformat(),
            "observation_effective_at": obs.effective_at.isoformat(),
            "observation_schema_version": obs.schema_version,
            "observation_ingestion_run_id": obs.ingestion_run_id,
        }


# ---------------------------------------------------------------------------
# 6.2 Specific event type: MigrationObserved
# ---------------------------------------------------------------------------


class MigrationObservedPayload(BaseModel):
    """Payload for ``MigrationObserved`` events.

    Emitted when a migration-related fact is observed.

    R1 uses a simplified migration model. The full Britney state
    machine is deferred to R2. This payload captures the minimal
    facts needed for historical analysis.

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

    def to_parquet_row(self) -> dict[str, Any]:
        """Flatten payload to a Parquet-compatible row dict."""
        obs = self.observation
        return {
            "migration_id": self.migration_id,
            "source": self.source,
            "target": self.target,
            "package": self.package,
            "version": self.version,
            "architecture": self.architecture,
            "status": self.status.value,
            "observation_source": obs.source,
            "observation_observed_at": obs.observed_at.isoformat(),
            "observation_effective_at": obs.effective_at.isoformat(),
            "observation_schema_version": obs.schema_version,
            "observation_ingestion_run_id": obs.ingestion_run_id,
        }
