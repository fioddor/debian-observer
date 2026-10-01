"""Ingestion: Provenance.

Records provenance for raw artifacts and ingested datasets.

Provenance tracking ensures that every piece of data in the system
can be traced back to its origin. The provenance chain includes:

- Raw artifact (the original downloaded file)
- Acquisition metadata (when, from where, by whom)
- Dataset manifest (checksum, count, size)
- Normalization steps (which transformation version)
- Events produced (which ingestion run generated them)

Design notes
------------

* ``RawArtifact`` is a lightweight model for immutable raw files.
* ``ProvenanceRecorder`` creates ``Observation`` metadata that
  links artifacts to ingestion runs.
* The provenance chain is flat in R1; graph provenance is deferred.
* All artifacts are treated as immutable. If the same source changes,
  a new artifact is created with a new timestamp.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from debian_observer.domain.models import Observation

# ---------------------------------------------------------------------------
# Raw artifact model
# ---------------------------------------------------------------------------

class RawArtifact(BaseModel):
    """Reference to an immutable raw data artifact.

    Raw artifacts are the original source data files, preserved
    exactly as acquired. They are never modified after creation.

    Attributes:
        artifact_id: Unique identifier for this artifact.
        source: Data source that produced this artifact.
        path: Filesystem path to the artifact.
        checksum: SHA-256 checksum of the artifact contents.
        size_bytes: Size of the artifact in bytes.
        acquired_at: UTC timestamp when the artifact was acquired.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,
        str_strip_whitespace=True,
    )

    artifact_id: str = Field(
        ...,
        description="Unique identifier for this artifact.",
    )
    source: str = Field(
        ...,
        description="Data source that produced this artifact.",
    )
    path: Path = Field(
        ...,
        description="Filesystem path to the artifact.",
    )
    checksum: str = Field(
        ...,
        description="SHA-256 checksum of the artifact contents.",
    )
    size_bytes: int = Field(
        ...,
        ge=0,
        description="Size of the artifact in bytes.",
    )
    acquired_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp when the artifact was acquired.",
    )

    @field_validator("artifact_id", "source", "checksum", mode="before")
    @classmethod
    def _validate_required_str(cls, value: Any) -> Any:
        if not value or not isinstance(value, str):
            raise ValueError("Field must be a non-empty string")
        return value

    @field_validator("path", mode="before")
    @classmethod
    def _validate_path(cls, value: Any) -> Path:
        if isinstance(value, str):
            return Path(value)
        if isinstance(value, Path):
            return value
        raise ValueError(f"path must be a string or Path, got {type(value).__name__}")

    @field_validator("acquired_at")
    @classmethod
    def _ensure_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Provenance recorder
# ---------------------------------------------------------------------------

class ProvenanceRecorder:
    """Records and retrieves provenance metadata for raw artifacts.

    The recorder bridges between raw artifacts and the domain model's
    ``Observation`` metadata. Each artifact gets an observation record
    that tracks when it was acquired, from which source, and which
    ingestion run produced it.

    Usage::

        recorder = ProvenanceRecorder()
        artifact = RawArtifact(...)
        observation = recorder.record_artifact(
            artifact,
            ingestion_run_id="run-001",
        )
    """

    def record_artifact(
        self,
        artifact: RawArtifact,
        ingestion_run_id: str,
        schema_version: int = 1,
    ) -> Observation:
        """Record provenance for a raw artifact.

        Args:
            artifact: The raw artifact to record.
            ingestion_run_id: Identifier of the ingestion run.
            schema_version: Schema version used for normalization.

        Returns:
            An Observation domain model instance.
        """
        return Observation(
            observation_id=artifact.artifact_id,
            entity_id=artifact.artifact_id,
            entity_type="raw_artifact",
            source=artifact.source,
            observed_at=artifact.acquired_at,
            effective_at=artifact.acquired_at,
            payload_reference=str(artifact.path),
            schema_version=schema_version,
            ingestion_run_id=ingestion_run_id,
        )

    def record_dataset_manifest(
        self,
        manifest_path: Path,
        source: str,
        ingestion_run_id: str,
        schema_version: int = 1,
    ) -> Observation:
        """Record provenance for a dataset manifest file.

        Args:
            manifest_path: Path to the manifest file.
            source: Data source identifier.
            ingestion_run_id: Identifier of the ingestion run.
            schema_version: Schema version used for normalization.

        Returns:
            An Observation domain model instance.
        """
        now = datetime.now(timezone.utc)
        return Observation(
            observation_id=f"manifest-{ingestion_run_id}",
            entity_id=f"manifest-{ingestion_run_id}",
            entity_type="dataset_manifest",
            source=source,
            observed_at=now,
            effective_at=now,
            payload_reference=str(manifest_path),
            schema_version=schema_version,
            ingestion_run_id=ingestion_run_id,
        )
