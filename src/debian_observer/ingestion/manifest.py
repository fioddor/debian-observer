"""Ingestion: Manifest.

Builds dataset manifests containing checksums, record counts,
and acquisition metadata for reproducible processing.

The manifest is the primary provenance artifact for an ingestion run.
It records:

- What dataset was acquired
- When it was retrieved
- Which source and version
- Checksum for integrity verification
- Size and record count
- License information

Design notes
------------

* Manifests are intentionally simple (Pydantic model → dict → YAML/JSON).
  Complex manifest standards are deferred to R2.
* The manifest becomes part of the provenance chain: downstream
  consumers reference it to understand the origin of normalized data.
* Checksums are computed by the DataSource, not the ManifestBuilder,
  maintaining separation of concerns.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from debian_observer.ingestion.acquisition import DataSource

# ---------------------------------------------------------------------------
# Dataset manifest model
# ---------------------------------------------------------------------------

class DatasetManifest(BaseModel):
    """Manifest describing a single acquired dataset.

    Each ingestion run produces one manifest per source dataset.
    The manifest is stored alongside the raw artifact and referenced
    by the provenance chain.

    Attributes:
        dataset_name: Human-readable name for this dataset.
        source: Canonical source identifier (e.g. "udd", "snapshot").
        source_version: Version or snapshot date of the source data.
        retrieved_at: UTC timestamp when data was acquired.
        checksum: SHA-256 checksum of the raw data.
        size_bytes: Size of the raw artifact in bytes.
        record_count: Number of records in the dataset.
        license: License under which the data is distributed.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,
        str_strip_whitespace=True,
    )

    dataset_name: str = Field(
        ...,
        description="Human-readable name for this dataset.",
    )
    source: str = Field(
        ...,
        description="Canonical source identifier (e.g. udd, snapshot).",
    )
    source_version: str | None = Field(
        default=None,
        description="Version or snapshot date of the source data.",
    )
    retrieved_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp when data was acquired.",
    )
    checksum: str | None = Field(
        default=None,
        description="SHA-256 checksum of the raw data.",
    )
    size_bytes: int | None = Field(
        default=None,
        ge=0,
        description="Size of the raw artifact in bytes.",
    )
    record_count: int | None = Field(
        default=None,
        ge=0,
        description="Number of records in the dataset.",
    )
    license: str | None = Field(
        default=None,
        description="License under which the data is distributed.",
    )

    @field_validator("dataset_name", "source", mode="before")
    @classmethod
    def _validate_required_str(cls, value: Any) -> Any:
        if not value or not isinstance(value, str):
            raise ValueError("Field must be a non-empty string")
        return value

    @field_validator("retrieved_at")
    @classmethod
    def _ensure_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dict suitable for YAML/JSON output."""
        return {
            "dataset_name": self.dataset_name,
            "source": self.source,
            "source_version": self.source_version,
            "retrieved_at": self.retrieved_at.isoformat(),
            "checksum": self.checksum,
            "size_bytes": self.size_bytes,
            "record_count": self.record_count,
            "license": self.license,
        }

    def to_json(self) -> str:
        """Serialize to a JSON string."""
        import json
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)

    def to_yaml(self) -> str:
        """Serialize to a YAML string.

        Requires PyYAML to be installed. Since PyYAML is not a core
        dependency, this method raises ImportError if unavailable.
        """
        try:
            import yaml
            return yaml.safe_dump(
                self.to_dict(),
                sort_keys=False,
                allow_unicode=True,
            )
        except ImportError:
            raise ImportError(
                "PyYAML is required for YAML output. "
                "Install with: pip install pyyaml"
            )


# ---------------------------------------------------------------------------
# Manifest builder
# ---------------------------------------------------------------------------

class ManifestBuilder:
    """Constructs dataset manifests from data sources.

    The builder bridges between the acquisition layer and the
    provenance layer. It takes a ``DataSource`` and produces a
    ``DatasetManifest`` with all available metadata.

    Usage::

        builder = ManifestBuilder()
        manifest = builder.build(
            source=my_source,
            dataset_name="udd-packages",
            license="public domain",
        )
        print(manifest.to_json())
    """

    def build(
        self,
        *,
        source: DataSource,
        dataset_name: str,
        source_version: str | None = None,
        license: str | None = None,
    ) -> DatasetManifest:
        """Build a manifest from a data source.

        Args:
            source: The data source that was acquired.
            dataset_name: Human-readable name for the dataset.
            source_version: Optional version/snapshot identifier.
            license: Optional license string.

        Returns:
            A frozen DatasetManifest with computed checksum and record count.
        """
        return DatasetManifest(
            dataset_name=dataset_name,
            source=source.name,
            source_version=source_version,
            checksum=source.compute_checksum(),
            record_count=source.record_count(),
            license=license,
        )
