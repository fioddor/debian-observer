"""Dataset manifest schema for ingestion provenance.

Each ingestion run produces a ``DatasetManifest`` that records:

- What was ingested (name, source, version)
- When it was retrieved
- Integrity verification (checksum, size)
- Volume metrics (record count)
- Licensing information
- Provenance chain references

Manifests are YAML-compatible Pydantic models: they serialize cleanly
to YAML, JSON, or Parquet. They are immutable and validated.

Usage::

    from debian_observer.events import DatasetManifest

    manifest = DatasetManifest(
        name="udd-packages-bookworm",
        source="udd.debian.net",
        source_version="2026-01-15",
        checksum="sha256:abc123...",
        size_bytes=45_231_890,
        record_count=1_234_567,
        license="MIT/Debian",  # or appropriate license
    )
    yaml_text = manifest.model_dump_yaml()
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ---------------------------------------------------------------------------
# UTC helper
# ---------------------------------------------------------------------------


def _utc_now() -> datetime:
    """Return the current UTC datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Checksum model
# ---------------------------------------------------------------------------


class Checksum(BaseModel):
    """Cryptographic checksum for a dataset artifact.

    Attributes:
        algorithm: Hash algorithm (sha256, sha512, blake2b, etc.).
        value: Hex-encoded checksum digest.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,
    )

    algorithm: str = Field(
        ...,
        description="Hash algorithm.",
        examples=["sha256", "sha512", "blake2b"],
    )
    value: str = Field(
        ...,
        description="Hex-encoded checksum digest.",
    )

    @field_validator("algorithm", mode="before")
    @classmethod
    def _validate_algorithm(cls, value: Any) -> Any:
        if not value or not isinstance(value, str):
            raise ValueError("algorithm must be a non-empty string")
        allowed = {"sha256", "sha512", "sha1", "md5", "blake2b", "blake2s"}
        if value.lower() not in allowed:
            raise ValueError(
                f"Unsupported checksum algorithm: {value!r}. "
                f"Supported: {', '.join(sorted(allowed))}"
            )
        return value.lower()

    @field_validator("value", mode="before")
    @classmethod
    def _validate_value(cls, value: Any) -> Any:
        if not value or not isinstance(value, str):
            raise ValueError("checksum value must be a non-empty string")
        # Strip common prefixes and validate hex
        cleaned = value.removeprefix("0x").removeprefix("0X")
        if not all(c in "0123456789abcdefABCDEF" for c in cleaned):
            raise ValueError("checksum value must be a hexadecimal string")
        return cleaned.lower()


# ---------------------------------------------------------------------------
# Dataset Manifest
# ---------------------------------------------------------------------------


class DatasetManifest(BaseModel):
    """Manifest describing a single ingested dataset.

    The manifest becomes part of the provenance chain. It records
    enough metadata to reproduce the ingestion and verify integrity.

    Attributes:
        name: Human-readable dataset name.
        source: Origin of the data (URL, service, or dataset identifier).
        source_version: Version or snapshot identifier from the source.
        retrieved_at: When the dataset was fetched (UTC).
        checksum: Integrity checksum of the raw artifact.
        size_bytes: Size of the raw artifact in bytes.
        record_count: Number of logical records (approximate is acceptable).
        license: License or terms under which the data is available.
        description: Optional human-readable description.
        ingestion_run_id: Identifier linking this manifest to a pipeline run.
        parent_manifest_id: Optional reference to a parent or prior manifest
            (for incremental updates).
        tags: Optional string tags for categorization.
        metadata: Optional free-form metadata dict.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,
        str_strip_whitespace=True,
    )

    name: str = Field(
        ...,
        description="Human-readable dataset name.",
        examples=["udd-packages-bookworm", "snapshot-sources-2026-01"],
    )
    source: str = Field(
        ...,
        description="Origin of the data (URL, service, or identifier).",
        examples=["udd.debian.net", "https://snapshot.debian.org/"],
    )
    source_version: str | None = Field(
        default=None,
        description="Version or snapshot identifier from the source.",
    )
    retrieved_at: datetime = Field(
        default_factory=_utc_now,
        description="When the dataset was fetched (UTC).",
    )
    checksum: Checksum = Field(
        ...,
        description="Integrity checksum of the raw artifact.",
    )
    size_bytes: int = Field(
        ...,
        ge=0,
        description="Size of the raw artifact in bytes.",
    )
    record_count: int = Field(
        ...,
        ge=0,
        description="Number of logical records.",
    )
    license: str | None = Field(
        default=None,
        description="License or terms under which the data is available.",
    )
    description: str | None = Field(
        default=None,
        description="Optional human-readable description.",
    )
    ingestion_run_id: str | None = Field(
        default=None,
        description="Identifier linking this manifest to a pipeline run.",
    )
    parent_manifest_id: str | None = Field(
        default=None,
        description="Reference to a parent/prior manifest (for incremental).",
    )
    tags: tuple[str, ...] = Field(
        default=(),
        description="Optional string tags for categorization.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional free-form metadata.",
    )

    @field_validator("name", "source", mode="before")
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

    @field_validator("size_bytes", "record_count")
    @classmethod
    def _non_negative(cls, value: int) -> int:
        if value < 0:
            raise ValueError("value must be non-negative")
        return value

    # -----------------------------------------------------------------------
    # Serialization helpers
    # -----------------------------------------------------------------------

    def to_yaml(self) -> str:
        """Serialize to a YAML-compatible string representation.

        This produces a clean YAML block suitable for writing to disk.
        Requires the ``pyyaml`` package.

        Returns:
            YAML string representation of the manifest.

        Raises:
            ImportError: If ``pyyaml`` is not installed.
        """
        try:
            import yaml
        except ImportError as exc:
            raise ImportError(
                "pyyaml is required for YAML serialization. "
                "Install with: pip install pyyaml"
            ) from exc

        # Use safe_dump with custom representer for datetime
        def _datetime_representer(dumper: Any, data: datetime) -> Any:
            return dumper.represent_scalar(
                "tag:yaml.org,2002:timestamp", data.isoformat()
            )

        yaml.add_representer(datetime, _datetime_representer)

        return yaml.safe_dump(
            self.model_dump(mode="json"),
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )

    @classmethod
    def from_yaml(cls, yaml_text: str) -> DatasetManifest:
        """Deserialize from a YAML string.

        Args:
            yaml_text: YAML string to parse.

        Returns:
            Parsed DatasetManifest instance.

        Raises:
            ImportError: If ``pyyaml`` is not installed.
            ValueError: If the YAML is malformed.
        """
        try:
            import yaml
        except ImportError as exc:
            raise ImportError(
                "pyyaml is required for YAML deserialization. "
                "Install with: pip install pyyaml"
            ) from exc

        try:
            data = yaml.safe_load(yaml_text)
        except Exception as exc:
            raise ValueError(f"Failed to parse YAML: {exc}") from exc

        if not isinstance(data, dict):
            raise ValueError("YAML must contain a mapping at the top level")

        return cls.model_validate(data)
