"""Ingestion: Pipeline.

Orchestrates the end-to-end ingestion workflow:

1. Acquire data from a source
2. Store raw artifact
3. Build dataset manifest
4. Record provenance
5. Transform records into domain events
6. Return ingestion result

The pipeline is intentionally stateless. All mutable state is
externalized to the filesystem (raw artifacts, manifests) and to
the event system (domain events). This makes the pipeline easy to
test, restart, and scale.

Design notes
------------

* ``IngestionPipeline`` is a single-pass batch processor in R1.
  Streaming/real-time ingestion is deferred to R2.
* The pipeline does not publish events directly to NATS. Instead,
  it returns the events as a list, allowing the caller to decide
  how to publish (local file, in-memory queue, or NATS).
* Raw artifacts are stored as JSON lines for simplicity and readability.
  Parquet raw storage is deferred to R2 if volume warrants it.
* The pipeline is idempotent when given the same source and run ID:
  the same input always produces the same checksum and events.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from debian_observer.events import EventEnvelope
from debian_observer.ingestion.acquisition import AcquisitionService, DataSource
from debian_observer.ingestion.manifest import DatasetManifest, ManifestBuilder
from debian_observer.ingestion.provenance import ProvenanceRecorder, RawArtifact
from debian_observer.normalization.normalizer import Normalizer
from debian_observer.observability.metrics import IngestionMetrics
from debian_observer.observability.tracing import get_tracer, span

# ---------------------------------------------------------------------------
# Ingestion result model
# ---------------------------------------------------------------------------


class IngestionResult(BaseModel):
    """Result of a single ingestion pipeline run.

        This model captures all outputs of an ingestion run, making it
    easy for callers to inspect what was produced and decide on next
        steps (e.g. publish events, store in Parquet).

        Attributes:
            ingestion_run_id: Identifier of the ingestion run.
            source_name: Name of the data source.
            record_count: Number of records ingested.
            checksum: SHA-256 checksum of the raw data.
            artifact_path: Path to the stored raw artifact.
            manifest: Dataset manifest describing the acquisition.
            manifest_path: Path to the persisted manifest JSON file.
            events: Domain events produced from the ingested records.
            acquired_at: UTC timestamp when acquisition completed.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,
        str_strip_whitespace=True,
    )

    ingestion_run_id: str = Field(..., description="Identifier of the ingestion run.")
    source_name: str = Field(..., description="Name of the data source.")
    record_count: int = Field(..., ge=0, description="Number of records ingested.")
    checksum: str = Field(..., description="SHA-256 checksum of the raw data.")
    artifact_path: Path | None = Field(
        default=None, description="Path to the stored raw artifact."
    )
    parquet_path: Path | None = Field(
        default=None, description="Path to the written Parquet file."
    )
    dataset_path: Path | None = Field(
        default=None,
        description="Path to the written normalized dataset file.",
    )
    manifest: DatasetManifest = Field(..., description="Dataset manifest.")
    manifest_path: Path | None = Field(
        default=None,
        description="Path to the persisted manifest JSON file.",
    )
    events: list[EventEnvelope] = Field(
        default_factory=list, description="Domain events produced."
    )
    acquired_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp when acquisition completed.",
    )

    @field_validator("artifact_path", mode="before")
    @classmethod
    def _validate_path(cls, value: Any) -> Path | None:
        if value is None:
            return None
        if isinstance(value, str):
            return Path(value)
        if isinstance(value, Path):
            return value
        raise ValueError(
            f"artifact_path must be a string or Path, got {type(value).__name__}"
        )

    @field_validator("acquired_at")
    @classmethod
    def _ensure_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class IngestionPipeline:
    """End-to-end ingestion pipeline for Debian Observatory.

    The pipeline coordinates acquisition, storage, manifest creation,
    provenance recording, and event generation. It is designed to be
    called as a single atomic operation for a given data source.

    Usage::

        pipeline = IngestionPipeline(raw_dir=Path("data/raw"))
        result = pipeline.run(source, ingestion_run_id="run-001")

        for event in result.events:
            print(f"Produced {event.event_type} for {event.entity_id}")

        # Raw artifact stored at:
        print(result.artifact_path)
    """

    def __init__(
        self,
        raw_dir: Path = Path("data/raw"),
        parquet_dir: Path | None = None,
        dataset_dir: Path | None = None,
    ) -> None:
        """Initialize the pipeline.

        Args:
            raw_dir: Directory for storing raw artifacts. Created if
                     it does not exist.
            parquet_dir: Optional directory for Parquet output. If
                         provided, events are written to Parquet after
                         normalization.
            dataset_dir: Optional directory for the normalized
                         package_versions dataset (spec 10.2). If
                         provided, PackageVersionObserved events are
                         projected into the dataset after normalization.
        """
        self.raw_dir = raw_dir
        self.parquet_dir = parquet_dir
        self.dataset_dir = dataset_dir
        self._acquisition = AcquisitionService()
        self._manifest_builder = ManifestBuilder()
        self._provenance = ProvenanceRecorder()
        self._metrics = IngestionMetrics()

    @span()
    def run(
        self,
        source: DataSource,
        ingestion_run_id: str | None = None,
    ) -> IngestionResult:
        """Run the full ingestion pipeline for a data source.

        Args:
            source: The data source to ingest.
            ingestion_run_id: Optional run identifier. If not provided,
                              a UUID is generated.

        Returns:
            An IngestionResult containing all outputs of the run.
        """
        import time

        start = time.perf_counter()
        run_id = ingestion_run_id or f"run-{uuid.uuid4().hex[:12]}"
        tracer = get_tracer("debian_observer.ingestion")

        try:
            with tracer.start_as_current_span("ingestion.acquire"):
                # Step 1: Acquire data
                acquisition_meta = self._acquisition.acquire(source)
                records = source.retrieve()

            with tracer.start_as_current_span("ingestion.store_raw"):
                # Step 2: Store raw artifact
                artifact_path = self._store_raw_artifact(records, run_id, source.name)
                self._metrics.add_bytes(
                    artifact_path.stat().st_size, source=source.name
                )

            with tracer.start_as_current_span("ingestion.build_manifest"):
                # Step 3: Build manifest
                manifest = self._manifest_builder.build(
                    source=source,
                    dataset_name=f"{source.name}-{run_id}",
                )

            manifest_path: Path | None = None
            with tracer.start_as_current_span("ingestion.store_manifest"):
                # Step 3b: Persist the manifest as JSON next to the
                # raw artifact so the provenance chain survives the
                # run and later audits need no captured stdout.
                manifest_path = artifact_path.with_suffix(".manifest.json")
                manifest_path.write_text(
                    manifest.to_json(), encoding="utf-8"
                )

            with tracer.start_as_current_span("ingestion.record_provenance"):
                # Step 4: Record provenance
                artifact_meta = RawArtifact(
                    artifact_id=f"artifact-{run_id}",
                    source=source.name,
                    path=artifact_path,
                    checksum=acquisition_meta["checksum"],
                    size_bytes=artifact_path.stat().st_size,
                )
                self._provenance.record_artifact(artifact_meta, ingestion_run_id=run_id)

            with tracer.start_as_current_span("ingestion.transform_events"):
                # Step 5: Normalize records to domain events
                normalizer = Normalizer(
                    source_name=source.name, ingestion_run_id=run_id
                )
                events, invalid = normalizer.normalize_records(records)
                if invalid:
                    # In R1 we silently drop invalid records; log in R2
                    pass

            # Step 6: Write to Parquet (if configured)
            parquet_path: Path | None = None
            if self.parquet_dir is not None and events:
                with tracer.start_as_current_span("ingestion.write_parquet"):
                    from debian_observer.storage.parquet import ParquetWriter

                    writer = ParquetWriter(output_dir=self.parquet_dir)
                    parquet_path = writer.write_events(events, ingestion_run_id=run_id)

            # Step 6b: Write the normalized dataset (if configured)
            dataset_path: Path | None = None
            if self.dataset_dir is not None and events:
                with tracer.start_as_current_span("ingestion.write_dataset"):
                    from debian_observer.storage.dataset import (
                        PackageVersionDatasetWriter,
                    )

                    dataset_writer = PackageVersionDatasetWriter(
                        output_dir=self.dataset_dir
                    )
                    dataset_path = dataset_writer.write_events(
                        events, ingestion_run_id=run_id
                    )

            # Record metrics
            duration = time.perf_counter() - start
            self._metrics.record_duration(duration, source=source.name)
            self._metrics.add_records(len(records), source=source.name)

            # Assemble result
            return IngestionResult(
                ingestion_run_id=run_id,
                source_name=source.name,
                record_count=acquisition_meta["record_count"],
                checksum=acquisition_meta["checksum"],
                artifact_path=artifact_path,
                parquet_path=parquet_path,
                dataset_path=dataset_path,
                manifest=manifest,
                manifest_path=manifest_path,
                events=events,
                acquired_at=datetime.fromisoformat(acquisition_meta["acquired_at"]),
            )
        except Exception:
            self._metrics.add_error(source=source.name)
            raise

    def _store_raw_artifact(
        self,
        records: list[dict[str, Any]],
        run_id: str,
        source_name: str,
    ) -> Path:
        """Store records as a JSON-lines raw artifact.

        Returns:
            Path to the created artifact file.
        """
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        filename = f"{source_name}-{timestamp}-{run_id}.jsonl"
        artifact_path = self.raw_dir / filename

        with open(artifact_path, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

        return artifact_path
