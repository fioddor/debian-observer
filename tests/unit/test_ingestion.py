"""Unit tests for Debian Observatory ingestion pipeline.

These tests verify that the ingestion layer correctly:
- Retrieves source data (via mock data sources)
- Validates acquisition (checksums, record counts)
- Records provenance
- Stores raw artifacts
- Creates dataset manifests
- Produces domain events
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from debian_observer.ingestion.acquisition import (
    AcquisitionService,
    DataSource,
    InMemoryDataSource,
    MockDebianDataSource,
)
from debian_observer.ingestion.manifest import DatasetManifest, ManifestBuilder
from debian_observer.ingestion.pipeline import IngestionPipeline, IngestionResult
from debian_observer.ingestion.provenance import ProvenanceRecorder, RawArtifact


# ---------------------------------------------------------------------------
# Acquisition tests
# ---------------------------------------------------------------------------

class TestInMemoryDataSource:
    """Tests for the in-memory data source (used for tests)."""

    def test_retrieve_records(self) -> None:
        source = InMemoryDataSource(
            name="test-source",
            records=[
                {"package": "libc6", "version": "2.37-12"},
                {"package": "python3", "version": "3.11.2-1"},
            ],
        )
        records = source.retrieve()
        assert len(records) == 2
        assert records[0]["package"] == "libc6"

    def test_compute_checksum(self) -> None:
        source = InMemoryDataSource(
            name="test-source",
            records=[
                {"package": "libc6", "version": "2.37-12"},
            ],
        )
        checksum = source.compute_checksum()
        assert isinstance(checksum, str)
        assert len(checksum) == 64  # SHA-256 hex

    def test_record_count(self) -> None:
        source = InMemoryDataSource(
            name="test-source",
            records=[{"package": "libc6"}],
        )
        assert source.record_count() == 1


class TestMockDebianDataSource:
    """Tests for the mock Debian data source."""

    def test_generates_package_records(self) -> None:
        source = MockDebianDataSource(package_count=5)
        records = source.retrieve()
        assert len(records) == 5
        assert all("package" in r for r in records)
        assert all("version" in r for r in records)

    def test_generates_version_records(self) -> None:
        source = MockDebianDataSource(
            package_count=3, versions_per_package=2
        )
        records = source.retrieve()
        assert len(records) == 6  # 3 packages × 2 versions
        assert all("distribution" in r for r in records)

    def test_deterministic_with_seed(self) -> None:
        source1 = MockDebianDataSource(package_count=3, seed=42)
        source2 = MockDebianDataSource(package_count=3, seed=42)
        records1 = source1.retrieve()
        records2 = source2.retrieve()
        assert records1 == records2


class TestAcquisitionService:
    """Tests for the acquisition service."""

    def test_acquire_data(self) -> None:
        source = InMemoryDataSource(
            name="udd-test",
            records=[{"package": "libc6"}],
        )
        service = AcquisitionService()
        result = service.acquire(source)

        assert result["source_name"] == "udd-test"
        assert result["record_count"] == 1
        assert "checksum" in result
        assert "acquired_at" in result

    def test_validate_checksum(self) -> None:
        source = InMemoryDataSource(
            name="test-source",
            records=[{"package": "libc6"}],
        )
        service = AcquisitionService()
        result = service.acquire(source)

        # Validation should succeed with correct checksum
        assert service.validate(result["checksum"], source) is True

        # Validation should fail with wrong checksum
        assert service.validate("wrong-checksum", source) is False


# ---------------------------------------------------------------------------
# Manifest tests
# ---------------------------------------------------------------------------

class TestDatasetManifest:
    """Tests for the dataset manifest model."""

    def test_construction(self) -> None:
        manifest = DatasetManifest(
            dataset_name="udd-packages",
            source="udd",
            source_version="2026-01-01",
            checksum="abc123",
            size_bytes=1024,
            record_count=100,
            license="public domain",
        )
        assert manifest.dataset_name == "udd-packages"
        assert manifest.record_count == 100

    def test_to_dict(self) -> None:
        manifest = DatasetManifest(
            dataset_name="udd-packages",
            source="udd",
            retrieved_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        d = manifest.to_dict()
        assert d["dataset_name"] == "udd-packages"
        assert d["source"] == "udd"
        assert "retrieved_at" in d


class TestManifestBuilder:
    """Tests for the manifest builder."""

    def test_build_from_source(self) -> None:
        source = InMemoryDataSource(
            name="udd",
            records=[{"package": "libc6"}],
        )
        builder = ManifestBuilder()
        manifest = builder.build(
            source=source,
            dataset_name="udd-snapshot",
        )

        assert manifest.dataset_name == "udd-snapshot"
        assert manifest.source == "udd"
        assert manifest.record_count == 1
        assert manifest.checksum is not None


# ---------------------------------------------------------------------------
# Provenance tests
# ---------------------------------------------------------------------------

class TestRawArtifact:
    """Tests for the raw artifact model."""

    def test_construction(self) -> None:
        artifact = RawArtifact(
            artifact_id="art-123",
            source="udd",
            path=Path("/tmp/test.csv"),
            checksum="sha256:abc",
            size_bytes=1024,
        )
        assert artifact.source == "udd"
        assert artifact.path.name == "test.csv"


class TestProvenanceRecorder:
    """Tests for the provenance recorder."""

    def test_record_artifact(self, tmp_path: Path) -> None:
        recorder = ProvenanceRecorder()
        artifact = RawArtifact(
            artifact_id="art-123",
            source="udd",
            path=tmp_path / "test.csv",
            checksum="sha256:abc",
            size_bytes=1024,
        )
        record = recorder.record_artifact(artifact, ingestion_run_id="run-1")
        assert record.ingestion_run_id == "run-1"
        assert record.entity_id == "art-123"
        assert record.source == "udd"


# ---------------------------------------------------------------------------
# Pipeline tests
# ---------------------------------------------------------------------------

class TestIngestionPipeline:
    """Integration-style tests for the ingestion pipeline."""

    def test_run_with_mock_source(self, tmp_path: Path) -> None:
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()

        source = MockDebianDataSource(package_count=3)
        pipeline = IngestionPipeline(raw_dir=raw_dir)
        result = pipeline.run(source, ingestion_run_id="run-001")

        assert isinstance(result, IngestionResult)
        assert result.ingestion_run_id == "run-001"
        assert result.record_count == 3
        assert result.manifest is not None
        assert result.manifest.checksum is not None
        assert result.events is not None
        assert len(result.events) == 3

    def test_stores_raw_artifact(self, tmp_path: Path) -> None:
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()

        source = MockDebianDataSource(package_count=2)
        pipeline = IngestionPipeline(raw_dir=raw_dir)
        result = pipeline.run(source, ingestion_run_id="run-002")

        # Raw artifact should be stored
        assert result.artifact_path is not None
        assert result.artifact_path.exists()

    def test_produces_valid_events(self, tmp_path: Path) -> None:
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()

        source = MockDebianDataSource(package_count=2)
        pipeline = IngestionPipeline(raw_dir=raw_dir)
        result = pipeline.run(source, ingestion_run_id="run-003")

        # Events should be valid domain events
        from debian_observer.events import EventEnvelope
        for event in result.events:
            assert isinstance(event, EventEnvelope)
            assert event.event_type is not None
            assert event.entity_id is not None
            assert event.source is not None

    def test_idempotency_same_run(self, tmp_path: Path) -> None:
        """Pipeline should be deterministic for same source and run ID."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()

        source = MockDebianDataSource(package_count=2, seed=42)
        pipeline1 = IngestionPipeline(raw_dir=raw_dir)
        pipeline2 = IngestionPipeline(raw_dir=raw_dir)

        result1 = pipeline1.run(source, ingestion_run_id="run-004")
        result2 = pipeline2.run(source, ingestion_run_id="run-004")

        assert result1.record_count == result2.record_count
        assert result1.manifest.checksum == result2.manifest.checksum


# ---------------------------------------------------------------------------
# Coverage gap tests
# ---------------------------------------------------------------------------

class TestManifestCoverage:
    """Tests for manifest paths not exercised by main tests."""

    def test_manifest_validators(self) -> None:
        with pytest.raises(ValueError):
            DatasetManifest(dataset_name="", source="test")
        with pytest.raises(ValueError):
            DatasetManifest(dataset_name="test", source="")

    def test_manifest_naive_datetime(self) -> None:
        from datetime import datetime
        manifest = DatasetManifest(
            dataset_name="test",
            source="test",
            retrieved_at=datetime(2026, 1, 1, 12, 0, 0),  # naive
        )
        assert manifest.retrieved_at.tzinfo is not None

    def test_manifest_to_json(self) -> None:
        manifest = DatasetManifest(
            dataset_name="test",
            source="test",
            checksum="abc123",
        )
        json_str = manifest.to_json()
        assert "test" in json_str
        assert "abc123" in json_str

    def test_manifest_to_yaml(self) -> None:
        pytest = __import__("pytest")
        yaml = pytest.importorskip("yaml")
        manifest = DatasetManifest(
            dataset_name="test",
            source="test",
            checksum="abc123",
        )
        yaml_str = manifest.to_yaml()
        assert "dataset_name" in yaml_str


class TestProvenanceCoverage:
    """Tests for provenance paths not exercised by main tests."""

    def test_record_dataset_manifest(self, tmp_path: Path) -> None:
        recorder = ProvenanceRecorder()
        manifest_path = tmp_path / "manifest.yaml"
        manifest_path.write_text("test")
        obs = recorder.record_dataset_manifest(
            manifest_path=manifest_path,
            source="udd",
            ingestion_run_id="run-005",
        )
        assert obs.entity_type == "dataset_manifest"
        assert obs.source == "udd"

    def test_artifact_path_string(self) -> None:
        artifact = RawArtifact(
            artifact_id="art-1",
            source="udd",
            path="/tmp/test.json",  # string, not Path
            checksum="sha256:abc",
            size_bytes=100,
        )
        assert isinstance(artifact.path, Path)

    def test_artifact_naive_datetime(self) -> None:
        from datetime import datetime
        artifact = RawArtifact(
            artifact_id="art-1",
            source="udd",
            path=Path("/tmp/test.json"),
            checksum="sha256:abc",
            size_bytes=100,
            acquired_at=datetime(2026, 1, 1, 12, 0, 0),  # naive
        )
        assert artifact.acquired_at.tzinfo is not None

    def test_artifact_non_utc_datetime(self) -> None:
        from datetime import datetime, timedelta, timezone
        cet = timezone(timedelta(hours=1))
        artifact = RawArtifact(
            artifact_id="art-1",
            source="udd",
            path=Path("/tmp/test.json"),
            checksum="sha256:abc",
            size_bytes=100,
            acquired_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=cet),
        )
        assert artifact.acquired_at.tzinfo == timezone.utc

    def test_artifact_invalid_path_type(self) -> None:
        with pytest.raises(ValueError, match="path must be a string or Path"):
            RawArtifact(
                artifact_id="art-1",
                source="udd",
                path=123,  # invalid type
                checksum="sha256:abc",
                size_bytes=100,
            )

    def test_artifact_invalid_checksum(self) -> None:
        with pytest.raises(ValueError):
            RawArtifact(
                artifact_id="art-1",
                source="udd",
                path=Path("/tmp/test.json"),
                checksum=123,  # invalid type
                size_bytes=100,
            )


class TestPipelineCoverage:
    """Tests for pipeline edge cases."""

    def test_ingestion_result_path_validator(self) -> None:
        # Valid string path
        result = IngestionResult(
            ingestion_run_id="run-1",
            source_name="test",
            record_count=1,
            checksum="abc",
            manifest=DatasetManifest(dataset_name="test", source="test"),
            artifact_path="/tmp/test.json",
        )
        assert isinstance(result.artifact_path, Path)

    def test_ingestion_result_path_none(self) -> None:
        result = IngestionResult(
            ingestion_run_id="run-1",
            source_name="test",
            record_count=1,
            checksum="abc",
            manifest=DatasetManifest(dataset_name="test", source="test"),
            artifact_path=None,
        )
        assert result.artifact_path is None

    def test_ingestion_result_naive_datetime(self) -> None:
        from datetime import datetime
        result = IngestionResult(
            ingestion_run_id="run-1",
            source_name="test",
            record_count=1,
            checksum="abc",
            manifest=DatasetManifest(dataset_name="test", source="test"),
            acquired_at=datetime(2026, 1, 1, 12, 0, 0),  # naive
        )
        assert result.acquired_at.tzinfo is not None

    def test_ingestion_result_invalid_path(self) -> None:
        with pytest.raises(ValueError, match="artifact_path must be a string or Path"):
            IngestionResult(
                ingestion_run_id="run-1",
                source_name="test",
                record_count=1,
                checksum="abc",
                manifest=DatasetManifest(dataset_name="test", source="test"),
                artifact_path=123,  # invalid type
            )

    def test_data_source_default_record_count(self) -> None:
        """Test the default record_count() implementation in DataSource."""
        class MinimalSource(DataSource):
            @property
            def name(self) -> str:
                return "minimal"

            def retrieve(self) -> list[dict[str, Any]]:
                return [{"a": 1}, {"a": 2}]

            def compute_checksum(self) -> str:
                return "test"

        source = MinimalSource()
        assert source.record_count() == 2


class TestAcquisitionCoverage:
    """Tests for acquisition edge cases."""

    def test_in_memory_source_empty_name(self) -> None:
        # InMemoryDataSource does not validate name in __init__;
        # validation happens at manifest/provenance layer.
        # This test documents that behavior.
        source = InMemoryDataSource(name="", records=[])
        assert source.name == ""

    def test_mock_source_invalid_count(self) -> None:
        with pytest.raises(ValueError):
            MockDebianDataSource(package_count=0)
        with pytest.raises(ValueError):
            MockDebianDataSource(package_count=1, versions_per_package=0)
