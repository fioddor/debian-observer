"""Tests for storage/parquet module."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from debian_observer.events import EventEnvelope
from debian_observer.storage.parquet import ParquetWriter


class TestParquetWriter:
    def test_write_events_creates_file(self, tmp_path: Path):
        writer = ParquetWriter(output_dir=tmp_path)
        now = datetime.now(timezone.utc)
        event = EventEnvelope(
            event_type="PackageVersionObserved",
            source="udd",
            entity_id="vim:amd64",
            payload={
                "package_id": "vim:amd64",
                "version": "2:9.0-1",
                "distribution": "debian",
                "release": "sid",
                "architecture": "amd64",
                "component": "main",
                "priority": "optional",
            },
            observed_at=now,
            effective_at=now,
        )
        path = writer.write_events([event], ingestion_run_id="run-test-001")
        assert path.exists()
        assert path.suffix == ".parquet"

    def test_written_file_contains_data(self, tmp_path: Path):
        writer = ParquetWriter(output_dir=tmp_path)
        now = datetime.now(timezone.utc)
        event = EventEnvelope(
            event_type="PackageVersionObserved",
            source="udd",
            entity_id="vim:amd64",
            payload={
                "package_id": "vim:amd64",
                "version": "2:9.0-1",
                "distribution": "debian",
                "release": "sid",
            },
            observed_at=now,
            effective_at=now,
        )
        path = writer.write_events([event])
        table = pq.read_table(path)
        assert table.num_rows == 1
        assert table.column_names == [
            "event_id",
            "event_type",
            "source",
            "entity_id",
            "observed_at",
            "effective_at",
            "ingestion_run_id",
            "package_id",
            "source_package",
            "version",
            "distribution",
            "release",
            "architecture",
            "component",
            "section",
            "priority",
            "suite",
        ]

    def test_empty_events_raises(self, tmp_path: Path):
        writer = ParquetWriter(output_dir=tmp_path)
        with pytest.raises(ValueError, match="empty"):
            writer.write_events([])

    def test_list_files(self, tmp_path: Path):
        writer = ParquetWriter(output_dir=tmp_path)
        now = datetime.now(timezone.utc)
        event = EventEnvelope(
            event_type="PackageVersionObserved",
            source="udd",
            entity_id="a:all",
            payload={"package_id": "a:all", "version": "1.0"},
            observed_at=now,
            effective_at=now,
        )
        writer.write_events([event], ingestion_run_id="run-001")
        writer.write_events([event], ingestion_run_id="run-002")
        files = writer.list_files()
        assert len(files) == 2


class TestParquetExtendedFactColumns:
    """Parquet output carries release/section/source_package columns."""

    def test_written_row_contains_extended_fields(self, tmp_path: Path):
        writer = ParquetWriter(output_dir=tmp_path)
        now = datetime.now(timezone.utc)
        event = EventEnvelope(
            event_type="PackageVersionObserved",
            source="udd",
            entity_id="librte-dpdk:amd64",
            payload={
                "package_id": "librte-dpdk:amd64",
                "source_package": "dpdk",
                "version": "25.11.3-1",
                "distribution": "debian",
                "release": "bullseye-security",
                "architecture": "amd64",
                "component": "main",
                "section": "libs",
                "priority": "optional",
            },
            observed_at=now,
            effective_at=now,
        )
        path = writer.write_events([event], ingestion_run_id="run-ext-001")
        table = pq.read_table(path)
        row = table.to_pylist()[0]
        assert row["source_package"] == "dpdk"
        assert row["release"] == "bullseye-security"
        assert row["section"] == "libs"


class TestParquetSuiteColumn:
    """Parquet output carries the suite role column."""

    def test_written_row_contains_suite(self, tmp_path: Path):
        writer = ParquetWriter(output_dir=tmp_path)
        now = datetime.now(timezone.utc)
        event = EventEnvelope(
            event_type="PackageVersionObserved",
            source="debian.mirror",
            entity_id="vim:amd64",
            payload={
                "package_id": "vim:amd64",
                "version": "2:9.0-1",
                "distribution": "debian",
                "release": "bookworm",
                "suite": "stable",
                "architecture": "amd64",
                "component": "main",
                "priority": "optional",
            },
            observed_at=now,
            effective_at=now,
        )
        path = writer.write_events([event], ingestion_run_id="run-suite-001")
        table = pq.read_table(path)
        row = table.to_pylist()[0]
        assert row["suite"] == "stable"
