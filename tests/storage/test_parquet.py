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
                "distribution": "sid",
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
                "distribution": "sid",
            },
            observed_at=now,
            effective_at=now,
        )
        path = writer.write_events([event])
        table = pq.read_table(path)
        assert table.num_rows == 1
        assert table.column_names == [
            "event_id", "event_type", "source", "entity_id",
            "observed_at", "effective_at", "ingestion_run_id",
            "package_id", "version", "distribution",
            "architecture", "component", "priority",
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
