"""Tests for storage/analytical module."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from debian_observer.events import EventEnvelope
from debian_observer.storage.parquet import ParquetWriter
from debian_observer.storage.analytical import AnalyticalStorage


class TestAnalyticalStorage:
    def test_query_single_file(self, tmp_path: Path):
        writer = ParquetWriter(output_dir=tmp_path)
        now = datetime.now(timezone.utc)
        events = [
            EventEnvelope(
                event_type="PackageVersionObserved",
                source="udd",
                entity_id="vim:amd64",
                payload={
                    "package_id": "vim:amd64",
                    "version": "2:9.0-1",
                    "distribution": "sid",
                    "architecture": "amd64",
                },
                observed_at=now,
                effective_at=now,
            ),
            EventEnvelope(
                event_type="PackageVersionObserved",
                source="udd",
                entity_id="emacs:all",
                payload={
                    "package_id": "emacs:all",
                    "version": "1:28.2+1",
                    "distribution": "bookworm",
                },
                observed_at=now,
                effective_at=now,
            ),
        ]
        writer.write_events(events, ingestion_run_id="run-001")

        analytics = AnalyticalStorage(tmp_path)
        result = analytics.query("SELECT distribution, COUNT(*) as n FROM events GROUP BY distribution ORDER BY distribution")
        rows = result.fetchall()
        assert len(rows) == 2
        assert {row[0] for row in rows} == {"sid", "bookworm"}

    def test_count_events(self, tmp_path: Path):
        writer = ParquetWriter(output_dir=tmp_path)
        now = datetime.now(timezone.utc)
        events = [
            EventEnvelope(
                event_type="PackageVersionObserved",
                source="udd",
                entity_id="a:all",
                payload={"package_id": "a:all", "version": "1.0"},
                observed_at=now,
                effective_at=now,
            ) for _ in range(5)
        ]
        writer.write_events(events, ingestion_run_id="run-002")

        analytics = AnalyticalStorage(tmp_path)
        assert analytics.count_events() == 5

    def test_no_files_raises(self, tmp_path: Path):
        analytics = AnalyticalStorage(tmp_path)
        with pytest.raises(RuntimeError, match="No Parquet"):
            analytics.count_events()

    def test_summary(self, tmp_path: Path):
        writer = ParquetWriter(output_dir=tmp_path)
        now = datetime.now(timezone.utc)
        events = [
            EventEnvelope(
                event_type="PackageVersionObserved",
                source="udd",
                entity_id="a:all",
                payload={"package_id": "a:all", "version": "1.0"},
                observed_at=now,
                effective_at=now,
            ) for _ in range(3)
        ]
        writer.write_events(events, ingestion_run_id="run-003")

        analytics = AnalyticalStorage(tmp_path)
        summary = analytics.summary()
        assert summary["files"] == 1
        assert summary["events"] == 3
        assert summary["types"]["PackageVersionObserved"] == 3
