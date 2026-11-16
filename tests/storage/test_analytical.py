"""Tests for storage/analytical module."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from debian_observer.events import EventEnvelope
from debian_observer.storage.analytical import AnalyticalStorage
from debian_observer.storage.parquet import ParquetWriter


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
                    "distribution": "debian",
                    "release": "sid",
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
                    "distribution": "debian",
                    "release": "bookworm",
                },
                observed_at=now,
                effective_at=now,
            ),
        ]
        writer.write_events(events, ingestion_run_id="run-001")

        analytics = AnalyticalStorage(tmp_path)
        result = analytics.query(
            "SELECT release, COUNT(*) as n FROM events GROUP BY release ORDER BY release"
        )
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
            )
            for _ in range(5)
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
            )
            for _ in range(3)
        ]
        writer.write_events(events, ingestion_run_id="run-003")

        analytics = AnalyticalStorage(tmp_path)
        summary = analytics.summary()
        assert summary["files"] == 1
        assert summary["events"] == 3
        assert summary["types"]["PackageVersionObserved"] == 3


    # -- dataset views: one per directory, named from the directory ----

    def _two_runs_same_schema(self, tmp_path: Path) -> Path:
        """A dataset directory with two shards of ONE schema."""
        dataset = tmp_path / "package_versions"
        dataset.mkdir()
        writer = ParquetWriter(output_dir=dataset)
        now = datetime.now(timezone.utc)
        for run in ("run-a", "run-b"):
            writer.write_events(
                [
                    EventEnvelope(
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
                ],
                ingestion_run_id=run,
            )
        return dataset

    def test_view_named_after_directory(self, tmp_path: Path):
        """Two shards, one schema: ONE view, named from the directory."""
        dataset = self._two_runs_same_schema(tmp_path)
        analytics = AnalyticalStorage(tmp_path / "no-events", dataset)

        described = analytics.describe()
        assert list(described) == ["package_versions"], (
            "describe must list the directory, not the shards"
        )
        n = analytics.query(
            "SELECT COUNT(*) AS n FROM package_versions"
        ).fetchone()
        assert n[0] == 2, "both shards must read as one table"

    def test_describe_names_are_from_names(self, tmp_path: Path):
        """What describe lists is exactly what goes in FROM."""
        dataset = self._two_runs_same_schema(tmp_path)
        analytics = AnalyticalStorage(tmp_path / "no-events", dataset)

        for name in analytics.describe():
            cols = analytics.query(
                f"SELECT COUNT(*) AS n FROM {name}"
            ).fetchone()
            assert cols is not None, f"FROM {name} must resolve"
