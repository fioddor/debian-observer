"""Tests for storage/dataset: the normalized package_versions dataset.

Spec §10.2 / ADR-003 reference ``data/normalized/package_versions/*.parquet``
as the analytical surface. ADR-011 assigns its serialization to the
storage layer via PackageVersionDatasetWriter.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from debian_observer.domain.models import PackageVersion
from debian_observer.events import EventEnvelope
from debian_observer.storage.dataset import PackageVersionDatasetWriter


def _pv_event(
    *,
    package_id: str = "vim:amd64",
    version: str = "2:9.0-1",
    release: str | None = "sid",
    suite: str | None = None,
) -> EventEnvelope:
    now = datetime.now(timezone.utc)
    return EventEnvelope(
        event_type="PackageVersionObserved",
        source="udd",
        entity_id=package_id,
        payload={
            "package_id": package_id,
            "source_package": "vim",
            "version": version,
            "distribution": "debian",
            "release": release,
            "suite": suite,
            "architecture": "amd64",
            "component": "main",
            "section": "editors",
            "priority": "optional",
        },
        observed_at=now,
        effective_at=now,
    )


class TestPackageVersionDatasetWriter:
    def test_write_events_creates_file(self, tmp_path: Path):
        writer = PackageVersionDatasetWriter(output_dir=tmp_path)
        path = writer.write_events([_pv_event()], ingestion_run_id="run-ds-001")
        assert path.exists()
        assert path.suffix == ".parquet"
        assert path.name == "run-ds-001.parquet"

    def test_schema_tracks_package_version_model(self, tmp_path: Path):
        # Anti-trap (ADR-011): the dataset must project every PackageVersion
        # field. A new domain field without a dataset column turns this red.
        writer = PackageVersionDatasetWriter(output_dir=tmp_path)
        path = writer.write_events([_pv_event()])
        table = pq.read_table(path)
        assert set(table.column_names) == set(PackageVersion.model_fields.keys())
        assert table.num_rows == 1

    def test_rows_carry_archive_semantics(self, tmp_path: Path):
        writer = PackageVersionDatasetWriter(output_dir=tmp_path)
        path = writer.write_events(
            [_pv_event(release="bullseye-security", suite="oldstable")]
        )
        row = pq.read_table(path).to_pylist()[0]
        assert row["source_package"] == "vim"
        assert row["release"] == "bullseye-security"
        assert row["suite"] == "oldstable"
        assert row["section"] == "editors"

    def test_observed_at_is_envelope_iso_string(self, tmp_path: Path):
        writer = PackageVersionDatasetWriter(output_dir=tmp_path)
        path = writer.write_events([_pv_event()])
        row = pq.read_table(path).to_pylist()[0]
        # Envelope observed_at, ISO string, UTC — not a payload echo.
        assert isinstance(row["observed_at"], str)
        assert row["observed_at"].endswith("+00:00")

    def test_non_package_version_events_are_projected_out(self, tmp_path: Path):
        writer = PackageVersionDatasetWriter(output_dir=tmp_path)
        now = datetime.now(timezone.utc)
        other = EventEnvelope(
            event_type="PackageObserved",
            source="udd",
            entity_id="vim",
            payload={"package_id": "vim", "name": "vim"},
            observed_at=now,
            effective_at=now,
        )
        path = writer.write_events([_pv_event(), other])
        table = pq.read_table(path)
        assert table.num_rows == 1
        assert "event_id" not in table.column_names

    def test_empty_projection_raises(self, tmp_path: Path):
        writer = PackageVersionDatasetWriter(output_dir=tmp_path)
        with pytest.raises(ValueError, match="empty"):
            writer.write_events([])

    def test_projection_of_only_non_pv_events_raises(self, tmp_path: Path):
        writer = PackageVersionDatasetWriter(output_dir=tmp_path)
        now = datetime.now(timezone.utc)
        other = EventEnvelope(
            event_type="PackageObserved",
            source="udd",
            entity_id="vim",
            payload={"package_id": "vim", "name": "vim"},
            observed_at=now,
            effective_at=now,
        )
        with pytest.raises(ValueError, match="empty"):
            writer.write_events([other])

    def test_spec_query_by_release(self, tmp_path: Path):
        # The ADR-003 / spec §10.2 example, executable on the real dataset.
        writer = PackageVersionDatasetWriter(output_dir=tmp_path)
        writer.write_events(
            [
                _pv_event(package_id="vim:amd64", version="2:9.0-1", release="sid"),
                _pv_event(package_id="bash:amd64", version="5.2-2", release="bookworm"),
            ],
            ingestion_run_id="run-q",
        )
        con = duckdb.connect(":memory:")
        rows = con.execute(
            f"SELECT package_id, version, release "
            f"FROM read_parquet('{tmp_path}/*.parquet') "
            f"WHERE release = 'sid'"
        ).fetchall()
        assert rows == [("vim:amd64", "2:9.0-1", "sid")]

    def test_list_files_sorted(self, tmp_path: Path):
        writer = PackageVersionDatasetWriter(output_dir=tmp_path)
        writer.write_events([_pv_event()], ingestion_run_id="run-a")
        writer.write_events([_pv_event()], ingestion_run_id="run-b")
        names = [p.name for p in writer.list_files()]
        assert names == ["run-a.parquet", "run-b.parquet"]
