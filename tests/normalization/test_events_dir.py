"""Tests for the per-event-type Parquet event log.

``--events-dir DIR`` writes the Parquet event log with **one
directory per event type** (snake_case of the type) and one file
per run. A reader of one event-type directory must read it with a
plain ``read_parquet`` glob, **without** ``union_by_name`` — and
column order drift within one type must not break the read.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest


import pytest
from datetime import datetime, timezone

from debian_observer.normalization.events import EventFactory


@pytest.fixture()
def sample_events():
    """Three PackageVersionObserved events."""
    factory = EventFactory("udddump", ingestion_run_id="run-001")
    from debian_observer.domain.models import PackageVersion
    versions = [
        PackageVersion(
            package_id=f"pkg{i}:amd64",
            source_package=f"src{i}",
            version=f"1.0.{i}",
            distribution="debian",
            release="trixie",
            suite=None,
            architecture="amd64",
            component="main",
            section="libs",
            priority="optional",
            observed_at=datetime.now(timezone.utc),
            source="udddump",
        )
        for i in range(3)
    ]
    return factory.from_package_versions(versions)

from debian_observer.storage.parquet import (
    event_type_directory,
    schema_for_event_type,
    writer_for_event_type,
)


class TestPerTypeDirectories:
    def test_event_type_directory_is_snake_case(self) -> None:
        assert (
            event_type_directory("PackageVersionObserved")
            == "package_version_observed"
        )
        assert (
            event_type_directory("DependencyObserved")
            == "dependency_observed"
        )
        assert event_type_directory("PopconObserved") == "popcon_observed"

    def test_schema_for_r1_type_is_the_default_schema(self) -> None:
        schema = schema_for_event_type("PackageVersionObserved")
        assert "package_id" in schema.names
        assert "version" in schema.names

    def test_schema_for_unknown_type_is_an_error(self) -> None:
        with pytest.raises(ValueError, match="NoSuchObserved"):
            schema_for_event_type("NoSuchObserved")

    def test_writer_writes_into_its_type_directory(self, tmp_path: Path) -> None:
        writer = writer_for_event_type(
            "PackageVersionObserved", events_dir=str(tmp_path)
        )
        path = tmp_path / "package_version_observed" / "run-001.parquet"
        assert writer.output_dir == path.parent


class TestPlainGlobRead:
    """The one-directory-one-schema contract (mandatory)."""

    def test_written_file_reads_with_a_plain_glob(
        self, tmp_path: Path, sample_events
    ) -> None:
        writer = writer_for_event_type(
            "PackageVersionObserved", events_dir=str(tmp_path)
        )
        written = writer.write_events(sample_events, ingestion_run_id="run-001")
        files = list(
            (tmp_path / "package_version_observed").glob("*.parquet")
        )
        assert written in files
        # The mandatory read: a plain glob over one event-type
        # directory, NO union_by_name.
        table = pq.read_table(
            [str(f) for f in files], schema=None
        )
        assert table.num_rows == len(sample_events)
        assert "package_id" in table.column_names

    def test_two_runs_in_one_type_directory_still_read(
        self, tmp_path: Path, sample_events
    ) -> None:
        writer = writer_for_event_type(
            "PackageVersionObserved", events_dir=str(tmp_path)
        )
        writer.write_events(sample_events, ingestion_run_id="run-001")
        writer.write_events(sample_events, ingestion_run_id="run-002")
        files = sorted(
            (tmp_path / "package_version_observed").glob("*.parquet")
        )
        assert len(files) == 2
        table = pq.read_table([str(f) for f in files])
        assert table.num_rows == 2 * len(sample_events)