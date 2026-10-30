"""Tests for the filter prosumer.

These tests exercise the junction combinations and verify the AND
logic of distribution + release filters.
"""

from __future__ import annotations

import subprocess
import sys
from io import StringIO
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from prosumers.filter import main as filter_main
from debian_observer.prosumers.stream import FileSink, FileSource, Record, StreamSink, StreamSource


def make_parquet(path: Path, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)


def rows_to_records(rows: list[dict]) -> list[Record]:
    records = []
    for row in rows:
        meta_cols = {"event_id", "event_type", "observed_at", "effective_at", "ingestion_run_id"}
        meta = {k: row[k] for k in meta_cols}
        payload = {k: v for k, v in row.items() if k not in meta_cols}
        records.append(Record(event_type=row["event_type"], payload=payload, meta=meta))
    return records


EVENT_ROWS = [
    {
        "event_id": "e1",
        "event_type": "PackageVersionObserved",
        "observed_at": "2026-10-01T10:00:00Z",
        "effective_at": "2026-10-01T10:00:00Z",
        "ingestion_run_id": "r1",
        "package_id": "pkg-a",
        "source_package": "pkg-a",
        "version": "1.0",
        "distribution": "debian",
        "release": "sid",
        "suite": None,
        "architecture": "amd64",
        "component": "main",
        "section": "utils",
        "priority": "optional",
        "source": "udd",
    },
    {
        "event_id": "e2",
        "event_type": "PackageVersionObserved",
        "observed_at": "2026-10-01T11:00:00Z",
        "effective_at": "2026-10-01T11:00:00Z",
        "ingestion_run_id": "r1",
        "package_id": "pkg-b",
        "source_package": "pkg-b",
        "version": "2.0",
        "distribution": "debian",
        "release": "bookworm",
        "suite": "stable",
        "architecture": "all",
        "component": "main",
        "section": "utils",
        "priority": "optional",
        "source": "mirror",
    },
    {
        "event_id": "e3",
        "event_type": "PackageVersionObserved",
        "observed_at": "2026-10-01T12:00:00Z",
        "effective_at": "2026-10-01T12:00:00Z",
        "ingestion_run_id": "r2",
        "package_id": "pkg-c",
        "source_package": "pkg-c",
        "version": "3.0",
        "distribution": "ubuntu",
        "release": "noble",
        "suite": "stable",
        "architecture": "amd64",
        "component": "universe",
        "section": "utils",
        "priority": "optional",
        "source": "udd",
    },
    # Non-package event — should pass through regardless of filters
    {
        "event_id": "e4",
        "event_type": "HealthCheck",
        "observed_at": "2026-10-01T13:00:00Z",
        "effective_at": "2026-10-01T13:00:00Z",
        "ingestion_run_id": "r2",
        "package_id": None,
        "source_package": None,
        "version": None,
        "distribution": None,
        "release": None,
        "suite": None,
        "architecture": None,
        "component": None,
        "section": None,
        "priority": None,
        "source": None,
    },
]


class TestFilterEngine:
    """Unit tests for the filter logic."""

    def test_match_distribution_only(self) -> None:
        from prosumers.filter import _match
        rec = rows_to_records(EVENT_ROWS)[0]  # debian / sid
        assert _match(rec, {"debian"}, set()) is True
        assert _match(rec, {"ubuntu"}, set()) is False

    def test_match_release_only(self) -> None:
        from prosumers.filter import _match
        rec = rows_to_records(EVENT_ROWS)[1]  # bookworm
        assert _match(rec, set(), {"bookworm"}) is True
        assert _match(rec, set(), {"sid"}) is False

    def test_match_both(self) -> None:
        from prosumers.filter import _match
        rec = rows_to_records(EVENT_ROWS)[0]  # debian / sid
        assert _match(rec, {"debian"}, {"sid"}) is True
        assert _match(rec, {"debian"}, {"bookworm"}) is False

    def test_no_filter_passes_through(self) -> None:
        from prosumers.filter import _match
        rec = rows_to_records(EVENT_ROWS)[0]
        assert _match(rec, set(), set()) is True

    def test_non_package_events_pass(self) -> None:
        from prosumers.filter import _match
        rec = rows_to_records(EVENT_ROWS)[3]  # HealthCheck
        assert _match(rec, {"debian"}, {"sid"}) is True


class TestFilterCLI:
    """CLI integration tests."""

    def test_filter_by_distribution_file_to_file(self, tmp_path: Path) -> None:
        in_file = tmp_path / "events.parquet"
        out_file = tmp_path / "filtered.parquet"
        make_parquet(in_file, EVENT_ROWS)

        runner = CliRunner()
        result = runner.invoke(
            filter_main,
            [
                "--file-in", str(in_file),
                "--file-out", str(out_file),
                "--distribution", "debian",
            ],
        )
        assert result.exit_code == 0, result.output
        with FileSource(out_file) as source:
            records = list(source)
        # pkg-a (debian) + pkg-b (debian) + HealthCheck = 3
        assert len(records) == 3

    def test_filter_by_release_file_to_file(self, tmp_path: Path) -> None:
        in_file = tmp_path / "events.parquet"
        out_file = tmp_path / "filtered.parquet"
        make_parquet(in_file, EVENT_ROWS)

        runner = CliRunner()
        result = runner.invoke(
            filter_main,
            [
                "--file-in", str(in_file),
                "--file-out", str(out_file),
                "--release", "sid",
            ],
        )
        assert result.exit_code == 0, result.output
        with FileSource(out_file) as source:
            records = list(source)
        # pkg-a (sid) + HealthCheck = 2
        assert len(records) == 2

    def test_filter_by_both_stream_to_stream(self) -> None:
        records = rows_to_records(EVENT_ROWS)
        in_buf = StringIO()
        with StreamSink(in_buf) as sink:
            for rec in records:
                sink.emit(rec)

        # pytest captures stdin → run CLI in subprocess instead
        import os
        env = dict(os.environ, PYTHONPATH="src")
        result = subprocess.run(
            [sys.executable, "src/prosumers/filter.py", "--stream-in", "--stream-out",
             "--distribution", "debian", "--release", "sid"],
            input=in_buf.getvalue(),
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode == 0, result.stderr
        out_buf = StringIO(result.stdout)
        with StreamSource(out_buf) as source:
            read_back = list(source)
        # pkg-a (debian+sid) + HealthCheck = 2
        assert len(read_back) == 2

    def test_multiple_distributions(self, tmp_path: Path) -> None:
        in_file = tmp_path / "events.parquet"
        out_file = tmp_path / "filtered.parquet"
        make_parquet(in_file, EVENT_ROWS)

        runner = CliRunner()
        result = runner.invoke(
            filter_main,
            [
                "--file-in", str(in_file),
                "--file-out", str(out_file),
                "--distribution", "debian",
                "--distribution", "ubuntu",
            ],
        )
        assert result.exit_code == 0, result.output
        with FileSource(out_file) as source:
            records = list(source)
        # All 3 packages + HealthCheck = 4
        assert len(records) == 4
