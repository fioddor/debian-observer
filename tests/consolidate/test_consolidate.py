"""Tests for the consolidate prosumer.

These tests exercise the junction combinations (file/file, file/stream,
stream/file) and verify the consolidation fold engine.
"""

from __future__ import annotations

import os
import subprocess
import sys
from io import StringIO
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from prosumers.consolidate import main as consolidate_main
from debian_observer.prosumers.packages import ConsolidateError, fold_package_versions
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
    # Same package/version — later observation wins
    {
        "event_id": "e2",
        "event_type": "PackageVersionObserved",
        "observed_at": "2026-10-01T11:00:00Z",
        "effective_at": "2026-10-01T11:00:00Z",
        "ingestion_run_id": "r1",
        "package_id": "pkg-a",
        "source_package": "pkg-a",
        "version": "1.0",
        "distribution": "debian",
        "release": "sid",
        "suite": None,
        "architecture": "amd64",
        "component": "main",
        "section": "devel",
        "priority": "standard",
        "source": "mirror",
    },
    # Different package
    {
        "event_id": "e3",
        "event_type": "PackageVersionObserved",
        "observed_at": "2026-10-01T09:00:00Z",
        "effective_at": "2026-10-01T09:00:00Z",
        "ingestion_run_id": "r2",
        "package_id": "pkg-b",
        "source_package": "pkg-b",
        "version": "2.0",
        "distribution": "ubuntu",
        "release": "noble",
        "suite": "stable",
        "architecture": "all",
        "component": "universe",
        "section": "utils",
        "priority": "optional",
        "source": "udd",
    },
    # Duplicate event_id (same as e1, different observed_at impossible in reality,
    # but tests dedup by event_id regardless of other fields).
    {
        "event_id": "e1",
        "event_type": "PackageVersionObserved",
        "observed_at": "2026-10-01T12:00:00Z",
        "effective_at": "2026-10-01T12:00:00Z",
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
    # Non-package event — should be ignored
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


class TestFoldEngine:
    """Unit tests for ``fold_package_versions``."""

    def test_deduplicates_by_event_id(self) -> None:
        records = rows_to_records(EVENT_ROWS)
        rows, stats = fold_package_versions(records)
        # e1 duplicated → collapsed; e2, e3 survive; e4 ignored
        assert stats.pvo_events == 3
        assert stats.rows == 2  # pkg-a and pkg-b

    def test_last_observed_wins(self) -> None:
        records = rows_to_records(EVENT_ROWS)
        rows, _ = fold_package_versions(records)
        pkg_a = [r for r in rows if r["package_id"] == "pkg-a"][0]
        # e2 has later observed_at and different section/priority/source
        assert pkg_a["section"] == "devel"
        assert pkg_a["priority"] == "standard"
        assert pkg_a["source"] == "mirror"

    def test_first_last_at(self) -> None:
        records = rows_to_records(EVENT_ROWS)
        rows, _ = fold_package_versions(records)
        pkg_a = [r for r in rows if r["package_id"] == "pkg-a"][0]
        # e1 (10:00) was first, e2 (11:00) was last
        assert pkg_a["observed_first_at"] == "2026-10-01T10:00:00Z"
        assert pkg_a["observed_last_at"] == "2026-10-01T11:00:00Z"

    def test_no_package_events_raises(self) -> None:
        with pytest.raises(ConsolidateError, match="No PackageVersionObserved"):
            fold_package_versions([])


class TestConsolidateCLI:
    """CLI integration tests using Click's CliRunner.

    Stream-input tests use subprocess to avoid pytest's stdin capture
    ("reading from stdin while output is captured").
    """

    def test_file_in_file_out(self, tmp_path: Path) -> None:
        in_file = tmp_path / "events.parquet"
        out_file = tmp_path / "consolidated.parquet"
        make_parquet(in_file, EVENT_ROWS)

        runner = CliRunner()
        result = runner.invoke(
            consolidate_main,
            ["--file-in", str(in_file), "--file-out", str(out_file)],
        )
        assert result.exit_code == 0, result.output
        assert out_file.exists()

        with FileSource(out_file) as source:
            records = list(source)
        assert len(records) == 2

    def test_file_in_stream_out(self, tmp_path: Path) -> None:
        in_file = tmp_path / "events.parquet"
        make_parquet(in_file, EVENT_ROWS)

        runner = CliRunner()
        result = runner.invoke(
            consolidate_main,
            ["--file-in", str(in_file), "--stream-out"],
        )
        assert result.exit_code == 0, result.output
        lines = result.output.strip().split("\n")
        ndjson_lines = [l for l in lines if l.startswith("{")]
        assert len(ndjson_lines) == 2

    def test_stream_in_file_out(self, tmp_path: Path) -> None:
        out_file = tmp_path / "consolidated.parquet"
        records = rows_to_records(EVENT_ROWS)
        buf = StringIO()
        with StreamSink(buf) as sink:
            for rec in records:
                sink.emit(rec)

        # pytest captures stdin → run CLI in subprocess instead
        # (inherit the real environment: PATH etc. must survive)
        env = dict(os.environ, PYTHONPATH="src")
        result = subprocess.run(
            [sys.executable, "src/prosumers/consolidate.py", "--stream-in", "--file-out", str(out_file)],
            input=buf.getvalue(),
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode == 0, result.stderr
        assert out_file.exists()

        with FileSource(out_file) as source:
            read_back = list(source)
        assert len(read_back) == 2
