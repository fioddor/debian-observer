"""Tests for chained prosumer pipelines.

These test the exact pipeline patterns requested by the assignment,
using subprocess with shell pipes to avoid pytest stdin capture.
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

from debian_observer.prosumers.stream import FileSource, StreamSource


def make_parquet(path: Path, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)


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
    # Different package, different distribution
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
    # Another debian package
    {
        "event_id": "e4",
        "event_type": "PackageVersionObserved",
        "observed_at": "2026-10-01T12:00:00Z",
        "effective_at": "2026-10-01T12:00:00Z",
        "ingestion_run_id": "r2",
        "package_id": "pkg-c",
        "source_package": "pkg-c-src",
        "version": "3.0",
        "distribution": "debian",
        "release": "bookworm",
        "suite": "stable",
        "architecture": "arm64",
        "component": "main",
        "section": "libs",
        "priority": "optional",
        "source": "mirror",
    },
]


class TestChainConsolidateThenFilter:
    """file → consolidate → stream → filter → file (via shell pipe)"""

    def test_chain(self, tmp_path: Path) -> None:
        in_file = tmp_path / "events.parquet"
        out_file = tmp_path / "out.parquet"
        make_parquet(in_file, EVENT_ROWS)

        env = dict(os.environ, PYTHONPATH="src")
        cmd = (
            f"{sys.executable} src/debian_observer/prosumers/consolidate.py --file-in {in_file} --stream-out | "
            f"{sys.executable} src/debian_observer/prosumers/filter.py --stream-in --file-out {out_file} --distribution debian"
        )
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode == 0, result.stderr
        assert out_file.exists()

        with FileSource(out_file) as source:
            records = list(source)
        # Consolidated: pkg-a (1 row) + pkg-b (1) + pkg-c (1) = 3
        # Filter debian: pkg-a (debian) + pkg-c (debian) = 2 (pkg-b is ubuntu)
        assert len(records) == 2
        pkg_ids = {r.payload["package_id"] for r in records}
        assert pkg_ids == {"pkg-a", "pkg-c"}


class TestChainFilterThenFilter:
    """file → filter → stream → filter → stream (via shell pipe)"""

    def test_chain(self, tmp_path: Path) -> None:
        in_file = tmp_path / "events.parquet"
        make_parquet(in_file, EVENT_ROWS)

        env = dict(os.environ, PYTHONPATH="src")
        cmd = (
            f"{sys.executable} src/debian_observer/prosumers/filter.py --file-in {in_file} --stream-out --distribution debian | "
            f"{sys.executable} src/debian_observer/prosumers/filter.py --stream-in --stream-out --release sid --release bookworm"
        )
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode == 0, result.stderr

        # Parse final ND-JSON stdout
        buf = StringIO(result.stdout)
        with StreamSource(buf) as source:
            records = list(source)
        # After first filter: pkg-a (debian/sid) + pkg-c (debian/bookworm) = 2
        # Second filter keeps both (sid + bookworm) = 2
        assert len(records) == 2
        releases = {r.payload["release"] for r in records}
        assert releases == {"sid", "bookworm"}
