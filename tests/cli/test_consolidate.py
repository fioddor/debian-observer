"""CLI tests for the `consolidate` command.

`consolidate` folds the append-only event log (Parquet, one file per
ingestion run) into the consolidated ``package_versions`` table:
one re-generable file, written atomically (tmp file + rename), at a
fixed name inside ``--dataset-dir`` (default
``data/normalized/package_versions``).
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq
from click.testing import CliRunner

from debian_observer.cli.main import cli
from debian_observer.events import EventEnvelope
from debian_observer.storage.parquet import ParquetWriter

NOW = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc)


def _pvo_event(
    *, release: str = "sid", observed_at: datetime = NOW
) -> EventEnvelope:
    return EventEnvelope(
        event_id=uuid4(),
        event_type="PackageVersionObserved",
        source="udd",
        entity_id="vim:amd64",
        payload={
            "package_id": "vim:amd64",
            "source_package": "vim",
            "version": "2:9.0-1",
            "distribution": "debian",
            "release": release,
            "suite": None,
            "architecture": "amd64",
            "component": "main",
            "section": "editors",
            "priority": "optional",
        },
        observed_at=observed_at,
        effective_at=observed_at,
    )


def _events_dir(tmp_path: Path) -> Path:
    events_dir = tmp_path / "events"
    events_dir.mkdir()
    writer = ParquetWriter(output_dir=events_dir)
    writer.write_events(
        [_pvo_event(release="sid"), _pvo_event(release="bookworm")],
        ingestion_run_id="run-1",
    )
    return events_dir


def test_consolidate_requires_events_dir():
    """--events-dir is required: absent → usage error (exit 2)."""
    runner = CliRunner()
    result = runner.invoke(cli, ["consolidate"])

    assert result.exit_code == 2
    assert "--events-dir" in result.output
    assert "Traceback" not in result.output


def test_consolidate_writes_the_dataset(tmp_path: Path):
    runner = CliRunner()
    events_dir = _events_dir(tmp_path)
    dataset_dir = tmp_path / "dataset"

    result = runner.invoke(
        cli,
        [
            "consolidate",
            "--events-dir", str(events_dir),
            "--dataset-dir", str(dataset_dir),
        ],
    )

    assert result.exit_code == 0
    # The echo: files, events, PVO events, rows, output path.
    assert "Files read: 1" in result.output
    assert "Events read: 2" in result.output
    assert "2 PackageVersionObserved events" in result.output
    assert "Consolidated rows: 2" in result.output
    assert str(dataset_dir / "package_versions.parquet") in result.output

    out = dataset_dir / "package_versions.parquet"
    assert out.exists()
    # Atomic: no tmp leftovers.
    assert not list(dataset_dir.glob("*.tmp"))
    assert not list(dataset_dir.glob(".*"))
    assert [p.name for p in dataset_dir.iterdir()] == [
        "package_versions.parquet"
    ]


def test_consolidate_default_dataset_dir(tmp_path: Path, monkeypatch):
    """--dataset-dir defaults to data/normalized/package_versions."""
    runner = CliRunner()
    events_dir = _events_dir(tmp_path)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        cli, ["consolidate", "--events-dir", str(events_dir)]
    )

    assert result.exit_code == 0
    out = (
        tmp_path
        / "data"
        / "normalized"
        / "package_versions"
        / "package_versions.parquet"
    )
    assert out.exists()


def test_consolidate_is_regenerable(tmp_path: Path):
    """A second run over the same log rewrites the identical table."""
    runner = CliRunner()
    events_dir = _events_dir(tmp_path)
    dataset_dir = tmp_path / "dataset"
    args = [
        "consolidate",
        "--events-dir", str(events_dir),
        "--dataset-dir", str(dataset_dir),
    ]

    first = runner.invoke(cli, args)
    assert first.exit_code == 0
    out = dataset_dir / "package_versions.parquet"
    first_rows = pq.read_table(out).to_pylist()

    second = runner.invoke(cli, args)
    assert second.exit_code == 0
    assert pq.read_table(out).to_pylist() == first_rows
    # Still one file — regeneration, not accumulation.
    assert [p.name for p in dataset_dir.iterdir()] == [
        "package_versions.parquet"
    ]


def test_consolidate_empty_log_is_a_usage_error(tmp_path: Path):
    """An events dir without Parquet files fails naming the dir."""
    runner = CliRunner()
    empty_dir = tmp_path / "events"
    empty_dir.mkdir()

    result = runner.invoke(
        cli,
        [
            "consolidate",
            "--events-dir", str(empty_dir),
            "--dataset-dir", str(tmp_path / "dataset"),
        ],
    )

    assert result.exit_code != 0
    assert str(empty_dir) in result.output
    assert "Traceback" not in result.output


def test_consolidate_help_lists_the_options():
    runner = CliRunner()
    result = runner.invoke(cli, ["consolidate", "--help"])

    assert result.exit_code == 0
    assert "--events-dir" in result.output
    assert "--dataset-dir" in result.output
    # The default is surfaced (CONTRIBUTING --help rule).
    assert "data/normalized/package_versions" in result.output