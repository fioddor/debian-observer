"""CLI tests for replay command."""

from __future__ import annotations
from pathlib import Path
from click.testing import CliRunner

from debian_observer.cli.main import cli


def test_replay_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["replay", "--help"])
    assert result.exit_code == 0
    assert "--input" in result.output
    assert "--dry-run" in result.output


def _write_events_file(path: Path, lines: list[dict]) -> None:
    """Write JSON-lines event file for replay testing."""
    import json
    with open(path, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(json.dumps(line) + "\n")


def test_replay_loads_events(tmp_path: Path):
    runner = CliRunner()
    event_file = tmp_path / "events.jsonl"
    _write_events_file(event_file, [
        {
            "event_id": "550e8400-e29b-41d4-a716-446655440000",
            "event_type": "PackageVersionObserved",
            "schema_version": 1,
            "source": "udd",
            "observed_at": "2024-01-01T00:00:00Z",
            "effective_at": "2024-01-01T00:00:00Z",
            "entity_id": "pkg-1",
            "ingestion_run_id": "run-001",
            "payload": {"package_id": "pkg-1", "version": "1.0-1"},
        },
        {
            "event_id": "550e8400-e29b-41d4-a716-446655440001",
            "event_type": "PackageVersionObserved",
            "schema_version": 1,
            "source": "udd",
            "observed_at": "2024-01-01T00:00:00Z",
            "effective_at": "2024-01-01T00:00:00Z",
            "entity_id": "pkg-2",
            "ingestion_run_id": "run-001",
            "payload": {"package_id": "pkg-2", "version": "2.0-1"},
        },
    ])

    result = runner.invoke(cli, ["replay", "--input", str(event_file)])
    assert result.exit_code == 0
    assert "Loaded 2 events" in result.output


def test_replay_dry_run(tmp_path: Path):
    runner = CliRunner()
    event_file = tmp_path / "events.jsonl"
    _write_events_file(event_file, [
        {
            "event_id": "550e8400-e29b-41d4-a716-446655440000",
            "event_type": "PackageVersionObserved",
            "schema_version": 1,
            "source": "udd",
            "observed_at": "2024-01-01T00:00:00Z",
            "effective_at": "2024-01-01T00:00:00Z",
            "entity_id": "pkg-1",
            "ingestion_run_id": "run-001",
            "payload": {"package_id": "pkg-1", "version": "1.0-1"},
        },
    ])

    result = runner.invoke(cli, ["replay", "--input", str(event_file), "--dry-run"])
    assert result.exit_code == 0
    assert "Loaded 1 events" in result.output
    assert "Dry run" in result.output
    assert "no events published" in result.output


def test_replay_publish_output(tmp_path: Path):
    runner = CliRunner()
    event_file = tmp_path / "events.jsonl"
    _write_events_file(event_file, [
        {
            "event_id": "550e8400-e29b-41d4-a716-446655440000",
            "event_type": "PackageVersionObserved",
            "schema_version": 1,
            "source": "udd",
            "observed_at": "2024-01-01T00:00:00Z",
            "effective_at": "2024-01-01T00:00:00Z",
            "entity_id": "pkg-1",
            "ingestion_run_id": "run-001",
            "payload": {"package_id": "pkg-1", "version": "1.0-1"},
        },
        {
            "event_id": "550e8400-e29b-41d4-a716-446655440001",
            "event_type": "PackageVersionObserved",
            "schema_version": 1,
            "source": "udd",
            "observed_at": "2024-01-01T00:00:00Z",
            "effective_at": "2024-01-01T00:00:00Z",
            "entity_id": "pkg-2",
            "ingestion_run_id": "run-001",
            "payload": {"package_id": "pkg-2", "version": "2.0-1"},
        },
    ])

    result = runner.invoke(cli, ["replay", "--input", str(event_file)])
    assert result.exit_code == 0
    assert "Would publish 2 events" in result.output
    assert "nats://localhost:4222" in result.output


def test_replay_empty_file(tmp_path: Path):
    runner = CliRunner()
    event_file = tmp_path / "empty.jsonl"
    event_file.write_text("", encoding="utf-8")

    result = runner.invoke(cli, ["replay", "--input", str(event_file)])
    assert result.exit_code == 0
    assert "Loaded 0 events" in result.output
