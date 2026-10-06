"""CLI tests for replay command."""

from __future__ import annotations
from pathlib import Path
from unittest.mock import patch
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

    # Mock _publish_events to avoid NATS connection in test
    with patch("debian_observer.cli.commands.replay._publish_events") as mock_pub:
        result = runner.invoke(cli, ["replay", "--input", str(event_file)])
        assert result.exit_code == 0
        assert "Loaded 2 events" in result.output
        mock_pub.assert_called_once()


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

    # Mock _publish_events to verify it was called with correct args
    with patch("debian_observer.cli.commands.replay._publish_events") as mock_pub:
        result = runner.invoke(cli, ["replay", "--input", str(event_file)])
        assert result.exit_code == 0
        assert "Loaded 2 events" in result.output
        mock_pub.assert_called_once()
        # Verify args: events list, nats_url, create_stream=False
        _, kwargs = mock_pub.call_args
        assert len(kwargs) == 0  # all positional
        call_args = mock_pub.call_args[0]
        assert len(call_args) == 3
        assert call_args[1] == "nats://localhost:4222"  # nats_url
        assert call_args[2] is False  # create_stream


def test_replay_empty_file(tmp_path: Path):
    runner = CliRunner()
    event_file = tmp_path / "empty.jsonl"
    event_file.write_text("", encoding="utf-8")

    # No NATS call needed for empty file (returns before _publish_events)
    result = runner.invoke(cli, ["replay", "--input", str(event_file)])
    assert result.exit_code == 0
    assert "Loaded 0 events" in result.output


def test_replay_with_real_event_envelope(tmp_path: Path):
    """Lightweight integration: serialize real EventEnvelope to JSONL, replay parses it.

    Validates round-trip serialization/deserialization without mocks.
    """
    from debian_observer.events import EventEnvelope
    from debian_observer.events.types import ObservationMeta, PackageVersionObservedPayload

    runner = CliRunner()
    event_file = tmp_path / "real-events.jsonl"

    event = EventEnvelope.from_payload(
        event_type="PackageVersionObserved",
        source="udd",
        entity_id="hello:amd64",
        payload_model=PackageVersionObservedPayload(
            package_id="hello:amd64",
            version="2.10-3",
            distribution="sid",
            architecture="amd64",
            component="main",
            priority="optional",
            observation=ObservationMeta(source="udd"),
        ),
        ingestion_run_id="run-real-001",
    )

    # Serialize via model_dump_json for proper JSON-lines format
    event_file.write_text(event.model_dump_json() + "\n", encoding="utf-8")

    # Mock _publish_events to avoid NATS connection in test
    with patch("debian_observer.cli.commands.replay._publish_events") as mock_pub:
        result = runner.invoke(cli, ["replay", "--input", str(event_file)])
        assert result.exit_code == 0
        assert "Loaded 1 events" in result.output
        mock_pub.assert_called_once()
