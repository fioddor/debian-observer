"""CLI tests for status command."""

from __future__ import annotations
from unittest.mock import patch
from click.testing import CliRunner

from debian_observer.cli.main import cli


def test_status_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--help"])
    assert result.exit_code == 0
    assert "--parquet-dir" in result.output


def test_status_with_data():
    runner = CliRunner()
    mock_summary = {
        "files": 3,
        "events": 150,
        "types": {
            "PackageVersionObserved": 100,
            "PackageObserved": 50,
        },
    }

    with patch("debian_observer.cli.commands.status.AnalyticalStorage") as MockStorage:
        MockStorage.return_value.summary.return_value = mock_summary
        result = runner.invoke(cli, ["status"])

    assert result.exit_code == 0
    assert "Files: 3" in result.output
    assert "Events: 150" in result.output
    assert "PackageVersionObserved: 100" in result.output
    assert "PackageObserved: 50" in result.output


def test_status_empty_directory():
    runner = CliRunner()
    mock_summary = {"files": 0, "events": 0, "types": {}}

    with patch("debian_observer.cli.commands.status.AnalyticalStorage") as MockStorage:
        MockStorage.return_value.summary.return_value = mock_summary
        result = runner.invoke(cli, ["status", "--parquet-dir", "/tmp/empty"])

    assert result.exit_code == 0
    assert "Files: 0" in result.output
    assert "Events: 0" in result.output
    # No event types printed when empty
    assert "Event types:" not in result.output


def test_status_exception():
    runner = CliRunner()

    with patch("debian_observer.cli.commands.status.AnalyticalStorage") as MockStorage:
        MockStorage.return_value.summary.side_effect = RuntimeError("DuckDB failure")
        result = runner.invoke(cli, ["status"])

    assert result.exit_code == 0  # Click catches exception, writes to stderr
    assert "Status unavailable" in result.output
    assert "DuckDB failure" in result.output


def test_status_reads_real_parquet(tmp_path: Path):
    """Lightweight integration: write real Parquet, run status CLI without mocks.

    Validates that AnalyticalStorage + DuckDB work end-to-end with a real file.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    from debian_observer.events import EventEnvelope
    from debian_observer.events.types import ObservationMeta, PackageVersionObservedPayload

    runner = CliRunner()
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir()

    # Build a real event and write it as Parquet (same schema as storage expects)
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
        ingestion_run_id="run-status-001",
    )

    row = {
        "event_id": str(event.event_id),
        "event_type": event.event_type,
        "source": event.source,
        "entity_id": event.entity_id,
        "observed_at": event.observed_at.isoformat(),
        "effective_at": event.effective_at.isoformat(),
        "ingestion_run_id": event.ingestion_run_id,
        "package_id": event.payload["package_id"],
        "version": event.payload["version"],
        "distribution": event.payload["distribution"],
        "architecture": event.payload["architecture"],
        "component": event.payload["component"],
        "priority": event.payload["priority"],
    }
    table = pa.Table.from_pylist([row])
    pq.write_table(table, parquet_dir / "test-events.parquet")

    result = runner.invoke(cli, ["status", "--parquet-dir", str(parquet_dir)])
    assert result.exit_code == 0
    assert "Files: 1" in result.output
    assert "Events: 1" in result.output
    assert "PackageVersionObserved: 1" in result.output
