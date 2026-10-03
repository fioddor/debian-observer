"""CLI tests for query command."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow.parquet as pq
from click.testing import CliRunner

from debian_observer.cli.main import cli
from debian_observer.events import EventEnvelope
from debian_observer.events.types import ObservationMeta, PackageVersionObservedPayload


def test_query_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["query", "--help"])
    assert result.exit_code == 0


def test_query_runs_over_parquet_data():
    """Query should return results from Parquet files."""
    runner = CliRunner()

    # Build a minimal Parquet file with one event
    with tempfile.TemporaryDirectory() as tmpdir:
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
            ingestion_run_id="run-test",
        )

        # Write via pyarrow directly (simplest for test)
        import pyarrow as pa

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
        pq.write_table(table, Path(tmpdir) / "test-events.parquet")

        result = runner.invoke(
            cli,
            [
                "query",
                "SELECT entity_id, version FROM events WHERE distribution = 'sid'",
                "--parquet-dir",
                tmpdir,
                "--format",
                "table",
            ],
        )

        assert result.exit_code == 0
        assert "hello:amd64" in result.output
        assert "2.10-3" in result.output
