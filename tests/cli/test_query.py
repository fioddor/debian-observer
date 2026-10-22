"""CLI tests for the `query` command (event log over --events-dir)."""

from __future__ import annotations

from click.testing import CliRunner

from debian_observer.cli.main import cli


def test_query_help_mentions_events_dir():
    runner = CliRunner()
    result = runner.invoke(cli, ["query", "--help"])
    assert result.exit_code == 0
    # the event-log flag is content-named, not format-named (ADR-012 amendment)
    assert "--events-dir" in result.output
    assert "--parquet-dir" not in result.output
    assert "data/events" in result.output


def test_query_reads_events_dir(tmp_path):
    """--events-dir wires the event log into the query (real Parquet)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    runner = CliRunner()
    events_dir = tmp_path / "events"
    events_dir.mkdir()
    table = pa.Table.from_pylist(
        [{"package_id": "hello:amd64", "version": "2.10-3"}]
    )
    pq.write_table(table, events_dir / "run-001.parquet")

    result = runner.invoke(
        cli,
        ["query", "SELECT count(*) AS c FROM events", "--events-dir", str(events_dir)],
    )
    assert result.exit_code == 0
