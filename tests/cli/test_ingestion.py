"""CLI tests for ingestion command."""
from __future__ import annotations

from click.testing import CliRunner
from debian_observer.cli.main import cli


def test_ingest_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["ingest", "--help"])
    assert result.exit_code == 0
    assert "--source" in result.output
    assert "mock" in result.output


def test_ingest_invalid_source():
    """Unknown --source values are usage errors: exit 2, one line, no traceback."""
    runner = CliRunner()
    result = runner.invoke(cli, ["ingest", "--source", "unknown"])
    assert result.exit_code == 2
    assert "unknown" in result.output
    assert "mock" in result.output
    assert "Traceback" not in result.output
    assert "TypeError" not in result.output


def test_ingest_help_lists_source_choices():
    """`ingest --help` shows the accepted --source values in the synopsis."""
    runner = CliRunner()
    result = runner.invoke(cli, ["ingest", "--help"])
    assert result.exit_code == 0
    assert "[mock]" in result.output
