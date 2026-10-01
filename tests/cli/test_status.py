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
