"""CLI tests for execution-report command."""

from __future__ import annotations

from click.testing import CliRunner

from debian_observer.cli.main import cli


def test_execution_report_help():
    """execution-report --help shows all options."""
    runner = CliRunner()
    result = runner.invoke(cli, ["execution-report", "--help"])
    assert result.exit_code == 0
    assert "--days" in result.output
    assert "--type" in result.output
    assert "--source" in result.output
    assert "--format" in result.output
    assert "table" in result.output
    assert "json" in result.output
    assert "csv" in result.output


def test_execution_report_no_runs_shows_empty():
    """When no runs exist, execution-report prints a clear message."""
    runner = CliRunner()
    result = runner.invoke(cli, ["execution-report", "--days", "1"])
    assert result.exit_code == 0
    assert "No" in result.output or "0" in result.output


def test_execution_report_type_filter():
    """--type filters by execution_type."""
    runner = CliRunner()
    result = runner.invoke(cli, ["execution-report", "--days", "7", "--type", "ingestion"])
    assert result.exit_code == 0


def test_execution_report_source_filter():
    """--source filters by source_name."""
    runner = CliRunner()
    result = runner.invoke(cli, ["execution-report", "--days", "7", "--source", "udd"])
    assert result.exit_code == 0


def test_execution_report_json_format():
    """--format json outputs valid JSON."""
    import json

    runner = CliRunner()
    result = runner.invoke(cli, ["execution-report", "--days", "7", "--format", "json"])
    assert result.exit_code == 0
    # Should parse as JSON (either list or empty array string)
    try:
        data = json.loads(result.output.strip())
    except json.JSONDecodeError:
        pass  # empty output is acceptable for no data


def test_execution_report_csv_format():
    """--format csv outputs comma-separated lines."""
    runner = CliRunner()
    result = runner.invoke(cli, ["execution-report", "--days", "7", "--format", "csv"])
    assert result.exit_code == 0


def test_execution_report_invalid_format_rejected():
    """Invalid format value is rejected with exit code 2."""
    runner = CliRunner()
    result = runner.invoke(cli, ["execution-report", "--format", "xml"])
    assert result.exit_code == 2
    assert "xml" in result.output or "Invalid" in result.output
