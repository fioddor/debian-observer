"""CLI tests for pipeline commands."""
from __future__ import annotations

from click.testing import CliRunner
from debian_observer.cli.main import cli


def test_status_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--help"])
    assert result.exit_code == 0


def test_benchmark_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["benchmark", "--help"])
    assert result.exit_code == 0


def test_benchmark_runs():
    runner = CliRunner()
    result = runner.invoke(cli, ["benchmark", "--records", "10"])
    assert result.exit_code == 0
    assert "Records: 10" in result.output
    assert "Throughput:" in result.output
