"""CLI tests for query command."""

from __future__ import annotations

from click.testing import CliRunner

from debian_observer.cli.main import cli


def test_query_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["query", "--help"])
    assert result.exit_code == 0
