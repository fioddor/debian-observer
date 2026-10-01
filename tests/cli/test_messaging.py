"""CLI tests for messaging commands."""
from __future__ import annotations

from click.testing import CliRunner
from debian_observer.cli.main import cli


def test_replay_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["replay", "--help"])
    assert result.exit_code == 0
