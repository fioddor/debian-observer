"""CLI tests for main help."""

from __future__ import annotations

from click.testing import CliRunner

from debian_observer.cli.main import cli


def test_main_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "Debian Observer CLI" in result.output


def test_main_help_has_no_dev_install_hint():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "pip install" not in result.output
    assert "docs/user-guide.md" in result.output
