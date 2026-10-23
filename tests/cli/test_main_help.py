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


def test_consume_is_not_a_command():
    """The generic consume CLI is dead (externalized to the
    examples/ recipe): `consume` is no longer a subcommand — usage
    error, exit 2, never a traceback."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["consume", "--stream", "x", "--subject", "y", "--durable", "z"],
    )

    assert result.exit_code == 2
    assert "No such command" in result.output
    assert "Traceback" not in result.output


def test_publish_is_a_command():
    """`publish` is registered (the JetStream view of the log)."""
    runner = CliRunner()
    result = runner.invoke(cli, ["publish", "--help"])

    assert result.exit_code == 0
    assert "--events-dir" in result.output
