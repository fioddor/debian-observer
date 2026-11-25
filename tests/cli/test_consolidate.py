"""CLI tests for the removed `consolidate` subcommand.

Consolidation is not a CLI command: the consolidated
`package_versions` table is built by the example prosumer
`debian-observer-consolidate` (batch: `--file-in <event log>`
`--file-out data/normalized/package_versions/package_versions.parquet`)
or, off the queue, by the `examples/consume_packages.py` recipe —
both on `storage.consolidation`.
"""

from __future__ import annotations

from click.testing import CliRunner

from debian_observer.cli.main import cli


def test_consolidate_is_not_a_command():
    """`consolidate` is no longer a subcommand.

    Folding the event log into the table is the prosumer's job
    (a pipeable example), so the CLI must reject the invocation as
    an unknown command: usage error, exit 2, never a traceback.
    """
    runner = CliRunner()
    result = runner.invoke(
        cli, ["consolidate", "--events-dir", "x", "--dataset-dir", "y"]
    )

    assert result.exit_code == 2
    assert "No such command" in result.output
    assert "Traceback" not in result.output


def test_consolidate_is_not_registered():
    """The command registry keeps no `consolidate` entry.

    The command list is the CLI's contract with the operator: a
    stale registration would advertise a command whose fold lives
    in the prosumer now.
    """
    assert "consolidate" not in cli.commands


def test_main_help_lists_no_consolidate():
    """`--help` advertises no `consolidate` command.

    The help output names the subcommands one per line; the
    prosumer (mentioned in the epilogue text) is not one of them.
    """
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])

    assert result.exit_code == 0
    commands = [
        line.strip().split(" ")[0]
        for line in result.output.splitlines()
        if line.startswith("  ") and line.strip()
    ]
    assert "consolidate" not in commands