"""Tests for the --help surfaces of the normalize command."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from debian_observer.cli.main import cli
from debian_observer.normalization.eventizers import (
    EVENTIZERS,
    list_eventizers,
)


def test_normalize_help_lists_the_sources():
    """--help names every source the registry knows."""
    # A surface with eventizers but no subcommand is invisible to
    # the researcher.
    runner = CliRunner()
    result = runner.invoke(cli, ["normalize", "--help"])
    assert result.exit_code == 0
    for source in EVENTIZERS:
        assert source in result.output


@pytest.mark.parametrize("source", sorted(EVENTIZERS))
def test_help_names_the_source_eventizers(source):
    """The source's help lists its registry's eventizers.

    The --eventizer choice is a choice between named things: each
    source's help lists the eventizers its registry offers, read from
    the registry itself — the contract, not the help's implementation
    — so a help that stops listing what its source offers fails, and a
    registry that grows does not fail yesterday's help.
    """
    offered = list_eventizers(source)
    assert offered, f"the registry offers no eventizer for {source}"
    runner = CliRunner()
    result = runner.invoke(cli, ["normalize", source, "--help"])
    assert result.exit_code == 0
    for name in offered:
        assert name in result.output
    # the default is named among them
    assert "package-versions" in result.output