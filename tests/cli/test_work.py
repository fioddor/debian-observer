"""Tests: the parse worker CLI command.

Pins the worker's CLI surface (docs/release3.project.v0.1.md §12,
ADR-012): a `work` group with one subcommand per queue — `britney`
for the parse work queue — carrying the worker's knobs:

* `--concurrency N` — the worker's slots (pull batch and
  ``max_ack_pending``, backpressure §11.2);
* `--drain S` — integer seconds before the worker stops,
  0 = run until signalled;
* `--nats-url` — the NATS URL as a parameter (the worker runs on any
  machine that reaches the server, §14).

Help texts only: no network.
"""

from __future__ import annotations

from click.testing import CliRunner

from debian_observer.cli.main import cli


def test_work_is_a_command_group() -> None:
    result = CliRunner().invoke(cli, ["work", "--help"])
    assert result.exit_code == 0
    assert "britney" in result.output


def test_work_britney_lists_the_worker_knobs() -> None:
    result = CliRunner().invoke(cli, ["work", "britney", "--help"])
    assert result.exit_code == 0
    for option in ("--concurrency", "--drain", "--nats-url"):
        assert option in result.output, f"--help must list {option}"