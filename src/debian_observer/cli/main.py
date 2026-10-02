"""CLI: Main entry point.

Defines the top-level Click group for operational commands.
"""

from __future__ import annotations

import click

from debian_observer.cli.commands import benchmark
from debian_observer.cli.commands import consume
from debian_observer.cli.commands import ingest
from debian_observer.cli.commands import normalize
from debian_observer.cli.commands import query
from debian_observer.cli.commands import replay
from debian_observer.cli.commands import status
from debian_observer.cli.commands import validate


@click.group()
@click.version_option(version="0.1.0", prog_name="debian-observer")
def cli() -> None:
    """Debian Observer CLI

       This is the `debian-observer` executable: the way to interact
       with the Debian Observatory from the command line (see
       docs/user-guide.md § Installation).

       Commands are listed below; run
       `debian-observer <command> --help` for their options. The
       implemented data sources are described by
       `debian-observer ingest --help`.
    """
    pass


# Register commands
cli.add_command(benchmark.benchmark)
cli.add_command(consume.consume_command)
cli.add_command(ingest.ingest)
cli.add_command(normalize.normalize)
cli.add_command(query.query)
cli.add_command(replay.replay)
cli.add_command(status.status)
cli.add_command(validate.validate)


if __name__ == "__main__":
    cli()
