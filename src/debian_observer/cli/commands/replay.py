"""CLI command: replay.

Re-publish events from a JSON-lines file to NATS JetStream.
"""

from __future__ import annotations

import json

import click

from debian_observer.events import EventEnvelope


@click.command(name="replay")
@click.option("--input", "input_file", type=click.File("r"), required=True, help="Input JSON lines file with events.")
@click.option("--nats-url", default="nats://localhost:4222", help="NATS server URL (default: nats://localhost:4222).")
@click.option("--dry-run", is_flag=True, help="Parse events but do not publish.")
@click.pass_context
def replay(
    ctx: click.Context,
    input_file: click.File,
    nats_url: str,
    dry_run: bool,
) -> None:
    """Replay events from a file to NATS."""
    events = []
    for line in input_file:
        line = line.strip()
        if line:
            data = json.loads(line)
            events.append(EventEnvelope.model_validate(data))

    click.echo(f"Loaded {len(events)} events")

    if dry_run:
        click.echo("Dry run — no events published")
        return

    # Publishing requires an async context; for R1 we show the intent
    click.echo(f"Would publish {len(events)} events to {nats_url}")
    click.echo("Note: Full replay requires NATS connection (future enhancement)")
