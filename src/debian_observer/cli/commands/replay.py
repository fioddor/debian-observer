"""CLI command: replay.

Re-publish events from a JSON-lines file to NATS JetStream.
"""

from __future__ import annotations

import asyncio
import json

import click

from debian_observer.events import EventEnvelope
from debian_observer.messaging.client import NATSClient
from debian_observer.messaging.config import StreamConfig
from debian_observer.messaging.publisher import EventPublisher


@click.command(name="replay")
@click.option(
    "--input",
    "input_file",
    type=click.File("r"),
    required=True,
    help="Input JSON lines file with events.",
)
@click.option("--nats-url", default="nats://localhost:4222", help="NATS server URL (default: nats://localhost:4222).")
@click.option("--dry-run", is_flag=True, help="Parse events but do not publish.")
@click.option(
    "--create-stream",
    is_flag=True,
    help="Create the JetStream stream if it does not exist.",
)
@click.option(
    "--timeout",
    default=5,
    type=int,
    help="Connection timeout in seconds (default: 5).",
)
@click.pass_context
def replay(
    ctx: click.Context,
    input_file: click.File,
    nats_url: str,
    dry_run: bool,
    create_stream: bool,
    timeout: int,
) -> None:
    """Replay events from a file to NATS."""
    events = []
    for line in input_file:
        line = line.strip()
        if line:
            data = json.loads(line)
            events.append(EventEnvelope.model_validate(data))

    click.echo(f"Loaded {len(events)} events")

    if not events:
        click.echo("No events to publish")
        return

    if dry_run:
        click.echo("Dry run — no events published")
        return

    # Run async publishing with timeout
    try:
        asyncio.run(
            asyncio.wait_for(
                _publish_events(events, nats_url, create_stream),
                timeout=timeout,
            )
        )
    except asyncio.TimeoutError:
        click.echo(f"NATS connection timed out after {timeout}s", err=True)
        raise click.ClickException(f"NATS connection timed out after {timeout}s")


async def _publish_events(
    events: list[EventEnvelope], nats_url: str, create_stream: bool
) -> None:
    """Async helper: connect to NATS and publish events."""
    client = NATSClient(servers=[nats_url])
    stream_config = StreamConfig()

    try:
        await client.connect()
        click.echo(f"Connected to NATS at {nats_url}")

        if create_stream:
            js = client.jetstream
            try:
                await js.add_stream(**stream_config.to_nats_config())
                click.echo(f"Created stream: {stream_config.stream_name}")
            except Exception as exc:
                # Stream may already exist — skip if that's the case
                if "already exists" in str(exc).lower():
                    click.echo(f"Stream {stream_config.stream_name} already exists")
                else:
                    raise

        publisher = EventPublisher(client=client, stream_config=stream_config)

        published = 0
        for event in events:
            try:
                seq = await publisher.publish(event)
                published += 1
                click.echo(f"Published {event.event_type} → seq={seq}")
            except Exception as exc:
                click.echo(
                    f"Failed to publish {event.event_type}: {exc}", err=True
                )

        click.echo(f"Published {published}/{len(events)} events")

    except Exception as exc:
        click.echo(f"NATS connection failed: {exc}", err=True)
        raise click.ClickException(str(exc)) from exc

    finally:
        await client.close()
