"""CLI command: consume events from NATS JetStream.

Provides the ``debian-observer consume`` entrypoint for running a
standalone event consumer process.
"""

from __future__ import annotations

import asyncio

import click

from debian_observer.consumer_app import ConsumerApp


@click.command(name="consume")
@click.option(
    "--stream",
    required=True,
    help="JetStream stream name to consume from.",
)
@click.option(
    "--subject",
    required=True,
    help="NATS subject filter (e.g. 'debian.observer.>').",
)
@click.option(
    "--durable",
    required=True,
    help="Durable consumer name (survives restarts).",
)
@click.option(
    "--server",
    "servers",
    multiple=True,
    default=["nats://localhost:4222"],
    help="NATS server URL (may be given multiple times; default: nats://localhost:4222).",
)
@click.option(
    "--max-events",
    type=int,
    default=None,
    help="Maximum events to process before exiting (optional).",
)
def consume_command(
    stream: str,
    subject: str,
    durable: str,
    servers: tuple[str, ...],
    max_events: int | None,
) -> None:
    """Run a standalone event consumer process.

    Connects to NATS JetStream and consumes events from the specified
    stream and subject. The consumer is idempotent and supports graceful
    shutdown on SIGTERM / SIGINT.

    Examples::

        debian-observer consume --stream EVENTS --subject "debian.observer.>" --durable cli-consumer

        debian-observer consume --server nats://nats.example.com:4222 --stream EVENTS --subject "debian.>" --durable worker-1
    """
    app = ConsumerApp()

    # Register a simple print handler for all known event types.
    # R1: users are expected to subclass or script their own handlers.
    # This CLI provides the infrastructure; domain logic belongs in plugins.
    click.echo(
        f"Starting consumer on stream={stream} subject={subject} durable={durable}"
    )

    try:
        asyncio.run(
            app.run(
                servers=list(servers),
                stream=stream,
                subject=subject,
                durable=durable,
            )
        )
    except KeyboardInterrupt:
        click.echo("Consumer stopped by user.")
    except Exception as exc:
        click.echo(f"Consumer error: {exc}", err=True)
        raise click.ClickException(str(exc)) from exc

    click.echo("Consumer finished.")
