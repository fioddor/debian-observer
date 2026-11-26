"""CLI command group: work.

Work a distributed queue: the commands run a worker that pulls jobs
from a NATS JetStream work queue and does them. One subcommand per
queue (ADR-012's one-surface-per-source shape, applied to queues):
`britney` works the parse queue — the distributed snapshot parse
(ADR-027).

The worker's knobs (docs/release3.project.v0.1.md §12):

* `--concurrency N` — the worker's slots;
* `--drain S` — integer seconds before the worker stops,
  0 = run until signalled;
* `--nats-url` — the NATS URL as a parameter: the worker runs on any
  machine that reaches the server (§14), with the platform wheel
  installed unchanged.
"""

from __future__ import annotations

import asyncio

import click

from debian_observer.messaging.parse_queue import (
    PARSE_QUEUE_DURABLE,
    RAW_BUCKET,
    RESULTS_BUCKET,
)
from debian_observer.messaging.parse_worker import ParseWorker


@click.group(name="work")
def work() -> None:
    """Work a distributed queue (one subcommand per queue).

    Run `debian-observer work <queue> --help` for a worker's options.
    """


@work.command(name="britney")
@click.option(
    "--concurrency",
    type=int,
    default=1,
    show_default=True,
    help=(
        "Worker slots: the pull batch and the consumer's "
        "max_ack_pending (backpressure). No more than this many "
        "snapshots parse at once on this worker."
    ),
)
@click.option(
    "--drain",
    type=int,
    default=0,
    show_default=True,
    help=(
        "Integer seconds before the worker stops "
        "(0 = run until signalled: Ctrl-C / SIGTERM)."
    ),
)
@click.option(
    "--nats-url",
    "servers",
    multiple=True,
    default=("nats://localhost:4222",),
    show_default=True,
    help="NATS server URL of the parse queue (may be given multiple times).",
)
@click.option(
    "--raw-bucket",
    default=RAW_BUCKET,
    show_default=True,
    help="Object Store bucket the raw .yaml.gz objects live in.",
)
@click.option(
    "--results-bucket",
    default=RESULTS_BUCKET,
    show_default=True,
    help="Object Store bucket the compact states go back into.",
)
@click.option(
    "--durable",
    "durable_name",
    default=PARSE_QUEUE_DURABLE,
    show_default=True,
    help=(
        "The shared durable consumer name: every worker of the "
        "queue joins it; a machine that disappears has its jobs "
        "redelivered to the others."
    ),
)
def work_britney(
    concurrency: int,
    drain: int,
    servers: tuple[str, ...],
    raw_bucket: str,
    results_bucket: str,
    durable_name: str,
) -> None:
    """Work the britney parse queue (the distributed snapshot parse).

    One job = one snapshot: the raw object is read from the NATS
    Object Store (never from the source), verified by SHA-256,
    parsed, and the compact state put back into the results bucket —
    then acked. In-progress acks keep slow parses alive; a poisoned
    snapshot is dead-lettered at max_deliver and the queue continues.

    --drain 0 (the default) runs until signalled: the run ends on
    Ctrl-C / SIGTERM with the jobs in flight redelivered to the
    other workers.
    """
    from debian_observer.messaging.client import NATSClient

    if concurrency < 1:
        raise click.ClickException("--concurrency must be at least 1")
    if drain < 0:
        raise click.ClickException("--drain must be 0 or positive seconds")

    client = NATSClient(servers=list(servers))
    worker = ParseWorker(
        client=client,
        concurrency=concurrency,
        drain_seconds=drain,
        raw_bucket=raw_bucket,
        results_bucket=results_bucket,
        durable_name=durable_name,
    )

    async def _run() -> None:
        await client.connect()
        try:
            await worker.run()
        finally:
            await client.close()

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        click.echo("Drained by signal", err=True)
    click.echo(
        f"Worked: {len(worker.done)} snapshots "
        f"({', '.join(worker.done) if len(worker.done) <= 8 else str(len(worker.done)) + ' ids'}) "
        f"from {', '.join(servers)}",
        err=True,
    )