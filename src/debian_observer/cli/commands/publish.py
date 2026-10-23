"""CLI command: publish — project the event log onto NATS JetStream.

The stream is a **view** of the append-only Parquet event log
(ADR-004 amendment, 2026-10-23), not a source of truth: it is fully
rebuildable from ``--events-dir`` at any time, and ``consolidate
--events-dir`` regenerates the same consolidated table the
streaming consumers derive from it. Every message carries
``Nats-Msg-Id: <event_id>``, so JetStream deduplicates server-side
within the stream's duplicate window — re-publishing the same log
is a no-op (idempotent publish).

The tool deliberately ships **no ``consume`` subcommand**: a queue
consumer is a pipeline the researcher owns — copy
``examples/consume_packages.py``, adjust it, run it as a process
(the library surface is ``messaging`` + ``consumer_app``).
"""

from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import click

from debian_observer.events.envelope import EventEnvelope
from debian_observer.messaging.client import NATSClient
from debian_observer.messaging.config import DEFAULT_STREAM_NAME, StreamConfig
from debian_observer.messaging.publisher import EventPublisher, ensure_stream
from debian_observer.storage.consolidation import (
    ConsolidateError,
    read_event_log,
)

#: Payload columns re-serialized into the published envelopes — the
#: same flattened keys the event log stores (``storage.parquet``,
#: ADR-011): the package id travels in the payload, never in the
#: subject.
_PAYLOAD_KEYS: tuple[str, ...] = (
    "package_id",
    "source_package",
    "version",
    "distribution",
    "release",
    "suite",
    "architecture",
    "component",
    "section",
    "priority",
)


@dataclass(frozen=True)
class PublishStats:
    """Counts surfaced by the CLI echo."""

    files_read: int
    events_read: int
    published: dict[str, int] = field(default_factory=dict)
    duplicates_absorbed: int = 0
    stream_name: str = DEFAULT_STREAM_NAME
    servers: tuple[str, ...] = ()


def row_to_envelope(row: Mapping[str, Any]) -> EventEnvelope:
    """De-serialize a flattened event-log row back into an
    :class:`EventEnvelope` (the ``api.loader`` round-trip pattern:
    payload keys re-nest under ``payload``, envelope columns stay
    flat). Missing legacy columns load as absent/None — the envelope
    defaults carry them.
    """
    effective_at = row.get("effective_at") or None
    return EventEnvelope(
        event_id=row.get("event_id"),
        event_type=row.get("event_type"),
        source=row.get("source") or "",
        entity_id=row.get("entity_id") or "",
        observed_at=row.get("observed_at"),
        effective_at=effective_at,
        ingestion_run_id=row.get("ingestion_run_id") or None,
        payload={key: row.get(key) for key in _PAYLOAD_KEYS if key in row},
    )


async def publish_event_log(
    events_dir: str | Path,
    *,
    client: Any,
    stream_config: StreamConfig,
) -> PublishStats:
    """Publish every event of the log to JetStream (the publish
    pipeline: read → re-serialize → ensure stream → publish with
    ``Nats-Msg-Id`` dedup). Returns the publish statistics.
    """
    rows, files_read = read_event_log(events_dir)
    await ensure_stream(client, stream_config)

    publisher = EventPublisher(client=client, stream_config=stream_config)
    published: Counter[str] = Counter()
    duplicates = 0
    for row in rows:
        envelope = row_to_envelope(row)
        subject = stream_config.subject_for(envelope.event_type)
        if subject is None:
            raise ValueError(
                f"No subject mapping for event type: {envelope.event_type}"
            )
        await publisher.publish(
            envelope, headers={"Nats-Msg-Id": str(envelope.event_id)}
        )
        ack = getattr(publisher, "last_ack", None)
        if bool(getattr(ack, "duplicate", False)):
            duplicates += 1
        published[subject] += 1

    servers = tuple(
        client.servers
        if hasattr(client, "servers") and client.servers is not None
        else ()
    )
    return PublishStats(
        files_read=files_read,
        events_read=len(rows),
        published=dict(published),
        duplicates_absorbed=duplicates,
        stream_name=stream_config.stream_name,
        servers=servers,
    )


@click.command(name="publish")
@click.option(
    "--events-dir",
    "events_dir",
    required=True,
    type=click.Path(path_type=Path, exists=True, file_okay=False),
    help="Directory of the Parquet event log (one file per ingestion run).",
)
@click.option(
    "--stream",
    "stream",
    default=DEFAULT_STREAM_NAME,
    show_default=True,
    help="JetStream stream name to publish to (created if missing).",
)
@click.option(
    "--server",
    "servers",
    multiple=True,
    default=("nats://localhost:4222",),
    show_default=True,
    help="NATS server URL (may be given multiple times).",
)
def publish(events_dir: Path, stream: str, servers: tuple[str, ...]) -> None:
    """Publish the event log to NATS JetStream (a rebuildable view).

    Reads every *.parquet event-log file in --events-dir and
    publishes each event to its flat per-type subject —
    debian.package_version.observed, debian.package.observed,
    debian.release.observed, debian.migration.observed (ADR-004)
    — with Nats-Msg-Id: <event_id>, so JetStream absorbs
    re-published duplicates server-side: publishing the same log
    twice is a no-op. The stream is NOT a source of truth — it is a
    view of the Parquet event log, rebuildable at any time from
    --events-dir. Consuming is external: copy
    examples/consume_packages.py and run it as a process (the CLI
    has no consume command).
    """
    client = NATSClient(servers=list(servers))
    stream_config = StreamConfig(stream_name=stream)

    async def _run() -> PublishStats:
        await client.connect()
        try:
            return await publish_event_log(
                events_dir, client=client, stream_config=stream_config
            )
        finally:
            await client.close()

    try:
        stats = asyncio.run(_run())
    except ConsolidateError as exc:
        raise click.ClickException(str(exc)) from exc
    except (ValueError, RuntimeError, ConnectionError) as exc:
        raise click.ClickException(f"Publish failed: {exc}") from exc

    click.echo(f"Files read: {stats.files_read}")
    click.echo(f"Events read: {stats.events_read}")
    for subject in sorted(stats.published):
        click.echo(f"Published {subject}: {stats.published[subject]}")
    click.echo(f"Duplicates absorbed: {stats.duplicates_absorbed}")
    click.echo(f"Stream: {stats.stream_name}")
    click.echo(f"Servers: {', '.join(stats.servers)}")