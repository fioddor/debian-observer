"""Messaging: the migration stream — publication and replay.

The pair eventizer (``normalization.eventizers.excuses``) yields the
events of an eventized snapshot — the seed first, then one pair per
following snapshot, in ``generated_date`` order. This module
publishes those chunks to the dedicated ``debian-migrations`` stream
(ADR-026) pair by pair, and attaches the durable consumers that
re-read it:

* every message carries ``Nats-Msg-Id: <event_id>`` (ADR-025), so
  JetStream deduplicates server-side within the stream's duplicate
  window — a re-published window adds zero messages. An
  optimization: the guarantee stays in the consumers, which
  deduplicate by ``event_id`` (§6.2).
* the events of a pair are published only after every event of the
  previous pair (ADR-027 — the pair eventizer stays sequential);
  the caller drives the chunks, this module publishes them in the
  order they come, one pair fully before the next.
* a durable consumer (:func:`replay_migration_stream`) attaches
  with deliver policy **all**: it re-reads the stream from the
  start, whenever it arrives (§11.1 — the stream is the durable
  record), with the NATS URL as a parameter. Redeliveries may
  arrive out of order: the handler orders by the event, never by
  arrival (§6.3).

The stream is created if missing (``ensure_stream``), with the
declarative :class:`MigrationStreamConfig` retention.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from debian_observer.events.envelope import EventEnvelope
from debian_observer.messaging.client import NATSClient
from debian_observer.messaging.config import (
    MIGRATIONS_STREAM_NAME,
    MigrationStreamConfig,
)
from debian_observer.messaging.consumer import EventConsumer, EventHandler
from debian_observer.messaging.publisher import EventPublisher, ensure_stream

__all__ = [
    "MIGRATIONS_SUBJECT_FILTER",
    "MigrationPairPublisher",
    "MigrationPublishStats",
    "publish_eventized_pairs",
    "replay_migration_stream",
]

#: The subject filter a migration-stream consumer subscribes to:
#: both subject families of §6.1 (`debian.migration.excuse.>` and
#: `debian.migration.snapshot.>`), one filter token.
MIGRATIONS_SUBJECT_FILTER = "debian.migration.>"


@dataclass(frozen=True)
class MigrationPublishStats:
    """Counts surfaced by a publication run."""

    #: Eventized snapshots (seed + pairs) whose events were published.
    pairs: int = 0
    #: Messages the stream kept (not absorbed as duplicates).
    events: int = 0
    #: Messages the stream absorbed as duplicates (same Nats-Msg-Id).
    duplicates_absorbed: int = 0
    stream_name: str = MIGRATIONS_STREAM_NAME
    servers: tuple[str, ...] = ()


class MigrationPairPublisher:
    """Publishes eventized pairs to ``debian-migrations``, in order.

    One instance per run: :meth:`ensure_stream` once, then
    :meth:`publish_pair` per eventized snapshot, in the order the
    pair eventizer yields them (ADR-027) — the seed first, then one
    pair per following snapshot. Publishing pair by pair is what
    keeps the pair order: nothing buffers between pairs.
    """

    def __init__(
        self,
        *,
        client: Any,
        stream_config: MigrationStreamConfig | None = None,
    ) -> None:
        self._client = client
        self._config = stream_config or MigrationStreamConfig()
        self._publisher = EventPublisher(
            client=client, stream_config=self._config
        )
        self.pairs = 0
        self.events = 0
        self.duplicates_absorbed = 0

    async def ensure_stream(self) -> Any:
        """Create the migration stream if missing; return its info."""
        return await ensure_stream(self._client, self._config)

    async def publish_pair(
        self,
        snapshot_id: str,
        events: Sequence[EventEnvelope],
    ) -> None:
        """Publish one eventized snapshot's events, all before the next pair.

        Every message carries ``Nats-Msg-Id: <event_id>``; a
        duplicate the server reports (a re-published window inside
        the duplicate window) is counted, not re-delivered.
        """
        for event in events:
            await self._publisher.publish(
                event, headers={"Nats-Msg-Id": str(event.event_id)}
            )
            ack = getattr(self._publisher, "last_ack", None)
            if bool(getattr(ack, "duplicate", False)):
                self.duplicates_absorbed += 1
            else:
                self.events += 1
        self.pairs += 1

    def stats(self) -> MigrationPublishStats:
        """The run's publish counts (the CLI echo's source)."""
        servers = tuple(
            self._client.servers
            if hasattr(self._client, "servers")
            and self._client.servers is not None
            else ()
        )
        return MigrationPublishStats(
            pairs=self.pairs,
            events=self.events,
            duplicates_absorbed=self.duplicates_absorbed,
            stream_name=self._config.stream_name,
            servers=servers,
        )


async def publish_eventized_pairs(
    chunks: Iterable[tuple[str, list[EventEnvelope]]],
    *,
    client: Any,
    stream_config: MigrationStreamConfig | None = None,
) -> MigrationPublishStats:
    """Publish every eventized pair the ``chunks`` iterator yields.

    ``chunks`` is what
    :meth:`ExcusesEventizer.iter_eventized \
    <debian_observer.normalization.eventizers.excuses.ExcusesEventizer>`
    yields: ``(snapshot_id, events)`` per eventized snapshot, in
    order. The pairs publish sequentially (ADR-027) — one pair fully
    before the next — with ``Nats-Msg-Id = event_id`` (ADR-025).

    Returns the publish statistics (kept events, absorbed duplicates).
    """
    publisher = MigrationPairPublisher(
        client=client, stream_config=stream_config
    )
    await publisher.ensure_stream()
    for snapshot_id, events in chunks:
        await publisher.publish_pair(snapshot_id, events)
    return publisher.stats()


def _deliver_all() -> Any:
    """``DeliverPolicy.ALL``, imported lazily (like ``NATSClient``).

    The battery runs against fakes; the real enum only matters when
    a real NATS answers, and its absence must name the install, not
    crash the import.
    """
    try:
        from nats.js.api import DeliverPolicy
    except ImportError as exc:  # pragma: no cover - depends on nats-py
        raise RuntimeError(
            "nats-py is required. Install with: pip install nats-py"
        ) from exc
    return DeliverPolicy.ALL


async def replay_migration_stream(
    *,
    durable_name: str,
    handler: EventHandler,
    client: Any = None,
    servers: list[str] | None = None,
) -> EventConsumer:
    """Attach a durable consumer that replays ``debian-migrations``.

    A consumer that arrives later than the publisher re-reads the
    stream **from the start**: deliver policy all — the stream is the
    durable record (ADR-026), limits retention keeps history, and the
    subject filter :data:`MIGRATIONS_SUBJECT_FILTER` covers both
    subject families (§6.1). The NATS URL is a parameter
    (``servers``); an existing connected ``client`` is reused as-is.

    The handler owns the semantics: it deduplicates by ``event_id``
    (the standard pattern is
    :class:`~debian_observer.messaging.consumer.IngestionHandler`
    over a persistent store) — JetStream's server-side dedup is an
    optimization, bounded by the duplicate window (§6.2) — and it
    orders by the event's own timestamps, never by arrival (§6.3).

    Usage::

        consumer = await replay_migration_stream(
            durable_name="analytics",
            handler=my_handler,
            servers=["nats://nats:4222"],
        )
        ...
        await consumer.close()

    Args:
        durable_name: The durable consumer name (survives restarts).
        handler: Async ``handler(msg, event, payload, ack)``.
        client: A connected NATS client; built from ``servers``
            when omitted.
        servers: NATS server URLs for the built client
            (default ``nats://localhost:4222``).

    Returns:
        The subscribed :class:`EventConsumer` (``close`` to stop).
    """
    if client is None:
        client = NATSClient(servers=servers)
        await client.connect()
    consumer = EventConsumer(
        client=client,
        handler=handler,
        stream_name=MIGRATIONS_STREAM_NAME,
        durable_name=durable_name,
    )
    await consumer.subscribe(
        MIGRATIONS_SUBJECT_FILTER, deliver_policy=_deliver_all()
    )
    return consumer
