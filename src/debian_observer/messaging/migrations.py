"""Messaging: publication of the eventized pairs to the migration stream.

The pair eventizer (``normalization.eventizers.excuses``) yields the
events of an eventized snapshot — the seed first, then one pair per
following snapshot, in ``generated_date`` order. This module
publishes those chunks to the dedicated ``debian-migrations`` stream
(ADR-026) pair by pair:

* every message carries ``Nats-Msg-Id: <event_id>`` (ADR-025), so
  JetStream deduplicates server-side within the stream's duplicate
  window — a re-published window adds zero messages. An
  optimization: the guarantee stays in the consumers, which
  deduplicate by ``event_id`` (§6.2).
* the events of a pair are published only after every event of the
  previous pair (ADR-027 — the pair eventizer stays sequential);
  the caller drives the chunks, this module publishes them in the
  order they come, one pair fully before the next.

The stream is created if missing (``ensure_stream``), with the
declarative :class:`MigrationStreamConfig` retention.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from debian_observer.events.envelope import EventEnvelope
from debian_observer.messaging.config import (
    MIGRATIONS_STREAM_NAME,
    MigrationStreamConfig,
)
from debian_observer.messaging.publisher import EventPublisher, ensure_stream

__all__ = [
    "MigrationPairPublisher",
    "MigrationPublishStats",
    "publish_eventized_pairs",
]


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
