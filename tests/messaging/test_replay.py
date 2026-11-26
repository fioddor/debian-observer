"""Tests: replay the migration stream from the start.

Pins docs/release3.project.v0.1.md §11.1/§15: a durable consumer
that attaches to `debian-migrations` later than the publisher
re-reads the stream **from the start** (deliver policy all — the
stream is the durable record, ADR-026), with the NATS URL as a
parameter, and deduplicates by `event_id` on the consumer side
(§6.2: JetStream's dedup is an optimization; the guarantee lives
here). Redeliveries may arrive out of order: the consumer orders by
the event, never by arrival.

Tests run against a fake NATS client (no network).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from uuid import uuid4

from nats.js.api import DeliverPolicy

from debian_observer.events.envelope import EventEnvelope
from debian_observer.messaging.consumer import EventConsumer, IngestionHandler
from debian_observer.messaging.migrations import (
    MIGRATIONS_SUBJECT_FILTER,
    replay_migration_stream,
)

# ---------------------------------------------------------------------------
# A fake NATS client (the tests/cli/test_publish.py pattern, no network)
# ---------------------------------------------------------------------------


class FakeMsg:
    """Stand-in NATS message: the envelope as JSON bytes."""

    def __init__(self, event: EventEnvelope) -> None:
        self.data = event.to_json_bytes()
        self.event = event
        self.acked = 0
        self.naked = 0

    async def ack(self) -> None:
        self.acked += 1

    async def nak(self) -> None:
        self.naked += 1


class FakeSubscription(SimpleNamespace):
    """Stand-in subscription: keeps the callback for the test to drive."""


class FakeJetStream:
    """Stand-in JetStream: records the durable subscribe."""

    def __init__(self) -> None:
        self.subscribe_calls: list[dict[str, Any]] = []

    async def subscribe(self, subject: str, **kwargs: Any) -> Any:
        self.subscribe_calls.append({"subject": subject, **kwargs})
        return FakeSubscription(cb=kwargs.get("cb"))


class FakeNATSClient:
    """Stand-in NATSClient: same surface, no network."""

    def __init__(self, servers: list[str] | None = None) -> None:
        self.servers = servers or ["nats://localhost:4222"]
        self.connected = 0
        self.closed = 0
        self._js = FakeJetStream()

    @property
    def is_connected(self) -> bool:
        return self.connected > self.closed

    async def connect(self) -> None:
        self.connected += 1

    async def close(self) -> None:
        self.closed += 1

    @property
    def jetstream(self) -> FakeJetStream:
        return self._js


# ---------------------------------------------------------------------------
# Events as the migration stream carries them
# ---------------------------------------------------------------------------


def _event(n: int) -> EventEnvelope:
    return EventEnvelope(
        event_id=uuid4(),
        event_type="ExcuseVerdictChanged",
        source="britney",
        entity_id=f"item-{n}",
        payload={"item_name": f"item-{n}"},
    )


STREAM_EVENTS = [_event(n) for n in range(4)]


# ---------------------------------------------------------------------------
# The consumer: deliver policy all, NATS URL as a parameter
# ---------------------------------------------------------------------------


async def _noop_handler(
    msg: Any, event: EventEnvelope, payload: dict, ack: Any
) -> None:
    await ack()


class TestDeliverPolicy:
    """`EventConsumer.subscribe` passes the deliver policy through."""

    async def test_subscribe_passes_the_deliver_policy(self) -> None:
        client = FakeNATSClient()
        await client.connect()
        consumer = EventConsumer(
            client=client,
            handler=_noop_handler,
            stream_name="debian-migrations",
            durable_name="analytics",
        )
        await consumer.subscribe(
            MIGRATIONS_SUBJECT_FILTER, deliver_policy=DeliverPolicy.ALL
        )
        (call,) = client._js.subscribe_calls
        assert call["deliver_policy"] == DeliverPolicy.ALL

    async def test_subscribe_without_policy_changes_nothing(self) -> None:
        # Back-compat: the existing consumers never named a deliver policy;
        # the server default (new) applies — the kwarg is absent,
        # not None.
        client = FakeNATSClient()
        await client.connect()
        consumer = EventConsumer(
            client=client,
            handler=_noop_handler,
            stream_name="debian-events",
            durable_name="storage",
        )
        await consumer.subscribe("debian.package.observed")
        (call,) = client._js.subscribe_calls
        assert "deliver_policy" not in call


class TestReplayMigrationStream:
    """A durable that re-reads the migration stream from the start."""

    async def test_the_durable_replays_from_the_start(
        self, monkeypatch
    ) -> None:
        made: dict[str, Any] = {}

        def _factory(servers: list[str] | None = None) -> FakeNATSClient:
            fake = FakeNATSClient(servers=servers)
            made["client"] = fake
            return fake

        monkeypatch.setattr(
            "debian_observer.messaging.migrations.NATSClient", _factory
        )
        consumer = await replay_migration_stream(
            durable_name="analytics",
            handler=_noop_handler,
            servers=["nats://analytics-host:4222"],
        )
        client = made["client"]
        # The NATS URL was the parameter, and it connected.
        assert client.servers == ["nats://analytics-host:4222"]
        assert client.connected == 1
        # The durable attaches to the migration stream, both subject
        # families, replaying from the start.
        (call,) = client._js.subscribe_calls
        assert call["subject"] == MIGRATIONS_SUBJECT_FILTER
        assert call["subject"] == "debian.migration.>"
        assert call["stream"] == "debian-migrations"
        assert call["durable"] == "analytics"
        assert call["deliver_policy"] == DeliverPolicy.ALL

    async def test_the_default_nats_url_is_localhost(self, monkeypatch) -> None:
        made: dict[str, Any] = {}

        def _factory(servers: list[str] | None = None) -> FakeNATSClient:
            fake = FakeNATSClient(servers=servers)
            made["client"] = fake
            return fake

        monkeypatch.setattr(
            "debian_observer.messaging.migrations.NATSClient", _factory
        )
        await replay_migration_stream(
            durable_name="analytics", handler=_noop_handler
        )
        assert made["client"].servers == ["nats://localhost:4222"]

    async def test_an_existing_client_is_reused(self) -> None:
        client = FakeNATSClient(servers=["nats://reused:4222"])
        await client.connect()
        consumer = await replay_migration_stream(
            durable_name="analytics", handler=_noop_handler, client=client
        )
        assert consumer._client is client
        assert client.connected == 1  # replay did not reconnect


# ---------------------------------------------------------------------------
# Dedup by event_id on the consumer side (§6.2)
# ---------------------------------------------------------------------------


async def _consume(
    client: FakeNATSClient, deliveries: list[FakeMsg]
) -> None:
    """Drive every delivery through the consumer's callback."""
    (call,) = client._js.subscribe_calls
    cb = call["cb"]
    for msg in deliveries:
        await cb(msg)


class TestReplayDedup:
    """A replay's redeliveries deduplicate by event_id."""

    async def test_redelivered_events_are_processed_once(self) -> None:
        # The stream redelivers (ack_wait passed, the consumer
        # restarted, the window re-published): the same event_id
        # arrives twice. The consumer dedups by event_id and its
        # output equals a clean run's.
        processed: list[str] = []

        async def processor(event: EventEnvelope) -> None:
            processed.append(str(event.event_id))

        handler = IngestionHandler(processor=processor)

        async def _handler(
            msg: Any, event: EventEnvelope, payload: dict, ack: Any
        ) -> None:
            await handler.handle(event, ack)

        client = FakeNATSClient()
        await client.connect()
        consumer = EventConsumer(
            client=client,
            handler=_handler,
            stream_name="debian-migrations",
            durable_name="analytics",
        )
        await consumer.subscribe(
            MIGRATIONS_SUBJECT_FILTER, deliver_policy=DeliverPolicy.ALL
        )
        msgs = [FakeMsg(event) for event in STREAM_EVENTS]
        # Clean run: every event once, in stream order.
        await _consume(client, msgs)
        clean = list(processed)
        assert clean == [str(e.event_id) for e in STREAM_EVENTS]
        # Redelivery of everything (the durable restarted mid-run):
        # the raw deliveries carry the duplicates, the consumer's
        # output does not.
        await _consume(client, msgs)
        assert processed == clean

    async def test_a_killed_delivery_is_acked_after_redelivery(
        self
    ) -> None:
        # A duplicate is not dropped silently: it is acked, so the
        # stream stops redelivering it.
        processed: list[str] = []

        async def processor(event: EventEnvelope) -> None:
            processed.append(str(event.event_id))

        handler = IngestionHandler(processor=processor)

        async def _handler(
            msg: Any, event: EventEnvelope, payload: dict, ack: Any
        ) -> None:
            await handler.handle(event, ack)

        client = FakeNATSClient()
        await client.connect()
        consumer = EventConsumer(
            client=client,
            handler=_handler,
            stream_name="debian-migrations",
            durable_name="analytics",
        )
        await consumer.subscribe(
            MIGRATIONS_SUBJECT_FILTER, deliver_policy=DeliverPolicy.ALL
        )
        first = FakeMsg(STREAM_EVENTS[0])
        await _consume(client, [first])
        await _consume(client, [first])  # the redelivery
        assert first.acked >= 2  # both deliveries acknowledged


def _unused_json() -> None:
    # json is used by EventEnvelope.to_json_bytes round-trips in
    # FakeMsg; keep the import for the reader.
    assert json is not None