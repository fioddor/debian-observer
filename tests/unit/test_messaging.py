"""Unit tests for Debian Observatory NATS messaging layer.

These tests verify that the messaging layer correctly:
- Connects to NATS (async)
- Publishes events to JetStream streams
- Consumes events with idempotency and acknowledgement
- Configures streams with proper retention and subject hierarchy

The NATS server is mocked in unit tests. Integration tests with a
real NATS server are deferred to a separate test suite.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from debian_observer.events import EventEnvelope
from debian_observer.events.types import (
    ObservationMeta,
    PackageVersionObservedPayload,
)
from debian_observer.messaging.client import NATSClient
from debian_observer.messaging.config import StreamConfig
from debian_observer.messaging.publisher import EventPublisher
from debian_observer.messaging.consumer import EventConsumer, IngestionHandler


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_event() -> EventEnvelope:
    """Return a valid PackageVersionObserved event."""
    obs = ObservationMeta(source="udd")
    payload = PackageVersionObservedPayload(
        package_id="package:libc6:amd64",
        version="2.37-12",
        distribution="trixie",
        architecture="amd64",
        component="main",
        priority="required",
        observation=obs,
    )
    return EventEnvelope.from_payload(
        event_type="PackageVersionObserved",
        source="udd",
        entity_id="package:libc6:amd64",
        payload_model=payload,
        ingestion_run_id="run-001",
    )


@pytest.fixture
def mock_nats_client() -> MagicMock:
    """Return a fully mocked NATS client."""
    client = MagicMock(spec=NATSClient)
    client.connect = AsyncMock()
    client.close = AsyncMock()
    client.is_connected = True
    client.jetstream = AsyncMock()
    return client


@pytest.fixture
def mock_js() -> MagicMock:
    """Return a mocked JetStream context."""
    js = MagicMock()
    js.publish = AsyncMock(return_value=MagicMock(seq=1))
    js.subscribe = AsyncMock()
    js.add_stream = AsyncMock()
    return js


# ---------------------------------------------------------------------------
# Stream configuration tests
# ---------------------------------------------------------------------------

class TestStreamConfig:
    """Tests for JetStream stream configuration."""

    def test_default_stream_name(self) -> None:
        config = StreamConfig()
        assert config.stream_name == "debian.events"

    def test_subject_prefix(self) -> None:
        config = StreamConfig()
        assert config.subject_prefix == "debian."

    def test_event_subjects(self) -> None:
        config = StreamConfig()
        assert "debian.package.observed" in config.subjects
        assert "debian.package_version.observed" in config.subjects
        assert "debian.release.observed" in config.subjects
        assert "debian.migration.observed" in config.subjects

    def test_storage_type(self) -> None:
        config = StreamConfig()
        assert config.storage in ("file", "memory")

    def test_retention_policy(self) -> None:
        config = StreamConfig()
        assert config.retention == "limits"

    def test_to_nats_config(self) -> None:
        config = StreamConfig()
        nats_config = config.to_nats_config()
        assert nats_config["name"] == "debian.events"
        assert nats_config["subjects"] == config.subjects
        assert "retention" in nats_config
        assert "max_msgs" in nats_config
        assert "max_age" in nats_config


# ---------------------------------------------------------------------------
# NATS client tests
# ---------------------------------------------------------------------------

class TestNATSClient:
    """Tests for NATS client connection management."""

    @pytest.mark.asyncio
    async def test_connect(self) -> None:
        """Client should connect to NATS server."""
        client = NATSClient(servers=["nats://localhost:4222"])
        # Mock the underlying nats connection
        mock_conn = AsyncMock()
        mock_conn.jetstream = AsyncMock(return_value=MagicMock())
        mock_conn.drain = AsyncMock()
        mock_conn.close = AsyncMock()
        client._nc = mock_conn
        client._js = MagicMock()
        client._connected = True

        assert client.is_connected is True

    @pytest.mark.asyncio
    async def test_disconnect(self) -> None:
        """Client should disconnect gracefully."""
        client = NATSClient(servers=["nats://localhost:4222"])
        mock_conn = AsyncMock()
        mock_conn.drain = AsyncMock()
        mock_conn.close = AsyncMock()
        client._nc = mock_conn
        client._js = MagicMock()
        client._connected = True

        await client.close()
        mock_conn.drain.assert_awaited_once()
        mock_conn.close.assert_awaited_once()
        assert client.is_connected is False

    @pytest.mark.asyncio
    async def test_jetstream_context(self) -> None:
        """Client should expose JetStream context."""
        client = NATSClient(servers=["nats://localhost:4222"])
        mock_js = MagicMock()
        client._js = mock_js
        client._connected = True

        js = client.jetstream
        assert js is mock_js

    def test_default_servers(self) -> None:
        client = NATSClient()
        assert client.servers == ["nats://localhost:4222"]


# ---------------------------------------------------------------------------
# Publisher tests
# ---------------------------------------------------------------------------

class TestEventPublisher:
    """Tests for event publishing to JetStream."""

    @pytest.mark.asyncio
    async def test_publish_event(self, mock_nats_client: MagicMock, sample_event: EventEnvelope) -> None:
        """Publisher should publish an event to the correct subject."""
        mock_js = MagicMock()
        mock_js.publish = AsyncMock(return_value=MagicMock(seq=42))
        mock_nats_client.jetstream = mock_js

        publisher = EventPublisher(client=mock_nats_client)
        seq = await publisher.publish(sample_event)

        assert seq == 42
        mock_js.publish.assert_awaited_once()
        call_args = mock_js.publish.call_args[0]
        assert call_args[0] == "debian.package_version.observed"
        assert json.loads(call_args[1]) == json.loads(sample_event.to_json_bytes().decode())

    @pytest.mark.asyncio
    async def test_publish_routes_by_event_type(self, mock_nats_client: MagicMock) -> None:
        """Publisher should route events to correct subjects based on event type."""
        publisher = EventPublisher(client=mock_nats_client)

        test_cases = [
            ("PackageObserved", "debian.package.observed"),
            ("PackageVersionObserved", "debian.package_version.observed"),
            ("ReleaseObserved", "debian.release.observed"),
            ("MigrationObserved", "debian.migration.observed"),
        ]

        for event_type, expected_subject in test_cases:
            mock_nats_client.reset_mock()
            mock_js = MagicMock()
            mock_js.publish = AsyncMock(return_value=MagicMock(seq=1))
            mock_nats_client.jetstream = mock_js

            event = EventEnvelope(
                event_type=event_type,
                source="udd",
                entity_id="test",
                payload={},
            )
            await publisher.publish(event)

            call_args = mock_js.publish.call_args[0]
            assert call_args[0] == expected_subject, f"Wrong subject for {event_type}"

    @pytest.mark.asyncio
    async def test_publish_batch(self, mock_nats_client: MagicMock, sample_event: EventEnvelope) -> None:
        """Publisher should publish multiple events in batch."""
        mock_js = MagicMock()
        mock_js.publish = AsyncMock(return_value=MagicMock(seq=1))
        mock_nats_client.jetstream = mock_js

        publisher = EventPublisher(client=mock_nats_client)
        events = [sample_event, sample_event, sample_event]
        seqs = await publisher.publish_batch(events)

        assert len(seqs) == 3
        assert mock_js.publish.call_count == 3

    @pytest.mark.asyncio
    async def test_publish_unconnected(self, mock_nats_client: MagicMock, sample_event: EventEnvelope) -> None:
        """Publisher should raise if client is not connected."""
        mock_nats_client.is_connected = False

        publisher = EventPublisher(client=mock_nats_client)
        with pytest.raises(RuntimeError, match="NATS client not connected"):
            await publisher.publish(sample_event)


# ---------------------------------------------------------------------------
# Consumer tests
# ---------------------------------------------------------------------------

class TestEventConsumer:
    """Tests for event consumption from JetStream."""

    def test_handler_signature(self) -> None:
        """Handler should require 4 parameters: msg, event, data, ack."""
        handler = MagicMock()
        consumer = EventConsumer(
            client=MagicMock(),
            handler=handler,
            stream_name="debian.events",
            durable_name="test-consumer",
        )
        assert consumer.handler is handler

    @pytest.mark.asyncio
    async def test_extracts_event_from_message(self, mock_nats_client: MagicMock) -> None:
        """Consumer should extract EventEnvelope from NATS message data."""
        mock_js = MagicMock()
        mock_nats_client.jetstream = mock_js

        # Build a NATS-like message mock
        event = EventEnvelope(
            event_type="PackageVersionObserved",
            source="udd",
            entity_id="package:libc6",
            payload={"test": "data"},
        )
        mock_msg = MagicMock()
        mock_msg.data = event.to_json_bytes()
        mock_msg.ack = AsyncMock()
        mock_msg._acked = True  # handler will ack

        handler_calls: list[Any] = []

        async def handler(msg: Any, event_env: EventEnvelope, data: dict, ack: Any) -> None:
            handler_calls.append((event_env, data))
            await ack()

        consumer = EventConsumer(
            client=mock_nats_client,
            handler=handler,
            stream_name="debian.events",
            durable_name="test-consumer",
        )

        await consumer._process_message(mock_msg)

        assert len(handler_calls) == 1
        assert handler_calls[0][0].event_type == "PackageVersionObserved"
        assert handler_calls[0][1] == {"test": "data"}
        # Ack is called twice: once by handler, once by consumer cleanup
        assert mock_msg.ack.await_count == 2

    @pytest.mark.asyncio
    async def test_acknowledges_after_handler(self, mock_nats_client: MagicMock) -> None:
        """Consumer should ack only after handler succeeds."""
        mock_msg = MagicMock()
        mock_msg.data = b'{"event_type":"test","source":"udd","entity_id":"x","payload":{}}'
        mock_msg.ack = AsyncMock()

        async def handler(msg: Any, event: EventEnvelope, data: dict, ack: Any) -> None:
            pass

        consumer = EventConsumer(
            client=MagicMock(),
            handler=handler,
            stream_name="debian.events",
            durable_name="test-consumer",
        )

        await consumer._process_message(mock_msg)
        mock_msg.ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_ack_on_handler_failure(self, mock_nats_client: MagicMock) -> None:
        """Consumer should not ack if handler raises an exception."""
        mock_msg = MagicMock()
        mock_msg.data = b'{"event_type":"test","source":"udd","entity_id":"x","payload":{}}'
        mock_msg.ack = AsyncMock()
        mock_msg.nak = AsyncMock()

        async def handler(msg: Any, event: EventEnvelope, data: dict, ack: Any) -> None:
            raise ValueError("Processing failed")

        consumer = EventConsumer(
            client=MagicMock(),
            handler=handler,
            stream_name="debian.events",
            durable_name="test-consumer",
        )

        with pytest.raises(ValueError, match="Processing failed"):
            await consumer._process_message(mock_msg)

        mock_msg.ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_naks_on_handler_failure(self, mock_nats_client: MagicMock) -> None:
        """Consumer should nak (negative ack) on handler failure."""
        mock_msg = MagicMock()
        mock_msg.data = b'{"event_type":"test","source":"udd","entity_id":"x","payload":{}}'
        mock_msg.ack = AsyncMock()
        mock_msg.nak = AsyncMock()

        async def handler(msg: Any, event: EventEnvelope, data: dict, ack: Any) -> None:
            raise ValueError("fail")

        consumer = EventConsumer(
            client=MagicMock(),
            handler=handler,
            stream_name="debian.events",
            durable_name="test-consumer",
        )

        # _on_message catches the exception and calls nak
        with pytest.raises(ValueError):
            await consumer._on_message(mock_msg)

        mock_msg.nak.assert_awaited_once()


# ---------------------------------------------------------------------------
# Idempotency tests
# ---------------------------------------------------------------------------

class TestIngestionHandler:
    """Tests for the standard ingestion handler pattern."""

    @pytest.mark.asyncio
    async def test_calls_ack_after_processing(self) -> None:
        """Handler should call ack after successful processing."""
        processed: list[str] = []

        async def mock_processor(event: EventEnvelope) -> None:
            processed.append(event.event_id)

        handler = IngestionHandler(processor=mock_processor)

        event = EventEnvelope(
            event_type="PackageVersionObserved",
            source="udd",
            entity_id="pkg:1",
            payload={},
        )
        mock_ack = AsyncMock()

        await handler.handle(event, mock_ack)

        assert len(processed) == 1
        mock_ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skips_duplicate_event_ids(self) -> None:
        """Handler should skip events with already-processed event IDs."""
        processed: list[str] = []

        async def mock_processor(event: EventEnvelope) -> None:
            processed.append(str(event.event_id))

        handler = IngestionHandler(processor=mock_processor)

        event1 = EventEnvelope(
            event_type="PackageVersionObserved",
            source="udd",
            entity_id="pkg:1",
            payload={},
        )
        mock_ack = AsyncMock()

        await handler.handle(event1, mock_ack)
        assert len(processed) == 1
        mock_ack.assert_awaited_once()

        # Same event again — should be skipped
        mock_ack2 = AsyncMock()
        await handler.handle(event1, mock_ack2)
        assert len(processed) == 1  # Not processed again
        mock_ack2.assert_awaited_once()  # But still acked

    @pytest.mark.asyncio
    async def test_different_events_not_skipped(self) -> None:
        """Handler should not skip events with different event IDs."""
        processed: list[str] = []

        async def mock_processor(event: EventEnvelope) -> None:
            processed.append(str(event.event_id))

        handler = IngestionHandler(processor=mock_processor)

        event1 = EventEnvelope(
            event_type="PackageVersionObserved",
            source="udd",
            entity_id="pkg:1",
            payload={},
        )
        event2 = EventEnvelope(
            event_type="PackageVersionObserved",
            source="udd",
            entity_id="pkg:2",
            payload={},
        )

        await handler.handle(event1, AsyncMock())
        await handler.handle(event2, AsyncMock())

        assert len(processed) == 2


# ---------------------------------------------------------------------------
# Subscription management tests
# ---------------------------------------------------------------------------

class TestConsumerLifecycle:
    """Tests for consumer subscribe/unsubscribe/close."""

    @pytest.mark.asyncio
    async def test_subscribe_requires_connection(self) -> None:
        """subscribe() should raise if client is not connected."""
        client = MagicMock()
        client.is_connected = False

        consumer = EventConsumer(
            client=client,
            handler=AsyncMock(),
            stream_name="debian.events",
            durable_name="test",
        )
        with pytest.raises(RuntimeError, match="not connected"):
            await consumer.subscribe("debian.package.observed")

    @pytest.mark.asyncio
    async def test_subscribe_success(self) -> None:
        """subscribe() should create a JetStream subscription."""
        mock_js = MagicMock()
        mock_js.subscribe = AsyncMock(return_value=MagicMock())
        client = MagicMock()
        client.is_connected = True
        client.jetstream = mock_js

        consumer = EventConsumer(
            client=client,
            handler=AsyncMock(),
            stream_name="debian.events",
            durable_name="test",
        )
        await consumer.subscribe("debian.package.observed")

        mock_js.subscribe.assert_awaited_once()
        call_kwargs = mock_js.subscribe.call_args.kwargs
        assert call_kwargs["stream"] == "debian.events"
        assert call_kwargs["durable"] == "test"

    @pytest.mark.asyncio
    async def test_unsubscribe(self) -> None:
        """unsubscribe() should clean up the subscription."""
        sub = MagicMock()
        sub.unsubscribe = AsyncMock()
        client = MagicMock()
        client.is_connected = True
        client.jetstream = MagicMock()
        client.jetstream.subscribe = AsyncMock(return_value=sub)

        consumer = EventConsumer(
            client=client,
            handler=AsyncMock(),
            stream_name="debian.events",
            durable_name="test",
        )
        await consumer.subscribe("debian.package.observed")
        await consumer.unsubscribe()

        sub.unsubscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close(self) -> None:
        """close() should unsubscribe and release resources."""
        sub = MagicMock()
        sub.unsubscribe = AsyncMock()
        client = MagicMock()
        client.is_connected = True
        client.jetstream = MagicMock()
        client.jetstream.subscribe = AsyncMock(return_value=sub)

        consumer = EventConsumer(
            client=client,
            handler=AsyncMock(),
            stream_name="debian.events",
            durable_name="test",
        )
        await consumer.subscribe("debian.package.observed")
        await consumer.close()

        sub.unsubscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_on_message_catches_exception(self) -> None:
        """_on_message should catch handler exceptions and nak."""
        mock_msg = MagicMock()
        mock_msg.data = b'{"event_type":"test","source":"udd","entity_id":"x","payload":{}}'
        mock_msg.ack = AsyncMock()
        mock_msg.nak = AsyncMock()

        async def handler(msg: Any, event: EventEnvelope, data: dict, ack: Any) -> None:
            raise ValueError("fail")

        consumer = EventConsumer(
            client=MagicMock(),
            handler=handler,
            stream_name="debian.events",
            durable_name="test",
        )

        with pytest.raises(ValueError):
            await consumer._on_message(mock_msg)

        mock_msg.nak.assert_awaited_once()
