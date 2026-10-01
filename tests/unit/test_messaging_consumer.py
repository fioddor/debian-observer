"""Unit tests for EventConsumer and IngestionHandler with mocked NATSClient.

These tests verify message processing, ack/nak behaviour, and idempotency
without a real NATS server.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from debian_observer.events.envelope import EventEnvelope
from debian_observer.messaging.consumer import EventConsumer, IngestionHandler


@pytest.fixture
def connected_client() -> MagicMock:
    """Return a mocked NATSClient that is_connected=True."""
    client = MagicMock()
    client.is_connected = True
    mock_js = MagicMock()
    mock_js.subscribe = AsyncMock(return_value=MagicMock())
    client.jetstream = mock_js
    return client


@pytest.fixture
def sample_event_envelope() -> EventEnvelope:
    """Return a valid event envelope."""
    return EventEnvelope(
        event_type="PackageVersionObserved",
        source="udd",
        entity_id="package:libc6",
        payload={"name": "libc6", "version": "2.37-12"},
    )


def _make_mock_msg(event: EventEnvelope) -> MagicMock:
    """Build a mock NATS message carrying the given event."""
    mock_msg = MagicMock()
    mock_msg.data = event.model_dump_json().encode("utf-8")
    mock_msg.ack = AsyncMock()
    mock_msg.nak = AsyncMock()
    return mock_msg


class TestEventConsumerSubscribe:
    """Tests for EventConsumer.subscribe()."""

    async def test_subscribe_raises_when_not_connected(self) -> None:
        """Should raise RuntimeError if client is not connected."""
        client = MagicMock()
        client.is_connected = False

        consumer = EventConsumer(
            client=client,
            handler=AsyncMock(),
            stream_name="debian-events",
            durable_name="test",
        )
        with pytest.raises(RuntimeError, match="not connected"):
            await consumer.subscribe("debian.package.observed")

    async def test_subscribe_success(self, connected_client: MagicMock) -> None:
        """Should create a JetStream subscription."""
        consumer = EventConsumer(
            client=connected_client,
            handler=AsyncMock(),
            stream_name="debian-events",
            durable_name="test-consumer",
        )
        await consumer.subscribe("debian.package.observed")

        connected_client.jetstream.subscribe.assert_awaited_once()
        call_kwargs = connected_client.jetstream.subscribe.call_args.kwargs
        assert call_kwargs["stream"] == "debian-events"
        assert call_kwargs["durable"] == "test-consumer"
        assert call_kwargs["cb"] == consumer._on_message


class TestEventConsumerOnMessage:
    """Tests for EventConsumer._on_message()."""

    async def test_on_message_ack_on_success(self, sample_event_envelope: EventEnvelope) -> None:
        """Should ack the message after successful handler execution."""
        mock_msg = _make_mock_msg(sample_event_envelope)
        handler_calls: list[Any] = []

        async def handler(
            msg: Any, event: EventEnvelope, data: dict, ack: Any
        ) -> None:
            handler_calls.append(event)
            await ack()

        consumer = EventConsumer(
            client=MagicMock(),
            handler=handler,
            stream_name="debian-events",
            durable_name="test",
        )

        await consumer._on_message(mock_msg)

        assert len(handler_calls) == 1
        assert handler_calls[0].event_type == "PackageVersionObserved"
        # ack is called by handler AND by consumer cleanup = 2 total
        assert mock_msg.ack.await_count == 2
        mock_msg.nak.assert_not_awaited()

    async def test_on_message_nak_on_exception(self, sample_event_envelope: EventEnvelope) -> None:
        """Should nak the message when handler raises an exception."""
        mock_msg = _make_mock_msg(sample_event_envelope)

        async def handler(msg: Any, event: EventEnvelope, data: dict, ack: Any) -> None:
            raise ValueError("processing failed")

        consumer = EventConsumer(
            client=MagicMock(),
            handler=handler,
            stream_name="debian-events",
            durable_name="test",
        )

        with pytest.raises(ValueError, match="processing failed"):
            await consumer._on_message(mock_msg)

        mock_msg.nak.assert_awaited_once()
        mock_msg.ack.assert_not_awaited()

    async def test_on_message_nak_when_msg_lacks_ack(self, sample_event_envelope: EventEnvelope) -> None:
        """Should not crash if msg lacks nak method."""
        mock_msg = _make_mock_msg(sample_event_envelope)
        del mock_msg.nak

        async def handler(msg: Any, event: EventEnvelope, data: dict, ack: Any) -> None:
            raise ValueError("fail")

        consumer = EventConsumer(
            client=MagicMock(),
            handler=handler,
            stream_name="debian-events",
            durable_name="test",
        )

        with pytest.raises(ValueError, match="fail"):
            await consumer._on_message(mock_msg)

    async def test_on_message_nak_exception_swallowed(self, sample_event_envelope: EventEnvelope) -> None:
        """Should swallow exception from nak() and still re-raise handler error."""
        mock_msg = _make_mock_msg(sample_event_envelope)
        mock_msg.nak = AsyncMock(side_effect=RuntimeError("nak failed"))

        async def handler(msg: Any, event: EventEnvelope, data: dict, ack: Any) -> None:
            raise ValueError("handler fail")

        consumer = EventConsumer(
            client=MagicMock(),
            handler=handler,
            stream_name="debian-events",
            durable_name="test",
        )

        with pytest.raises(ValueError, match="handler fail"):
            await consumer._on_message(mock_msg)

        mock_msg.nak.assert_awaited_once()


class TestEventConsumerProcessMessage:
    """Tests for EventConsumer._process_message()."""

    async def test_process_message_deserializes_event(self, sample_event_envelope: EventEnvelope) -> None:
        """Should deserialize msg.data into an EventEnvelope."""
        mock_msg = _make_mock_msg(sample_event_envelope)
        received_event: EventEnvelope | None = None

        async def handler(msg: Any, event: EventEnvelope, data: dict, ack: Any) -> None:
            nonlocal received_event
            received_event = event
            await ack()

        consumer = EventConsumer(
            client=MagicMock(),
            handler=handler,
            stream_name="debian-events",
            durable_name="test",
        )

        await consumer._process_message(mock_msg)

        assert received_event is not None
        assert received_event.event_type == "PackageVersionObserved"
        assert received_event.entity_id == "package:libc6"
        assert received_event.payload == {"name": "libc6", "version": "2.37-12"}

    async def test_process_message_records_metrics_on_success(self, sample_event_envelope: EventEnvelope) -> None:
        """Should record consumer duration and consumed metrics."""
        mock_msg = _make_mock_msg(sample_event_envelope)
        mock_metrics = MagicMock()

        async def handler(msg: Any, event: EventEnvelope, data: dict, ack: Any) -> None:
            await ack()

        consumer = EventConsumer(
            client=MagicMock(),
            handler=handler,
            stream_name="debian-events",
            durable_name="test",
        )
        with patch.object(consumer, "_metrics", mock_metrics):
            await consumer._process_message(mock_msg)

        mock_metrics.record_consumer_duration.assert_called_once()
        call_args = mock_metrics.record_consumer_duration.call_args
        assert call_args.kwargs["event_type"] == "PackageVersionObserved"
        mock_metrics.add_consumed.assert_called_once_with(1, event_type="PackageVersionObserved")

    async def test_process_message_records_metrics_on_failure(self, sample_event_envelope: EventEnvelope) -> None:
        """Should record failed metrics on handler exception."""
        mock_msg = _make_mock_msg(sample_event_envelope)
        mock_metrics = MagicMock()

        async def handler(msg: Any, event: EventEnvelope, data: dict, ack: Any) -> None:
            raise ValueError("fail")

        consumer = EventConsumer(
            client=MagicMock(),
            handler=handler,
            stream_name="debian-events",
            durable_name="test",
        )
        with patch.object(consumer, "_metrics", mock_metrics):
            with pytest.raises(ValueError, match="fail"):
                await consumer._process_message(mock_msg)

        mock_metrics.add_failed.assert_called_once_with(1, event_type="PackageVersionObserved")

    async def test_process_message_ack_exception_swallowed(self, sample_event_envelope: EventEnvelope) -> None:
        """Should swallow exception from msg.ack() in the ack callback."""
        mock_msg = _make_mock_msg(sample_event_envelope)
        mock_msg.ack = AsyncMock(side_effect=RuntimeError("ack failed"))

        async def handler(msg: Any, event: EventEnvelope, data: dict, ack: Any) -> None:
            await ack()

        consumer = EventConsumer(
            client=MagicMock(),
            handler=handler,
            stream_name="debian-events",
            durable_name="test",
        )

        await consumer._process_message(mock_msg)
        # Should not raise despite ack failing


class TestEventConsumerLifecycle:
    """Tests for unsubscribe and close."""

    async def test_unsubscribe_when_subscription_exists(self, connected_client: MagicMock) -> None:
        """Should unsubscribe when subscription is active."""
        sub = MagicMock()
        sub.unsubscribe = AsyncMock()
        connected_client.jetstream.subscribe = AsyncMock(return_value=sub)

        consumer = EventConsumer(
            client=connected_client,
            handler=AsyncMock(),
            stream_name="debian-events",
            durable_name="test",
        )
        await consumer.subscribe("debian.package.observed")
        await consumer.unsubscribe()

        sub.unsubscribe.assert_awaited_once()
        assert consumer._subscription is None

    async def test_unsubscribe_when_no_subscription(self) -> None:
        """Should be a no-op when no subscription exists."""
        consumer = EventConsumer(
            client=MagicMock(),
            handler=AsyncMock(),
            stream_name="debian-events",
            durable_name="test",
        )
        await consumer.unsubscribe()
        assert consumer._subscription is None

    async def test_unsubscribe_handles_exception(self, connected_client: MagicMock) -> None:
        """Should swallow unsubscribe exceptions."""
        sub = MagicMock()
        sub.unsubscribe = AsyncMock(side_effect=RuntimeError("broken"))
        connected_client.jetstream.subscribe = AsyncMock(return_value=sub)

        consumer = EventConsumer(
            client=connected_client,
            handler=AsyncMock(),
            stream_name="debian-events",
            durable_name="test",
        )
        await consumer.subscribe("debian.package.observed")
        await consumer.unsubscribe()  # should not raise

        assert consumer._subscription is None

    async def test_close_calls_unsubscribe(self, connected_client: MagicMock) -> None:
        """close() should call unsubscribe."""
        sub = MagicMock()
        sub.unsubscribe = AsyncMock()
        connected_client.jetstream.subscribe = AsyncMock(return_value=sub)

        consumer = EventConsumer(
            client=connected_client,
            handler=AsyncMock(),
            stream_name="debian-events",
            durable_name="test",
        )
        await consumer.subscribe("debian.package.observed")
        await consumer.close()

        sub.unsubscribe.assert_awaited_once()


class TestIngestionHandler:
    """Tests for IngestionHandler idempotency."""

    async def test_ingestion_handler_processes_and_acks(self) -> None:
        """Should process new events and ack after success."""
        processed: list[str] = []

        async def processor(event: EventEnvelope) -> None:
            processed.append(str(event.event_id))

        handler = IngestionHandler(processor=processor)
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

    async def test_ingestion_handler_duplicate_skips(self) -> None:
        """Should skip duplicate event IDs but still ack."""
        processed: list[str] = []

        async def processor(event: EventEnvelope) -> None:
            processed.append(str(event.event_id))

        handler = IngestionHandler(processor=processor)
        event = EventEnvelope(
            event_type="PackageVersionObserved",
            source="udd",
            entity_id="pkg:1",
            payload={},
        )
        mock_ack1 = AsyncMock()
        await handler.handle(event, mock_ack1)
        assert len(processed) == 1

        # Same event again — should be skipped
        mock_ack2 = AsyncMock()
        await handler.handle(event, mock_ack2)
        assert len(processed) == 1  # Not processed again
        mock_ack2.assert_awaited_once()  # But still acked

    async def test_ingestion_handler_different_events(self) -> None:
        """Should process events with different IDs."""
        processed: list[str] = []

        async def processor(event: EventEnvelope) -> None:
            processed.append(str(event.event_id))

        handler = IngestionHandler(processor=processor)
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
