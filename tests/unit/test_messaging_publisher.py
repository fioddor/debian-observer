"""Unit tests for EventPublisher with mocked NATSClient.

These tests verify publishing, routing, metrics, and error handling
without a real NATS server.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from debian_observer.events.envelope import EventEnvelope
from debian_observer.messaging.config import StreamConfig
from debian_observer.messaging.publisher import EventPublisher


@pytest.fixture
def sample_event() -> EventEnvelope:
    """Return a valid event for publishing tests."""
    return EventEnvelope(
        event_type="PackageVersionObserved",
        source="udd",
        entity_id="package:libc6",
        payload={"name": "libc6", "version": "2.37-12"},
    )


@pytest.fixture
def connected_client() -> MagicMock:
    """Return a mocked NATSClient that is_connected=True."""
    client = MagicMock()
    client.is_connected = True
    mock_js = MagicMock()
    mock_js.publish = AsyncMock(return_value=MagicMock(seq=42))
    client.jetstream = mock_js
    return client


class TestEventPublisherPublish:
    """Tests for EventPublisher.publish()."""

    async def test_publish_raises_when_not_connected(self, sample_event: EventEnvelope) -> None:
        """Should raise RuntimeError if NATS client is not connected."""
        client = MagicMock()
        client.is_connected = False

        publisher = EventPublisher(client=client)
        with pytest.raises(RuntimeError, match="NATS client not connected"):
            await publisher.publish(sample_event)

    async def test_publish_raises_when_no_subject_mapping(self, connected_client: MagicMock) -> None:
        """Should raise ValueError if event type has no mapped subject."""
        publisher = EventPublisher(client=connected_client)
        event = EventEnvelope(
            event_type="UnknownEvent",
            source="udd",
            entity_id="test",
            payload={},
        )
        with pytest.raises(ValueError, match="No subject mapping"):
            await publisher.publish(event)

    async def test_publish_success(self, connected_client: MagicMock, sample_event: EventEnvelope) -> None:
        """Should publish and return the JetStream sequence number."""
        publisher = EventPublisher(client=connected_client)
        seq = await publisher.publish(sample_event)

        assert seq == 42
        connected_client.jetstream.publish.assert_awaited_once()
        call_args = connected_client.jetstream.publish.call_args[0]
        assert call_args[0] == "debian.package_version.observed"

    async def test_publish_records_metrics_and_tracing(self, connected_client: MagicMock, sample_event: EventEnvelope) -> None:
        """Should record metrics and tracing spans on success."""
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__ = MagicMock(return_value=mock_span)
        mock_tracer.start_as_current_span.return_value.__exit__ = MagicMock(return_value=False)

        mock_metrics = MagicMock()

        publisher = EventPublisher(client=connected_client)
        with patch.object(publisher, "_tracer", mock_tracer):
            with patch.object(publisher, "_metrics", mock_metrics):
                seq = await publisher.publish(sample_event)

        assert seq == 42
        mock_tracer.start_as_current_span.assert_called_once_with("messaging.publish")
        mock_span.set_attribute.assert_any_call("messaging.system", "NATS")
        mock_span.set_attribute.assert_any_call("messaging.destination", "debian.package_version.observed")
        mock_span.set_attribute.assert_any_call("event.type", "PackageVersionObserved")
        mock_metrics.add_published.assert_called_once_with(
            1, subject="debian.package_version.observed", event_type="PackageVersionObserved"
        )
        mock_span.set_attribute.assert_any_call("messaging.sequence", 42)

    async def test_publish_exception_updates_metrics(self, connected_client: MagicMock, sample_event: EventEnvelope) -> None:
        """Should record failed metrics and span error on exception."""
        connected_client.jetstream.publish = AsyncMock(side_effect=RuntimeError("broker down"))

        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__ = MagicMock(return_value=mock_span)
        mock_tracer.start_as_current_span.return_value.__exit__ = MagicMock(return_value=False)

        mock_metrics = MagicMock()

        publisher = EventPublisher(client=connected_client)
        with patch.object(publisher, "_tracer", mock_tracer):
            with patch.object(publisher, "_metrics", mock_metrics):
                with pytest.raises(RuntimeError, match="broker down"):
                    await publisher.publish(sample_event)

        mock_metrics.add_failed.assert_called_once_with(
            1, subject="debian.package_version.observed", event_type="PackageVersionObserved"
        )
        mock_span.set_status.assert_called_once()
        mock_span.record_exception.assert_called_once()

    async def test_publish_custom_stream_config(self, connected_client: MagicMock) -> None:
        """Should use custom StreamConfig for subject mapping."""
        config = StreamConfig()
        publisher = EventPublisher(client=connected_client, stream_config=config)
        event = EventEnvelope(
            event_type="PackageObserved",
            source="udd",
            entity_id="pkg:1",
            payload={},
        )
        seq = await publisher.publish(event)
        assert seq == 42
        call_args = connected_client.jetstream.publish.call_args[0]
        assert call_args[0] == "debian.package.observed"


class TestEventPublisherBatch:
    """Tests for EventPublisher.publish_batch()."""

    async def test_publish_batch(self, connected_client: MagicMock, sample_event: EventEnvelope) -> None:
        """Should publish multiple events and return sequence numbers."""
        connected_client.jetstream.publish = AsyncMock(
            side_effect=[MagicMock(seq=1), MagicMock(seq=2)]
        )

        publisher = EventPublisher(client=connected_client)
        events = [sample_event, sample_event]
        seqs = await publisher.publish_batch(events)

        assert seqs == [1, 2]
        assert connected_client.jetstream.publish.call_count == 2

    async def test_publish_batch_empty(self, connected_client: MagicMock) -> None:
        """Should return empty list for empty input."""
        publisher = EventPublisher(client=connected_client)
        seqs = await publisher.publish_batch([])
        assert seqs == []
        connected_client.jetstream.publish.assert_not_awaited()

    async def test_publish_batch_with_none_on_failure(self, connected_client: MagicMock, sample_event: EventEnvelope) -> None:
        """Should propagate exception if a single publish fails."""
        connected_client.jetstream.publish = AsyncMock(
            side_effect=[MagicMock(seq=1), RuntimeError("fail")]
        )

        publisher = EventPublisher(client=connected_client)
        events = [sample_event, sample_event]
        with pytest.raises(RuntimeError, match="fail"):
            await publisher.publish_batch(events)
