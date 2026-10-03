"""End-to-end integration: ingest + normalize + publish + consume.

This test verifies the full pipeline from raw ingestion through
NATS JetStream messaging without requiring a live NATS server.
All external dependencies are mocked.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Ensure psycopg2 is mocked before any imports
sys.modules["psycopg2"] = MagicMock()

from debian_observer.events import EventEnvelope
from debian_observer.ingestion.acquisition import UDDDataSource
from debian_observer.ingestion.pipeline import IngestionPipeline
from debian_observer.messaging.client import NATSClient
from debian_observer.messaging.consumer import EventConsumer
from debian_observer.messaging.publisher import EventPublisher

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_uddb_source() -> UDDDataSource:
    """Return a UDDDataSource that does not hit the network."""
    return UDDDataSource(table="packages", limit=3)


@pytest.fixture
def mock_nats_client() -> MagicMock:
    """Return a fully mocked NATS client with JetStream."""
    client = MagicMock(spec=NATSClient)
    client.is_connected = True

    mock_js = MagicMock()
    mock_publish_future = MagicMock()
    mock_publish_future.seq = 1
    mock_js.publish = AsyncMock(return_value=mock_publish_future)
    mock_js.add_stream = AsyncMock()

    client.jetstream = mock_js
    return client


# ---------------------------------------------------------------------------
# End-to-end pipeline test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ingest_normalize_publish_consume(mock_nats_client: MagicMock) -> None:
    """Full flow: ingest UDD → normalize → publish → consume."""

    # 1. Ingest mocked UDD records
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("hello", "2.10-3", "debian", "sid", "amd64"),
        ("world", "1.0", "debian", "trixie", "arm64"),
        ("foo", "3.0-1", "debian", "bookworm", "all"),
    ]
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("psycopg2.connect", return_value=mock_conn):
        source = UDDDataSource(table="packages", limit=3)
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline = IngestionPipeline(
                raw_dir=Path(tmpdir),
                parquet_dir=None,
            )
            result = pipeline.run(source)

    assert result.record_count == 3
    assert len(result.events) == 3
    assert all(isinstance(e, EventEnvelope) for e in result.events)

    # 2. Publish events to mocked NATS
    publisher = EventPublisher(client=mock_nats_client)
    published_seqs = []
    for event in result.events:
        seq = await publisher.publish(event)
        published_seqs.append(seq)

    assert len(published_seqs) == 3
    assert all(seq == 1 for seq in published_seqs)
    assert mock_nats_client.jetstream.publish.await_count == 3

    # 3. Consume events from mocked NATS
    consumed_events: list[EventEnvelope] = []

    async def handler(msg: Any, event: EventEnvelope, data: dict, ack: Any) -> None:
        consumed_events.append(event)
        await ack()

    # Build a mock subscription that immediately delivers the 3 events
    mock_subscription = MagicMock()
    mock_subscription.unsubscribe = AsyncMock()

    async def mock_subscribe(subject: str, *, cb: Any, **kwargs: Any) -> MagicMock:
        for event in result.events:
            mock_msg = MagicMock()
            mock_msg.data = event.to_json_bytes()
            mock_msg.ack = AsyncMock()
            await cb(mock_msg)
        return mock_subscription

    mock_nats_client.jetstream.subscribe = AsyncMock(side_effect=mock_subscribe)

    consumer = EventConsumer(
        client=mock_nats_client,
        handler=handler,
        stream_name="debian-events",
        durable_name="e2e-test-consumer",
    )
    await consumer.subscribe("debian.package_version.observed")
    await consumer.close()

    # 4. Validate round-trip
    assert len(consumed_events) == 3
    published_ids = {str(e.event_id) for e in result.events}
    consumed_ids = {str(e.event_id) for e in consumed_events}
    assert published_ids == consumed_ids

    # 5. Validate event payloads
    for event in consumed_events:
        assert event.event_type == "PackageVersionObserved"
        assert event.source == "udd"
        assert "package_id" in event.payload
        assert "version" in event.payload
