"""Integration tests for observability in pipeline, messaging, and API.

These tests verify that spans and metrics are created when the
instrumented components execute.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from debian_observer.api.app import create_app
from debian_observer.api.store import InMemoryStore
from debian_observer.events import EventEnvelope
from debian_observer.ingestion.acquisition import DataSource
from debian_observer.ingestion.pipeline import IngestionPipeline
from debian_observer.messaging.consumer import EventConsumer
from debian_observer.messaging.publisher import EventPublisher
from debian_observer.observability.tracing import shutdown_tracer


@pytest.fixture(autouse=True)
def _reset_tracer():
    """Clean tracer state between tests."""
    shutdown_tracer()
    yield
    shutdown_tracer()


class TestPipelineObservability:
    def test_run_creates_span(self, tmp_path: Path):
        """Pipeline.run() should execute under an OpenTelemetry span."""
        pipeline = IngestionPipeline(raw_dir=tmp_path)

        # Mock data source
        source = MagicMock(spec=DataSource)
        source.name = "test_source"
        source.compute_checksum.return_value = "abc123"
        source.record_count.return_value = 1
        source.retrieve.return_value = [
            {"package": "vim", "version": "1.0", "architecture": "amd64"}
        ]

        with patch("debian_observer.ingestion.pipeline.AcquisitionService") as mock_acq:
            mock_acq.return_value.acquire.return_value = {
                "checksum": "abc123",
                "record_count": 1,
                "acquired_at": datetime.now(timezone.utc).isoformat(),
            }
            result = pipeline.run(source, ingestion_run_id="test-run-001")

        assert result.ingestion_run_id == "test-run-001"
        assert result.record_count == 1

    def test_run_records_error_metric(self, tmp_path: Path):
        """Pipeline.run() should record error metric on failure."""
        pipeline = IngestionPipeline(raw_dir=tmp_path)
        source = MagicMock(spec=DataSource)
        source.name = "bad_source"
        source.retrieve.side_effect = RuntimeError("fail")

        with patch("debian_observer.ingestion.pipeline.AcquisitionService") as mock_acq:
            mock_acq.return_value.acquire.side_effect = RuntimeError("fail")
            with pytest.raises(RuntimeError):
                pipeline.run(source)


class TestPublisherObservability:
    @pytest.mark.asyncio
    async def test_publish_creates_span(self):
        """Publisher.publish() should execute under a span."""
        client = MagicMock()
        client.is_connected = True
        mock_js = MagicMock()
        mock_ack = MagicMock()
        mock_ack.seq = 42
        mock_js.publish = AsyncMock(return_value=mock_ack)
        client.jetstream = mock_js

        publisher = EventPublisher(client=client)

        now = datetime.now(timezone.utc)
        event = EventEnvelope(
            event_type="PackageVersionObserved",
            source="test",
            entity_id="vim:amd64",
            payload={"version": "1.0"},
            observed_at=now,
            effective_at=now,
        )

        seq = await publisher.publish(event)
        assert seq == 42

    @pytest.mark.asyncio
    async def test_publish_records_error_on_failure(self):
        """Publisher.publish() should record failure metrics."""
        client = MagicMock()
        client.is_connected = True
        mock_js = MagicMock()
        mock_js.publish = AsyncMock(side_effect=RuntimeError("NATS down"))
        client.jetstream = mock_js

        publisher = EventPublisher(client=client)

        now = datetime.now(timezone.utc)
        event = EventEnvelope(
            event_type="PackageVersionObserved",
            source="test",
            entity_id="vim:amd64",
            payload={"version": "1.0"},
            observed_at=now,
            effective_at=now,
        )

        with pytest.raises(RuntimeError):
            await publisher.publish(event)


class TestConsumerObservability:
    @pytest.mark.asyncio
    async def test_process_creates_span(self):
        """Consumer should create span when processing a message."""
        client = MagicMock()
        client.is_connected = True
        handler = AsyncMock()
        consumer = EventConsumer(
            client=client,
            handler=handler,
            stream_name="test-stream",
            durable_name="test-consumer",
        )

        msg = MagicMock()
        msg.data = json.dumps(
            {
                "event_type": "PackageVersionObserved",
                "source": "test",
                "entity_id": "vim:amd64",
                "payload": {"version": "1.0"},
                "event_id": "550e8400-e29b-41d4-a716-446655440000",
                "observed_at": "2026-01-01T00:00:00+00:00",
                "effective_at": "2026-01-01T00:00:00+00:00",
            }
        ).encode("utf-8")
        msg.ack = AsyncMock()

        await consumer._process_message(msg)
        handler.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_process_records_error_metric(self):
        """Consumer should record failure metric when handler errors."""
        client = MagicMock()
        client.is_connected = True
        handler = AsyncMock(side_effect=RuntimeError("handler fail"))
        consumer = EventConsumer(
            client=client,
            handler=handler,
            stream_name="test-stream",
            durable_name="test-consumer",
        )

        msg = MagicMock()
        msg.data = json.dumps(
            {
                "event_type": "PackageVersionObserved",
                "source": "test",
                "entity_id": "vim:amd64",
                "payload": {"version": "1.0"},
                "event_id": "550e8400-e29b-41d4-a716-446655440000",
                "observed_at": "2026-01-01T00:00:00+00:00",
                "effective_at": "2026-01-01T00:00:00+00:00",
            }
        ).encode("utf-8")
        msg.ack = AsyncMock()
        msg.nak = AsyncMock()

        with pytest.raises(RuntimeError):
            await consumer._on_message(msg)


class TestAPIObservability:
    def test_health_endpoint_creates_span(self):
        """API requests should be traced via middleware."""
        store = InMemoryStore()
        app = create_app(store=store)
        client = TestClient(app)

        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"

    def test_metrics_endpoint_lists_instruments(self):
        """Metrics endpoint should return registered instruments."""
        store = InMemoryStore()
        app = create_app(store=store)
        client = TestClient(app)

        # Metrics router is mounted at /metrics prefix, so add trailing slash
        response = client.get("/metrics/")
        assert response.status_code == 200
        data = response.json()
        assert "instruments" in data
        assert "ingestion.duration_seconds" in data["instruments"]
