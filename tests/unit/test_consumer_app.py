"""Tests for ConsumerApp standalone entrypoint."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from debian_observer.consumer_app import ConsumerApp
from debian_observer.events.envelope import EventEnvelope
from debian_observer.idempotency import IdempotencyStore


class TestConsumerApp:
    """Unit tests for the standalone consumer entrypoint."""

    def test_register_handler_stores_callable(self) -> None:
        app = ConsumerApp()
        handler = MagicMock()
        app.register_handler("package.updated", handler)
        assert app.handlers.get("package.updated") is handler

    def test_process_skips_duplicate_event(self) -> None:
        app = ConsumerApp()
        app.idempotency_store.mark_processed("evt-dup")
        envelope = MagicMock(spec=EventEnvelope)
        envelope.event_id = "evt-dup"
        envelope.event_type = "package.updated"

        result = app.process(envelope)
        assert result is True  # silently deduplicated

    def test_process_calls_handler_for_new_event(self) -> None:
        app = ConsumerApp()
        handler = MagicMock()
        app.register_handler("package.updated", handler)

        envelope = MagicMock(spec=EventEnvelope)
        envelope.event_id = "evt-new"
        envelope.event_type = "package.updated"

        result = app.process(envelope)
        assert result is True
        handler.assert_called_once_with(envelope)

    def test_process_returns_false_when_no_handler(self) -> None:
        app = ConsumerApp()
        envelope = MagicMock(spec=EventEnvelope)
        envelope.event_id = "evt-new"
        envelope.event_type = "unknown.type"

        result = app.process(envelope)
        assert result is False
        assert app.idempotency_store.is_processed("evt-new") is False

    def test_process_marks_processed_after_handler(self) -> None:
        app = ConsumerApp()
        handler = MagicMock()
        app.register_handler("package.updated", handler)

        envelope = MagicMock(spec=EventEnvelope)
        envelope.event_id = "evt-123"
        envelope.event_type = "package.updated"

        app.process(envelope)
        assert app.idempotency_store.is_processed("evt-123") is True

    def test_injected_idempotency_store(self) -> None:
        store = IdempotencyStore()
        store.mark_processed("evt-pre")
        app = ConsumerApp(idempotency_store=store)
        assert app.idempotency_store.is_processed("evt-pre") is True

    def test_shutdown_sets_event(self) -> None:
        """shutdown() should set the internal shutdown event."""
        app = ConsumerApp()
        assert not app._shutdown_event.is_set()
        app.shutdown()
        assert app._shutdown_event.is_set()


class TestConsumerAppRun:
    """Tests for ConsumerApp.run() lifecycle."""

    async def test_run_lifecycle(self) -> None:
        """run() should connect, subscribe, and clean up on shutdown."""
        mock_client = MagicMock()
        mock_client.is_connected = True
        mock_client.jetstream = MagicMock()
        mock_client.connect = AsyncMock()
        mock_client.close = AsyncMock()

        mock_consumer = MagicMock()
        mock_consumer.subscribe = AsyncMock()
        mock_consumer.close = AsyncMock()

        with patch("debian_observer.consumer_app.NATSClient", return_value=mock_client):
            with patch("debian_observer.consumer_app.EventConsumer", return_value=mock_consumer):
                app = ConsumerApp()

                # Start run in background and trigger shutdown quickly
                run_task = asyncio.create_task(
                    app.run(stream="EVENTS", subject="debian.observer.>", durable="consumer-01")
                )
                await asyncio.sleep(0.01)
                app.shutdown()
                await run_task

        mock_client.connect.assert_awaited_once()
        mock_consumer.subscribe.assert_awaited_once_with("debian.observer.>")
        mock_consumer.close.assert_awaited_once()
        mock_client.close.assert_awaited_once()

    async def test_run_with_custom_servers(self) -> None:
        """run() should pass custom servers to NATSClient."""
        mock_client = MagicMock()
        mock_client.is_connected = True
        mock_client.jetstream = MagicMock()
        mock_client.connect = AsyncMock()
        mock_client.close = AsyncMock()

        mock_consumer = MagicMock()
        mock_consumer.subscribe = AsyncMock()
        mock_consumer.close = AsyncMock()

        with patch("debian_observer.consumer_app.NATSClient", return_value=mock_client) as MockNATS:
            with patch("debian_observer.consumer_app.EventConsumer", return_value=mock_consumer):
                app = ConsumerApp()
                run_task = asyncio.create_task(
                    app.run(
                        servers=["nats://remote:4222"],
                        stream="EVENTS",
                        subject="test.>",
                        durable="d-01",
                    )
                )
                await asyncio.sleep(0.01)
                app.shutdown()
                await run_task

        assert MockNATS.call_args.kwargs["servers"] == ["nats://remote:4222"]

    async def test_run_wrapped_handler_processes_and_acks(self) -> None:
        """The internal wrapped handler should call process() and ack."""
        mock_client = MagicMock()
        mock_client.is_connected = True
        mock_client.jetstream = MagicMock()
        mock_client.connect = AsyncMock()
        mock_client.close = AsyncMock()

        mock_consumer = MagicMock()
        mock_consumer.subscribe = AsyncMock()
        mock_consumer.close = AsyncMock()

        handler_calls: list[Any] = []

        with patch("debian_observer.consumer_app.NATSClient", return_value=mock_client):
            with patch("debian_observer.consumer_app.EventConsumer", return_value=mock_consumer) as MockConsumer:
                app = ConsumerApp()
                user_handler = MagicMock()
                app.register_handler("PackageVersionObserved", user_handler)

                run_task = asyncio.create_task(
                    app.run(stream="EVENTS", subject="debian.observer.>", durable="consumer-01")
                )
                await asyncio.sleep(0.01)

                # Simulate the internal wrapped handler being called
                # Extract the handler that was passed to EventConsumer
                call_kwargs = MockConsumer.call_args.kwargs
                wrapped_handler = call_kwargs["handler"]

                envelope = EventEnvelope(
                    event_type="PackageVersionObserved",
                    source="udd",
                    entity_id="pkg:1",
                    payload={"name": "test"},
                )
                mock_ack = AsyncMock()
                await wrapped_handler(MagicMock(), envelope, envelope.payload, mock_ack)

                user_handler.assert_called_once_with(envelope)
                mock_ack.assert_awaited_once()

                app.shutdown()
                await run_task

    async def test_run_wrapped_handler_acks_when_no_handler(self) -> None:
        """The wrapped handler should ack even when no user handler is registered."""
        mock_client = MagicMock()
        mock_client.is_connected = True
        mock_client.jetstream = MagicMock()
        mock_client.connect = AsyncMock()
        mock_client.close = AsyncMock()

        mock_consumer = MagicMock()
        mock_consumer.subscribe = AsyncMock()
        mock_consumer.close = AsyncMock()

        with patch("debian_observer.consumer_app.NATSClient", return_value=mock_client):
            with patch("debian_observer.consumer_app.EventConsumer", return_value=mock_consumer) as MockConsumer:
                app = ConsumerApp()
                # No handlers registered

                run_task = asyncio.create_task(
                    app.run(stream="EVENTS", subject="debian.observer.>", durable="consumer-01")
                )
                await asyncio.sleep(0.01)

                call_kwargs = MockConsumer.call_args.kwargs
                wrapped_handler = call_kwargs["handler"]

                envelope = EventEnvelope(
                    event_type="UnknownEvent",
                    source="udd",
                    entity_id="pkg:1",
                    payload={},
                )
                mock_ack = AsyncMock()
                await wrapped_handler(MagicMock(), envelope, envelope.payload, mock_ack)

                mock_ack.assert_awaited_once()

                app.shutdown()
                await run_task
