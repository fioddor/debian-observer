"""Tests for ConsumerApp standalone entrypoint."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from debian_observer.consumer_app import ConsumerApp
from debian_observer.events.envelope import EventEnvelope
from debian_observer.idempotency import IdempotencyStore
from debian_observer.messaging.consumer import EventConsumer as MessagingConsumer


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
