"""Unit tests for PersistingHandler with mocked ParquetWriter.

These tests verify buffering, flushing, and run-id propagation
without writing real Parquet files.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from debian_observer.events.envelope import EventEnvelope
from debian_observer.storage.handlers import PersistingHandler


@pytest.fixture
def mock_writer() -> MagicMock:
    """Return a mocked ParquetWriter."""
    return MagicMock()


@pytest.fixture
def sample_event() -> EventEnvelope:
    """Return a valid event envelope."""
    return EventEnvelope(
        event_type="PackageVersionObserved",
        source="udd",
        entity_id="package:libc6",
        payload={"name": "libc6"},
        ingestion_run_id="run-abc",
    )


class TestPersistingHandlerBuffering:
    """Tests for event buffering behaviour."""

    def test_buffer_event_no_flush(self, mock_writer: MagicMock, sample_event: EventEnvelope) -> None:
        """Single event below threshold should not trigger flush."""
        handler = PersistingHandlersWithMockWriter(mock_writer, flush_threshold=5)
        handler(sample_event)

        mock_writer.write_events.assert_not_called()
        assert len(handler._buffer) == 1

    def test_flush_on_threshold(self, mock_writer: MagicMock, sample_event: EventEnvelope) -> None:
        """Reaching flush_threshold should auto-flush."""
        handler = PersistingHandlersWithMockWriter(mock_writer, flush_threshold=3)
        handler(sample_event)
        handler(sample_event)
        handler(sample_event)

        mock_writer.write_events.assert_called_once()
        assert len(handler._buffer) == 0

    def test_flush_explicit(self, mock_writer: MagicMock, sample_event: EventEnvelope) -> None:
        """Manual flush() should write buffered events."""
        handler = PersistingHandlersWithMockWriter(mock_writer, flush_threshold=100)
        handler(sample_event)
        handler(sample_event)
        handler.flush()

        mock_writer.write_events.assert_called_once()
        assert len(handler._buffer) == 0

    def test_flush_empty_noop(self, mock_writer: MagicMock) -> None:
        """Flushing an empty buffer should not call writer."""
        handler = PersistingHandlersWithMockWriter(mock_writer, flush_threshold=10)
        handler.flush()

        mock_writer.write_events.assert_not_called()
        assert len(handler._buffer) == 0


class TestPersistingHandlerRunId:
    """Tests for ingestion_run_id handling."""

    def test_uses_ingestion_run_id(self, mock_writer: MagicMock) -> None:
        """Should pass ingestion_run_id from first buffered event."""
        event = EventEnvelope(
            event_type="PackageVersionObserved",
            source="udd",
            entity_id="pkg:1",
            payload={},
            ingestion_run_id="run-xyz",
        )
        handler = PersistingHandlersWithMockWriter(mock_writer, flush_threshold=1)
        handler(event)

        mock_writer.write_events.assert_called_once()
        call_kwargs = mock_writer.write_events.call_args.kwargs
        assert call_kwargs.get("ingestion_run_id") == "run-xyz"

    def test_uses_none_when_no_run_id(self, mock_writer: MagicMock) -> None:
        """Should pass None when events have no ingestion_run_id."""
        event = EventEnvelope(
            event_type="PackageVersionObserved",
            source="udd",
            entity_id="pkg:1",
            payload={},
            ingestion_run_id=None,
        )
        handler = PersistingHandlersWithMockWriter(mock_writer, flush_threshold=1)
        handler(event)

        mock_writer.write_events.assert_called_once()
        call_kwargs = mock_writer.write_events.call_args.kwargs
        assert call_kwargs.get("ingestion_run_id") is None


class TestPersistingHandlerMultipleFlush:
    """Tests for multiple flush interactions."""

    def test_multiple_events_preserved_in_buffer(self, mock_writer: MagicMock, sample_event: EventEnvelope) -> None:
        """Buffer should accumulate events until flush."""
        handler = PersistingHandlersWithMockWriter(mock_writer, flush_threshold=10)
        handler(sample_event)
        handler(sample_event)
        handler(sample_event)

        assert len(handler._buffer) == 3
        mock_writer.write_events.assert_not_called()

    def test_threshold_exact_match(self, mock_writer: MagicMock, sample_event: EventEnvelope) -> None:
        """Flush should trigger exactly at threshold."""
        handler = PersistingHandlersWithMockWriter(mock_writer, flush_threshold=2)
        handler(sample_event)
        mock_writer.write_events.assert_not_called()
        handler(sample_event)
        mock_writer.write_events.assert_called_once()


# ---------------------------------------------------------------------------
# Helper: construct PersistingHandler with injected mock writer
# ---------------------------------------------------------------------------

class PersistingHandlersWithMockWriter(PersistingHandler):
    """Test subclass that injects a mock ParquetWriter."""

    def __init__(self, mock_writer: MagicMock, flush_threshold: int = 100) -> None:
        # Bypass parent __init__ to avoid real filesystem setup
        self.writer = mock_writer
        self.flush_threshold = flush_threshold
        self._buffer: list[EventEnvelope] = []
