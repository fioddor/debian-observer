"""Storage handlers for event-driven consumers.

Provides callable handler classes that integrate with :class:`ConsumerApp`
to persist consumed events to analytical storage.

Usage::

    from debian_observer.consumer_app import ConsumerApp
    from debian_observer.storage.handlers import PersistingHandler

    app = ConsumerApp()
    handler = PersistingHandler("data/parquet", flush_threshold=100)
    app.register_handler("PackageVersionObserved", handler)
    # Events consumed by app are automatically written to Parquet.
"""

from __future__ import annotations

from pathlib import Path

from debian_observer.events import EventEnvelope
from debian_observer.storage.parquet import ParquetWriter


class PersistingHandler:
    """Callable handler that writes consumed events to Parquet.

    Designed for registration with :class:`ConsumerApp`. Events are
    buffered in memory and flushed to Parquet when the buffer reaches
    ``flush_threshold`` or when :meth:`flush` is called explicitly.

    Args:
        output_dir: Directory for Parquet output files.
        flush_threshold: Number of buffered events before auto-flush.
    """

    def __init__(
        self,
        output_dir: str | Path,
        flush_threshold: int = 100,
    ) -> None:
        self.writer = ParquetWriter(output_dir)
        self.flush_threshold = flush_threshold
        self._buffer: list[EventEnvelope] = []

    def __call__(self, envelope: EventEnvelope) -> None:
        """Buffer an event; flush if threshold reached."""
        self._buffer.append(envelope)
        if len(self._buffer) >= self.flush_threshold:
            self.flush()

    def flush(self) -> None:
        """Write all buffered events to Parquet and clear the buffer.

        Safe to call when buffer is empty (no-op).
        """
        if not self._buffer:
            return

        # Use ingestion_run_id from first buffered event; fallback to auto
        run_id = self._buffer[0].ingestion_run_id or None
        self.writer.write_events(self._buffer, ingestion_run_id=run_id)
        self._buffer = []
