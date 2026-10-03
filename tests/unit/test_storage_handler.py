"""Tests for PersistingHandler — consumer-to-storage integration."""

from __future__ import annotations

import tempfile
from pathlib import Path

from debian_observer.events import EventEnvelope
from debian_observer.events.types import ObservationMeta, PackageVersionObservedPayload
from debian_observer.storage.handlers import PersistingHandler


def _make_event(run_id: str = "run-test") -> EventEnvelope:
    payload = PackageVersionObservedPayload(
        package_id="hello:amd64",
        version="2.10-3",
        distribution="sid",
        architecture="amd64",
        component="main",
        priority="optional",
        observation=ObservationMeta(source="udd"),
    )
    return EventEnvelope.from_payload(
        event_type="PackageVersionObserved",
        source="udd",
        entity_id="hello:amd64",
        payload_model=payload,
        ingestion_run_id=run_id,
    )


class TestPersistingHandler:
    def test_buffers_events_without_writing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            handler = PersistingHandler(output_dir=tmpdir, flush_threshold=5)
            handler(_make_event())
            handler(_make_event())
            # No flush yet — threshold not reached
            assert len(list(Path(tmpdir).glob("*.parquet"))) == 0

    def test_flushes_when_threshold_reached(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            handler = PersistingHandler(output_dir=tmpdir, flush_threshold=3)
            handler(_make_event("run-001"))
            handler(_make_event("run-001"))
            handler(_make_event("run-001"))
            # Threshold reached — should have written one file
            files = list(Path(tmpdir).glob("*.parquet"))
            assert len(files) == 1

    def test_explicit_flush_writes_buffered_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            handler = PersistingHandler(output_dir=tmpdir, flush_threshold=100)
            handler(_make_event("run-002"))
            handler(_make_event("run-002"))
            handler.flush()
            files = list(Path(tmpdir).glob("*.parquet"))
            assert len(files) == 1

    def test_flush_clears_buffer(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            handler = PersistingHandler(output_dir=tmpdir, flush_threshold=100)
            handler(_make_event())
            handler.flush()
            handler.flush()  # second flush should be a no-op
            files = list(Path(tmpdir).glob("*.parquet"))
            assert len(files) == 1

    def test_registered_with_consumer_app(self) -> None:
        """PersistingHandler should work as a ConsumerApp handler."""
        from debian_observer.consumer_app import ConsumerApp

        with tempfile.TemporaryDirectory() as tmpdir:
            app = ConsumerApp()
            handler = PersistingHandler(output_dir=tmpdir, flush_threshold=2)
            app.register_handler("PackageVersionObserved", handler)

            app.process(_make_event("run-003"))
            app.process(_make_event("run-003"))

            files = list(Path(tmpdir).glob("*.parquet"))
            assert len(files) == 1
