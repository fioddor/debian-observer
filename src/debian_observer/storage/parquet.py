"""Storage: Parquet writer.

Writes domain events and snapshots to Parquet files for efficient
analytical scans.

Design notes:
- One Parquet file per ingestion run (immutable, reproducible).
- Nested payloads are flattened for columnar storage.
- The per-event-type event log (one directory per event type,
  one file per run) and the incremental row-group writer (ADR-015:
  eventization must not materialize; peak memory bounded by the row
  group, not by the run).
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from debian_observer.events import EventEnvelope

# ---------------------------------------------------------------------------
# Schema definition (PackageVersionObserved — the core event type)
# ---------------------------------------------------------------------------

# Writer-owned schema (ADR-011): the event file layout lives
# here, in the storage layer; domain and event models do not
# serialize.
# suite is appended last: files written before the column existed
# (13- and 16-column layouts) still load positionally.
_DEFAULT_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("source", pa.string()),
        ("entity_id", pa.string()),
        ("observed_at", pa.string()),
        ("effective_at", pa.string()),
        ("ingestion_run_id", pa.string()),
        ("package_id", pa.string()),
        ("source_package", pa.string()),
        ("version", pa.string()),
        ("distribution", pa.string()),
        ("release", pa.string()),
        ("architecture", pa.string()),
        ("component", pa.string()),
        ("section", pa.string()),
        ("priority", pa.string()),
        ("suite", pa.string()),
    ]
)

#: Rows per Parquet row group (spec §9.2: a fixed size, e.g. 100 k).
#: Incremental writers flush one row group per buffer, so peak
#: memory is bounded by the row group, not by the run.
ROW_GROUP_SIZE = 100_000


# ---------------------------------------------------------------------------
# Per-event-type event log (spec §9.1, ADR-011 writer-owned serialization)
# ---------------------------------------------------------------------------

# One directory per event type (snake_case of the type name), one
# file per run: a read is a plain glob of one schema-homogeneous
# directory — never union_by_name (mandatory contract, spec §9.1).
# `_DEFAULT_SCHEMA` is the PackageVersionObserved schema.
EVENT_TYPE_SCHEMAS: dict[str, pa.Schema] = {
    "PackageVersionObserved": _DEFAULT_SCHEMA,
}


def event_type_directory(event_type: str) -> str:
    """The on-disk directory name of an event type (snake_case)."""
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", event_type).lower()
    return snake


def schema_for_event_type(event_type: str) -> pa.Schema:
    """The writer-owned Parquet schema of one event type."""
    if event_type not in EVENT_TYPE_SCHEMAS:
        known = ", ".join(sorted(EVENT_TYPE_SCHEMAS))
        raise ValueError(
            f"no Parquet schema registered for event type {event_type!r}; "
            f"registered types: {known}"
        )
    return EVENT_TYPE_SCHEMAS[event_type]


def writer_for_event_type(
    event_type: str, *, events_dir: str | Path
) -> "ParquetWriter":
    """A ParquetWriter for one event type, in its own directory.

    Layout: ``<events_dir>/<event_type_snake>/<run_id>.parquet``.
    """
    return ParquetWriter(
        output_dir=Path(events_dir) / event_type_directory(event_type),
        schema=schema_for_event_type(event_type),
    )


class IncrementalParquetWriter:
    """Row-group-incremental Parquet writer (bounds writer memory).

    Buffers events, flushes a Parquet row group whenever the buffer
    reaches ``row_group_size``, finalizes the file on close. Peak
    memory stays flat in the number of events (ADR-015): the full
    ``packages`` table (1,497,724 events) measured 4.9 GB peak RSS
    when the event list was materialized, and flat when streamed
    (2026-11-08, E1-preview on ghostell).
    """

    def __init__(
        self,
        file_path: str | Path,
        schema: pa.Schema,
        row_group_size: int = ROW_GROUP_SIZE,
    ) -> None:
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.schema = schema
        self.row_group_size = row_group_size
        self._buffer: list[dict[str, Any]] = []
        self._writer: pq.ParquetWriter | None = None
        self._closed = False
        self.rows_written = 0

    def append_event(self, event: EventEnvelope) -> None:
        """Buffer one event, flushing a row group per buffer full."""
        if self._closed:
            raise ValueError("writer is closed")
        self._buffer.append(self._event_to_row(event))
        if len(self._buffer) >= self.row_group_size:
            self._flush()

    def append_events(self, events) -> None:
        """Buffer many events (same per-event path)."""
        for event in events:
            self.append_event(event)

    # The row mapping is the writer's (ADR-011): same flatten as
    # ParquetWriter, one source of truth.
    _event_to_row = None  # set below (ParquetWriter defines it)

    def _flush(self) -> None:
        if not self._buffer:
            return
        table = pa.Table.from_pylist(self._buffer, schema=self.schema)
        if self._writer is None:
            self._writer = pq.ParquetWriter(self.file_path, self.schema)
        self._writer.write_table(table)
        self.rows_written += len(self._buffer)
        self._buffer = []

    def close(self) -> Path:
        """Flush the tail row group and finalize the file."""
        if self._closed:
            return self.file_path
        self._flush()
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        self._closed = True
        return self.file_path

    def __enter__(self) -> "IncrementalParquetWriter":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class ParquetWriter:
    """Writes EventEnvelope batches to Parquet files.

    Usage::

        writer = ParquetWriter(output_dir="data/events")
        writer.write_events(events, ingestion_run_id="run-001")

        # Or stream incrementally (bounded memory, ADR-015):
        with writer.open_parquet_file("run-001") as inc:
            for event in events:
                inc.append_event(event)
    """

    def __init__(
        self,
        output_dir: str | Path,
        schema: pa.Schema | None = None,
        row_group_size: int = ROW_GROUP_SIZE,
    ) -> None:
        """Initialize the writer.

        Args:
            output_dir: Directory where Parquet files will be stored.
            schema: Optional PyArrow schema. Uses the module's default schema if not provided.
            row_group_size: Rows per row group for the incremental
                writer (spec §9.2: a fixed size, e.g. 100 k).
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.schema = schema or _DEFAULT_SCHEMA
        self.row_group_size = row_group_size

    def _event_to_row(self, event: EventEnvelope) -> dict[str, Any]:
        """Flatten an EventEnvelope into a Parquet-compatible row dict."""
        payload = event.payload
        if isinstance(payload, str):
            payload = json.loads(payload)
        elif not isinstance(payload, dict):
            payload = {}

        return {
            "event_id": str(event.event_id),
            "event_type": event.event_type,
            "source": event.source,
            "entity_id": event.entity_id,
            "observed_at": event.observed_at.isoformat(),
            "effective_at": event.effective_at.isoformat(),
            "ingestion_run_id": event.ingestion_run_id or "",
            "package_id": payload.get("package_id", ""),
            "source_package": payload.get("source_package", ""),
            "version": payload.get("version", ""),
            "distribution": payload.get("distribution", ""),
            "release": payload.get("release", ""),
            "architecture": payload.get("architecture", ""),
            "component": payload.get("component", ""),
            "section": payload.get("section", ""),
            "priority": payload.get("priority", ""),
            "suite": payload.get("suite", ""),
        }

    def open_parquet_file(
        self, ingestion_run_id: str | None = None
    ) -> IncrementalParquetWriter:
        """Open an incremental writer for one run's Parquet file.

        The caller appends events one at a a time (or in small
        batches); row groups flush at ``row_group_size`` and the
        file finalizes on close — peak memory bounded by the row
        group, not by the run (ADR-015).
        """
        run_id = (
            ingestion_run_id
            or f"run-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        )
        writer = IncrementalParquetWriter(
            self.output_dir / f"{run_id}.parquet",
            self.schema,
            row_group_size=self.row_group_size,
        )
        # The row mapping is the writer's (ADR-011), one source:
        writer._event_to_row = self._event_to_row
        return writer

    def write_events(
        self,
        events: list[EventEnvelope],
        ingestion_run_id: str | None = None,
    ) -> Path:
        """Write a batch of events to a Parquet file.

        Args:
            events: List of EventEnvelope instances.
            ingestion_run_id: Optional run id for filename. Uses
                              a timestamped UUID if not provided.

        Returns:
            Path to the written file.
        """
        if not events:
            raise ValueError("Cannot write empty event list to Parquet")

        with self.open_parquet_file(ingestion_run_id) as writer:
            writer.append_events(events)
        return writer.file_path

    def list_files(self) -> list[Path]:
        """List all Parquet files in the output directory."""
        return sorted(self.output_dir.glob("*.parquet"))