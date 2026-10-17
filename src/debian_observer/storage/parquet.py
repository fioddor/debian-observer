"""Storage: Parquet writer.

Writes domain events and snapshots to Parquet files for efficient
analytical scans.

Design notes:
- One Parquet file per ingestion run (immutable, reproducible).
- Nested payloads are flattened for columnar storage.
- R1 writes PackageVersionObserved only; future releases extend schema.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from debian_observer.events import EventEnvelope

# ---------------------------------------------------------------------------
# Schema definition (R1 — PackageVersionObserved)
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


class ParquetWriter:
    """Writes EventEnvelope batches to Parquet files.

    Usage::

        writer = ParquetWriter(output_dir="data/parquet")
        writer.write_events(events, ingestion_run_id="run-001")

        # Or append to an existing dataset
        writer.write_events(events)
    """

    def __init__(
        self,
        output_dir: str | Path,
        schema: pa.Schema | None = None,
    ) -> None:
        """Initialize the writer.

        Args:
            output_dir: Directory where Parquet files will be stored.
            schema: Optional PyArrow schema. Uses R1 default if not provided.
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.schema = schema or _DEFAULT_SCHEMA

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

        run_id = (
            ingestion_run_id
            or f"run-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        )
        filename = f"{run_id}.parquet"
        file_path = self.output_dir / filename

        rows = [self._event_to_row(event) for event in events]
        table = pa.Table.from_pylist(rows, schema=self.schema)
        pq.write_table(table, file_path)

        return file_path

    def list_files(self) -> list[Path]:
        """List all Parquet files in the output directory."""
        return sorted(self.output_dir.glob("*.parquet"))
