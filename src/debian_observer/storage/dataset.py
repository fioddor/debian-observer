"""Storage: normalized package_versions dataset writer.

Projects ``PackageVersionObserved`` events into the normalized dataset
referenced by the spec (§10.2) and ADR-003:
``data/normalized/package_versions/*.parquet``.

ADR-011 assigns all Parquet serialization to the storage layer; this
writer owns the dataset schema, which is pinned to
``PackageVersion.model_fields`` by tests.
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
# Schema definition (R1 — PackageVersion projection)
# ---------------------------------------------------------------------------

# Writer-owned schema (ADR-011). One column per PackageVersion domain
# field; observed_at and source come from the EventEnvelope.
_DEFAULT_SCHEMA = pa.schema(
    [
        ("package_id", pa.string()),
        ("source_package", pa.string()),
        ("version", pa.string()),
        ("distribution", pa.string()),
        ("release", pa.string()),
        ("suite", pa.string()),
        ("architecture", pa.string()),
        ("component", pa.string()),
        ("section", pa.string()),
        ("priority", pa.string()),
        ("observed_at", pa.string()),
        ("source", pa.string()),
    ]
)


class PackageVersionDatasetWriter:
    """Writes the normalized package_versions dataset from events.

    The dataset is the analytical state projection of the event log:
    one row per observed package version, without event provenance
    columns. One Parquet file per ingestion run (immutable,
    append-only).
    """

    def __init__(
        self,
        output_dir: str | Path,
        schema: pa.Schema | None = None,
    ) -> None:
        """Initialize the writer.

        Args:
            output_dir: Dataset directory, e.g.
                ``data/normalized/package_versions``.
            schema: Optional PyArrow schema. Uses the R1 default if not
                provided.
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.schema = schema or _DEFAULT_SCHEMA

    def _event_to_row(self, event: EventEnvelope) -> dict[str, Any]:
        """Flatten a PackageVersionObserved envelope into a dataset row."""
        payload = event.payload
        if isinstance(payload, str):
            payload = json.loads(payload)
        elif not isinstance(payload, dict):
            payload = {}

        return {
            "package_id": payload.get("package_id", ""),
            "source_package": payload.get("source_package", ""),
            "version": payload.get("version", ""),
            "distribution": payload.get("distribution", ""),
            "release": payload.get("release", ""),
            "suite": payload.get("suite", ""),
            "architecture": payload.get("architecture", ""),
            "component": payload.get("component", ""),
            "section": payload.get("section", ""),
            "priority": payload.get("priority", ""),
            "observed_at": event.observed_at.isoformat(),
            "source": event.source,
        }

    def write_events(
        self,
        events: list[EventEnvelope],
        ingestion_run_id: str | None = None,
    ) -> Path:
        """Project a batch of events into a dataset Parquet file.

        Only ``PackageVersionObserved`` events are projected; other
        event types are skipped (they belong to other datasets).

        Args:
            events: List of EventEnvelope instances.
            ingestion_run_id: Optional run id for the filename. Uses a
                timestamped UUID if not provided.

        Returns:
            Path to the written file.

        Raises:
            ValueError: If the batch projects to zero rows.
        """
        rows = [
            self._event_to_row(event)
            for event in events
            if event.event_type == "PackageVersionObserved"
        ]
        if not rows:
            raise ValueError("Cannot write empty dataset projection to Parquet")

        run_id = (
            ingestion_run_id
            or f"run-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        )
        filename = f"{run_id}.parquet"
        file_path = self.output_dir / filename

        table = pa.Table.from_pylist(rows, schema=self.schema)
        pq.write_table(table, file_path)

        return file_path

    def list_files(self) -> list[Path]:
        """List all Parquet files in the dataset directory."""
        return sorted(self.output_dir.glob("*.parquet"))
