"""Storage: Parquet writer.

Writes domain events and snapshots to Parquet files for efficient
analytical scans.

Design notes:
- One Parquet file per ingestion run (immutable, reproducible).
- Nested payloads are flattened for columnar storage.
- One directory per event type, one file per run; the incremental
  row-group writer bounds peak memory by the row group, not by the
  run (ADR-015: eventization must not materialize).
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
# Schema definition (PackageVersionObserved)
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
#: Default directory of the observation event log (the prosumers'
#: input): one directory per event type, one file per run. Every
#: --events-dir flag defaults here.
EVENTS_DIR_DEFAULT = "data/observed"

#: Incremental writers flush one row group per buffer, so peak
#: memory is bounded by the row group, not by the run.
ROW_GROUP_SIZE = 100_000


# ---------------------------------------------------------------------------
# Per-event-type event log (ADR-011 writer-owned serialization)
# ---------------------------------------------------------------------------

# One directory per event type (snake_case of the type name), one
# file per run: a read is a plain glob of one schema-homogeneous
# directory — never union_by_name.
# The `_DEFAULT_SCHEMA` IS the PackageVersionObserved schema;
# every other type registers its own (envelope columns + flattened
# payload columns).
_DEPENDENCY_OBSERVED_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("source", pa.string()),
        ("entity_id", pa.string()),
        ("observed_at", pa.string()),
        ("effective_at", pa.string()),
        ("ingestion_run_id", pa.string()),
        ("distribution", pa.string()),
        ("release", pa.string()),
        ("architecture", pa.string()),
        ("component", pa.string()),
        ("package", pa.string()),
        ("version", pa.string()),
        ("kind", pa.string()),
        ("clause_index", pa.int64()),
        ("condition", pa.string()),
        ("is_complex", pa.bool_()),
        ("alternatives", pa.int64()),
    ]
)

_POPCON_OBSERVED_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("source", pa.string()),
        ("entity_id", pa.string()),
        ("observed_at", pa.string()),
        ("effective_at", pa.string()),
        ("ingestion_run_id", pa.string()),
        ("package", pa.string()),
        ("insts", pa.int64()),
        ("vote", pa.int64()),
        ("olde", pa.int64()),
        ("recent", pa.int64()),
        ("nofiles", pa.int64()),
    ]
)

EVENT_TYPE_SCHEMAS: dict[str, pa.Schema] = {
    "PackageVersionObserved": _DEFAULT_SCHEMA,
    "DependencyObserved": _DEPENDENCY_OBSERVED_SCHEMA,
    "PopconObserved": _POPCON_OBSERVED_SCHEMA,
}


def _envelope_columns() -> list[tuple[str, pa.DataType]]:
    """The envelope columns every event row carries (§6.2 of R3)."""
    return [
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("source", pa.string()),
        ("entity_id", pa.string()),
        ("observed_at", pa.string()),
        ("effective_at", pa.string()),
        ("ingestion_run_id", pa.string()),
    ]


# The britney excuse event lake (R3, C2 — docs/release3 §10): every
# type registers envelope columns plus its own payload columns; list
# payload fields (blocked_by, migrate_after, reason, before/after of
# the set comparison) are JSON string columns (see
# _flatten_payload_row).
def _snapshot_columns() -> list[tuple[str, pa.DataType]]:
    """The pair identifiers every excuse payload carries."""
    return [
        ("snapshot", pa.string()),
        ("prev_snapshot", pa.string()),
    ]


def _excuse_state_columns() -> list[tuple[str, pa.DataType]]:
    """The compact ExcuseState as columns (§5.1).

    ``source_package`` is the state's source package: the envelope
    already owns a ``source`` column (the data source), so the state
    field takes a distinct column name (see
    :data:`_PAYLOAD_COLUMN_ALIASES`).
    """
    return [
        ("item_name", pa.string()),
        ("source_package", pa.string()),
        ("old_version", pa.string()),
        ("new_version", pa.string()),
        ("verdict", pa.string()),
        ("is_candidate", pa.bool_()),
        ("blocked_by", pa.string()),
        ("migrate_after", pa.string()),
        ("reason", pa.string()),
    ]


#: Parquet columns that read a differently-named payload field: the
#: envelope owns ``source`` (the data source), the excuse state owns
#: its source package (§5.1) — one column name each.
_PAYLOAD_COLUMN_ALIASES = {"source_package": "source"}


EVENT_TYPE_SCHEMAS.update(
    {
        "ExcuseObserved": pa.schema(
            _envelope_columns()
            + _snapshot_columns()
            + _excuse_state_columns()
        ),
        "ExcuseEntered": pa.schema(
            _envelope_columns()
            + _snapshot_columns()
            + _excuse_state_columns()
        ),
        "ExcuseLeft": pa.schema(
            _envelope_columns()
            + _snapshot_columns()
            + _excuse_state_columns()
        ),
        "ExcuseVerdictChanged": pa.schema(
            _envelope_columns()
            + _snapshot_columns()
            + [
                ("item_name", pa.string()),
                ("before", pa.string()),
                ("after", pa.string()),
            ]
        ),
        "ExcuseBlockedByChanged": pa.schema(
            _envelope_columns()
            + _snapshot_columns()
            + [
                ("item_name", pa.string()),
                ("before", pa.string()),
                ("after", pa.string()),
            ]
        ),
        "ExcuseMigrateAfterChanged": pa.schema(
            _envelope_columns()
            + _snapshot_columns()
            + [
                ("item_name", pa.string()),
                ("before", pa.string()),
                ("after", pa.string()),
            ]
        ),
        "ExcuseVersionChanged": pa.schema(
            _envelope_columns()
            + _snapshot_columns()
            + [
                ("item_name", pa.string()),
                ("before", pa.string()),
                ("after", pa.string()),
            ]
        ),
        "SnapshotObserved": pa.schema(
            _envelope_columns()
            + [
                ("snapshot", pa.string()),
                ("generated_date", pa.string()),
                ("prev_snapshot", pa.string()),
                ("elapsed", pa.int64()),
                ("gap_before", pa.bool_()),
                ("item_count", pa.int64()),
            ]
        ),
    }
)


def _flatten_payload_row(event: EventEnvelope, schema: pa.Schema) -> dict:
    """Flatten any event into a row over its registered schema.

    Envelope columns first; payload fields fill the remaining
    schema columns by name. Payloads loaded as JSON strings are
    parsed first. List and dict payload fields (britney's
    ``blocked_by``, ``reason`` — lists by §5.1) flatten as JSON
    strings: the column stays one primitive value and the fold
    reads it back with one ``json.loads``.
    """
    payload = event.payload
    if isinstance(payload, str):
        payload = json.loads(payload)
    elif not isinstance(payload, dict):
        payload = {}
    row: dict[str, Any] = {
        "event_id": str(event.event_id),
        "event_type": event.event_type,
        "source": event.source,
        "entity_id": event.entity_id,
        "observed_at": event.observed_at.isoformat(),
        "effective_at": event.effective_at.isoformat(),
        "ingestion_run_id": event.ingestion_run_id or "",
    }
    for name in schema.names:
        if name in row:
            continue
        value = payload.get(_PAYLOAD_COLUMN_ALIASES.get(name, name))
        if isinstance(value, (list, dict)):
            value = json.dumps(value, ensure_ascii=False)
        elif name in ("clause_index", "alternatives") and value is None:
            value = 0
        elif name in ("insts", "vote", "olde", "recent", "nofiles"):
            if value is None:
                value = 0
        row[name] = value
    return row


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
    memory stays flat in the number of events (ADR-015).
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

        writer = ParquetWriter(output_dir="data/observed")
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
            schema: Optional PyArrow schema. Uses the package-versions schema if not provided.
            row_group_size: Rows per row group for the incremental
                writer.
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.schema = schema or _DEFAULT_SCHEMA
        self.row_group_size = row_group_size

    def _event_to_row(self, event: EventEnvelope) -> dict[str, Any]:
        """Flatten an EventEnvelope into a Parquet-compatible row dict.

        The row shape follows the schema registered for the event
        type: every type but PackageVersionObserved flattens its
        own payload fields (its schema), which overlaps the
        original mapping for the rest.
        """
        if event.event_type in EVENT_TYPE_SCHEMAS and (
            event.event_type != "PackageVersionObserved"
        ):
            return _flatten_payload_row(
                event, EVENT_TYPE_SCHEMAS[event.event_type]
            )
        return self._event_to_row_package_version(event)

    def _event_to_row_package_version(
        self, event: EventEnvelope
    ) -> dict[str, Any]:
        """The PackageVersionObserved row mapping."""
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
