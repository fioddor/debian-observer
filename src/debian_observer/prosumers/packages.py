"""Core consolidation logic: fold PackageVersionObserved events.

This module bridges the old ``debian_observer.consumer.packages``
design with the new ``prosumers`` I/O contract.  All business logic
lives here; the CLI scripts in ``consolidate.py`` and ``filter.py``
are thin wrappers around ``prosumers.stream``.

Design notes for researchers
----------------------------
* The fold key (PK) is the **full observation coordinates**:
  ``(package_id, version, distribution, release, suite, architecture)``.
* ``last-observed-wins`` uses ``(observed_at, event_id)`` as the
  ordering — ties are practically impossible because ``event_id`` is
  a UUID, but the tuple makes the ordering total and deterministic.
* ``observed_first_at`` / ``observed_last_at`` are ``min`` / ``max``
  of ``observed_at`` inside each PK group.
* Duplicate ``event_id`` values collapse even if they appear in
  different files (same event re-delivered or re-logged).
* The output schema is **writer-owned** (13 string columns).  Missing
  fields from legacy event logs become ``""``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

#: Output schema (13 string columns).
CONSOLIDATED_COLUMNS: tuple[str, ...] = (
    "package_id",
    "source_package",
    "version",
    "distribution",
    "release",
    "suite",
    "architecture",
    "component",
    "section",
    "priority",
    "observed_first_at",
    "observed_last_at",
    "source",
)

#: Fields that form the observation identity (fold key).
CONSOLIDATION_PK: tuple[str, ...] = (
    "package_id",
    "version",
    "distribution",
    "release",
    "suite",
    "architecture",
)

#: Attribute columns resolved by last-observed-wins.
_LAST_WINS_COLUMNS: tuple[str, ...] = (
    "source_package",
    "component",
    "section",
    "priority",
    "source",
)

_OUTPUT_FILENAME = "package_versions.parquet"

_CONSOLIDATED_SCHEMA = pa.schema(
    [(name, pa.string()) for name in CONSOLIDATED_COLUMNS]
)


class ConsolidateError(RuntimeError):
    """Fold/consolidation problems; carries a clear operator-facing message."""


@dataclass(frozen=True)
class FoldStats:
    """Counts surfaced by the CLI."""

    events_read: int
    pvo_events: int
    rows: int


def _s(value: Any) -> str:
    """Coerce a read-back value to the string schema (None → "")."""
    if value is None:
        return ""
    return str(value)


def _record_to_row(record: Any) -> dict[str, Any]:
    """Convert a ``prosumers.stream.Record`` into the flat dict layout
    the fold engine expects (envelope + payload merged).
    """
    row = dict(record.payload)
    row.update(record.meta)
    return row


def fold_package_versions(
    records: Iterable[Any],
) -> tuple[list[dict[str, str]], FoldStats]:
    """Fold an iterable of ``Record`` objects into consolidated rows.

    Returns the rows (sorted by PK for deterministic output) and the
    read statistics.

    Raises:
        ConsolidateError: If no ``PackageVersionObserved`` events are found.
    """
    # Flatten envelope + payload so the existing logic can operate on
    # plain dicts (unchanged from the old implementation).
    log_rows = [_record_to_row(r) for r in records]

    # Dedup by event_id (same event delivered twice = one observation).
    seen: set[str] = set()
    pvo_rows: list[dict[str, Any]] = []
    for row in log_rows:
        if row.get("event_type") != "PackageVersionObserved":
            continue
        event_id = _s(row.get("event_id"))
        if event_id in seen:
            continue
        seen.add(event_id)
        pvo_rows.append(row)

    if not pvo_rows:
        raise ConsolidateError("No PackageVersionObserved events found.")

    # Deterministic order: (observed_at, event_id).
    pvo_rows.sort(key=lambda r: (_s(r.get("observed_at")), _s(r.get("event_id"))))

    groups: dict[tuple[str, ...], dict[str, str]] = {}
    for row in pvo_rows:
        pk = tuple(_s(row.get(name)) for name in CONSOLIDATION_PK)
        group = groups.get(pk)
        if group is None:
            group = {
                name: _s(row.get(name)) for name in CONSOLIDATION_PK
            }
            group.update({name: _s(row.get(name)) for name in _LAST_WINS_COLUMNS})
            group["observed_first_at"] = _s(row.get("observed_at"))
            group["observed_last_at"] = _s(row.get("observed_at"))
            groups[pk] = group
        else:
            group.update({name: _s(row.get(name)) for name in _LAST_WINS_COLUMNS})
            group["observed_last_at"] = _s(row.get("observed_at"))

    rows = [groups[pk] for pk in sorted(groups)]
    stats = FoldStats(
        events_read=len(log_rows),
        pvo_events=len(pvo_rows),
        rows=len(rows),
    )
    return rows, stats


def write_consolidated_table(
    rows: list[dict[str, str]],
    *,
    dataset_dir: str | Path,
) -> Path:
    """Write consolidated rows as ONE atomic Parquet file.

    The fixed name ``package_versions.parquet`` lives inside
    *dataset_dir*.  Uses tmp+``os.replace`` so concurrent readers
    never see a partial file.
    """
    dataset_dir = Path(dataset_dir)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    output_path = dataset_dir / _OUTPUT_FILENAME
    tmp_path = dataset_dir / f".{_OUTPUT_FILENAME}.tmp"

    table = pa.Table.from_pylist(rows, schema=_CONSOLIDATED_SCHEMA)
    try:
        pq.write_table(table, tmp_path)
        os.replace(tmp_path, output_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    return output_path
