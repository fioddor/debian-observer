"""Consumer: consolidate the packages table from the event log.

The local consumer (R1): the "message queue" is the append-only event
log on disk — one Parquet file per ingestion run (ADR-003 amendment,
2026-10-23). This fold reads the whole log, keeps the
``PackageVersionObserved`` events and consolidates them into the
``package_versions`` table: one row per full observation PK
``(package_id, version, distribution, release, suite, architecture)``,
attribute columns last-observed-wins, ``observed_first_at`` /
``observed_last_at`` as the min/max ``observed_at`` of the group.

The fold is idempotent by construction (spec §15.1): duplicate
``event_id`` entries collapse, min/max absorb duplicated PK
observations, and folding the same log twice yields the same table.
The output is ONE re-generable file with a fixed name
(``package_versions.parquet``), written atomically (tmp file +
``os.replace``) so a concurrent reader (DuckDB/API) never sees a
partial file.

Reading follows ``storage.analytical``: the sorted ``*.parquet`` glob
of the events dir, loaded through DuckDB with ``union_by_name=true``
(legacy event-log layouts load missing fields as ``None``).

Usage::

    build_consolidated_packages(
        Path("data/parquet"), dataset_dir=Path("data/normalized/package_versions")
    )
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

#: Output schema (13 columns), writer-owned (ADR-011 pattern): all
#: string columns, matching the storage layer's event-log style.
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

#: The consolidation PK: the full coordinates of the observation.
CONSOLIDATION_PK: tuple[str, ...] = (
    "package_id",
    "version",
    "distribution",
    "release",
    "suite",
    "architecture",
)

#: Attribute columns resolved last-observed-wins.
_LAST_WINS_COLUMNS: tuple[str, ...] = (
    "source_package",
    "component",
    "section",
    "priority",
    "source",
)

#: Envelope columns the fold reads from each event-log row.
_EVENT_COLUMNS: tuple[str, ...] = (
    "event_id",
    "event_type",
    "observed_at",
    *CONSOLIDATION_PK,
    *_LAST_WINS_COLUMNS,
)

#: The fixed output file name inside ``--dataset-dir``.
OUTPUT_FILENAME = "package_versions.parquet"

_CONSOLIDATED_SCHEMA = pa.schema(
    [(name, pa.string()) for name in CONSOLIDATED_COLUMNS]
)


class ConsolidateError(RuntimeError):
    """Raised for fold/consolidation problems; always carries a clear,
    operator-facing message — never a bare traceback."""


@dataclass(frozen=True)
class FoldStats:
    """Counts surfaced by the CLI echo."""

    files_read: int
    events_read: int
    pvo_events: int
    rows: int


def _s(value: Any) -> str:
    """Coerce a read-back log value to the string schema (None → "")."""
    if value is None:
        return ""
    return str(value)


def _read_log_rows(events_dir: Path) -> tuple[list[dict[str, Any]], int]:
    """Read every ``*.parquet`` event-log file (sorted glob,
    ``union_by_name`` — the ``storage.analytical`` reading mechanism).

    Returns one dict per event row plus the number of files read.
    """
    files = sorted(events_dir.glob("*.parquet"))
    if not files:
        raise ConsolidateError(
            f"No Parquet event files found in events dir {events_dir}"
        )

    con = duckdb.connect(":memory:")
    file_list = ", ".join(f"'{f}'" for f in files)
    con.execute(
        f"CREATE VIEW log AS SELECT * FROM "
        f"read_parquet([{file_list}], union_by_name=true)"
    )
    cursor = con.execute("SELECT * FROM log")
    column_names = [d[0] for d in cursor.description]
    rows = [dict(zip(column_names, row)) for row in cursor.fetchall()]
    return rows, len(files)


def fold_package_versions(
    events_dir: str | Path,
) -> tuple[list[dict[str, str]], FoldStats]:
    """Fold the event log into consolidated ``package_versions`` rows.

    Returns the rows (sorted by PK for deterministic output) and the
    read statistics.

    Raises:
        ConsolidateError: If the events dir holds no Parquet files, or
            the log carries no ``PackageVersionObserved`` events.
    """
    events_dir = Path(events_dir)
    log_rows, files_read = _read_log_rows(events_dir)

    # Dedup by event_id within the fold (same event in two files is
    # one observation); file order is the stable tiebreak.
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
        raise ConsolidateError(
            f"No PackageVersionObserved events found in event log {events_dir}"
        )

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
            group.update(
                {name: _s(row.get(name)) for name in _LAST_WINS_COLUMNS}
            )
            # min/max observed_at absorb duplicates: initialize with
            # this first (earliest-sorted) observation.
            group["observed_first_at"] = _s(row.get("observed_at"))
            group["observed_last_at"] = _s(row.get("observed_at"))
            groups[pk] = group
        else:
            # Last-observed-wins for the attribute columns...
            group.update(
                {name: _s(row.get(name)) for name in _LAST_WINS_COLUMNS}
            )
            # ...and max observed_at: the sort guarantees this row is
            # the latest so far in its PK group.
            group["observed_last_at"] = _s(row.get("observed_at"))

    rows = [groups[pk] for pk in sorted(groups)]
    stats = FoldStats(
        files_read=files_read,
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
    """Write the consolidated rows as ONE file — the fixed name
    ``package_versions.parquet`` — atomically: a tmp file in the same
    dir, ``os.replace`` at the end, tmp cleaned on failure.
    """
    dataset_dir = Path(dataset_dir)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    output_path = dataset_dir / OUTPUT_FILENAME
    tmp_path = dataset_dir / f".{OUTPUT_FILENAME}.tmp"

    table = pa.Table.from_pylist(rows, schema=_CONSOLIDATED_SCHEMA)
    try:
        pq.write_table(table, tmp_path)
        os.replace(tmp_path, output_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    return output_path


def build_consolidated_packages(
    events_dir: str | Path,
    *,
    dataset_dir: str | Path,
) -> Path:
    """Fold the event log and write the consolidated table.

    The full consumer: read → fold → atomic single-file write. Returns
    the output path (``dataset_dir / package_versions.parquet``).
    """
    rows, _ = fold_package_versions(events_dir)
    return write_consolidated_table(rows, dataset_dir=dataset_dir)