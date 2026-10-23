"""Storage: consolidation — the fold behind the packages table.

The consolidation fold (moved from ``consumer/packages.py`` on
2026-10-23: the fold IS a writer of the consolidated table, so it
lives in the storage layer, writer-owned ADR-011 — "consumer" now
names the queue consumers over NATS, see ``consumer_app`` and
``messaging``). Two feeders share one semantics (spec §15.1):

* **Batch feeder** — the ``consolidate`` CLI folds the whole
  append-only event log (one Parquet file per ingestion run,
  ADR-003 amendment) and rewrites the table: pure regeneration,
  ``merge_existing=False``.
* **Streaming feeder** — the external consumer recipe
  (``examples/consume_packages.py``) feeds one observation at a time
  and rewrites the durable table per message: read-modify-write of
  the existing table (``merge_existing=True``, the default) +
  atomic rename, so a redelivered or cross-run observation merges
  into the durable table instead of being lost.

Both produce the identical table: ``fold(log parquet) ==
fold(stream)`` — the invariant the crash test pins.

The fold semantics are unchanged from the original consumer fold:
one row per full observation PK ``(package_id, version,
distribution, release, suite, architecture)``; attribute columns
last-observed-wins (ordered by ``(observed_at, event_id)`` —
ties broken by event id, the same total order the batch fold sorts
by); ``observed_first_at``/``observed_last_at`` as the min/max
``observed_at`` of the group; dedup by ``event_id``; idempotent by
construction (§15.1: A×3 == A). The output is ONE re-generable file
with a fixed name (``package_versions.parquet``), written atomically
(tmp file + ``os.replace``) so a concurrent reader (DuckDB/API)
never sees a partial file.

Reading follows ``storage.analytical``: the sorted ``*.parquet``
glob of the events dir, loaded through DuckDB with
``union_by_name=true`` (legacy event-log layouts load missing fields
as ``None``).

Usage::

    # batch (the consolidate CLI):
    build_consolidated_packages(
        Path("data/parquet"), dataset_dir=Path("data/normalized/package_versions")
    )

    # streaming (the external consumer recipe):
    merge = ConsolidationMerge()
    merge.feed(row_from_envelope(envelope))
    merge.write(dataset_dir)  # durable write BEFORE the ack
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from debian_observer.events.envelope import EventEnvelope

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

#: Payload columns carried on a flattened observation row (the same
#: keys ``storage.parquet`` writes into the event log, ADR-011).
_PAYLOAD_KEYS: tuple[str, ...] = (
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
)

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


def read_event_log(events_dir: str | Path) -> tuple[list[dict[str, Any]], int]:
    """Read every ``*.parquet`` event-log file (sorted glob,
    ``union_by_name`` — the ``storage.analytical`` reading mechanism).

    Returns one dict per event row plus the number of files read.
    Both feeders read the log this way: the batch feeder folds the
    rows, the publish CLI re-serializes them into envelopes.
    """
    events_dir = Path(events_dir)
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


def row_from_envelope(event: EventEnvelope) -> dict[str, Any]:
    """Flatten an :class:`EventEnvelope` into the observation-row
    shape the merge feeds on — the stream-side twin of a log row
    (the same flattened columns ``storage.parquet`` writes,
    ADR-011). ``observed_at`` becomes the ISO string the fold
    orders by, so a streamed observation compares equal to the same
    observation read back from the log.
    """
    payload = event.payload
    if not isinstance(payload, dict):
        payload = {}
    observed_at = event.observed_at
    if hasattr(observed_at, "isoformat"):
        observed_at = observed_at.isoformat()
    effective_at = event.effective_at
    if hasattr(effective_at, "isoformat"):
        effective_at = effective_at.isoformat()
    row: dict[str, Any] = {
        "event_id": str(event.event_id),
        "event_type": event.event_type,
        "source": event.source,
        "entity_id": event.entity_id,
        "observed_at": observed_at,
        "effective_at": effective_at,
        "ingestion_run_id": event.ingestion_run_id or "",
    }
    row.update({key: payload.get(key, "") for key in _PAYLOAD_KEYS})
    return row


class ConsolidationMerge:
    """Incremental fold state over ``PackageVersionObserved``.

    One semantics, two feeders (spec §15.1): feed whole-log rows
    (batch) or one streamed observation at a time; ``write`` always
    emits the consolidated table atomically. Duplicate ``event_id``
    entries collapse; min/max ``observed_at`` absorb duplicate PK
    observations in any order; last-observed-wins resolves by the
    ``(observed_at, event_id)`` total order.
    """

    def __init__(self) -> None:
        self._groups: dict[tuple[str, ...], dict[str, str]] = {}
        self._last: dict[tuple[str, ...], tuple[str, str]] = {}
        self._seen: set[str] = set()
        #: Rows offered to ``feed`` so far (this instance's session).
        self.events_read = 0
        #: PVO observations absorbed (post-dedup) so far.
        self.pvo_events = 0

    def feed(self, observation: Mapping[str, Any]) -> bool:
        """Absorb one flattened observation row — a log row (from
        ``read_event_log``) or an envelope-derived row (from
        ``row_from_envelope``).

        Returns:
            ``True`` if a ``PackageVersionObserved`` observation was
            absorbed; ``False`` when the row is another event type or
            a duplicate ``event_id``.
        """
        self.events_read += 1
        if observation.get("event_type") != "PackageVersionObserved":
            return False
        event_id = _s(observation.get("event_id"))
        if event_id in self._seen:
            return False
        self._seen.add(event_id)
        self.pvo_events += 1
        self._absorb(observation, event_id=event_id)
        return True

    def _absorb(self, observation: Mapping[str, Any], *, event_id: str) -> None:
        """Fold one PVO observation into the group state (no dedup —
        used directly for prior-table rows during a
        ``merge_existing`` write, which carry no event id)."""
        pk = tuple(_s(observation.get(name)) for name in CONSOLIDATION_PK)
        observed_at = _s(observation.get("observed_at"))
        group = self._groups.get(pk)
        if group is None:
            group = {
                name: _s(observation.get(name)) for name in CONSOLIDATION_PK
            }
            group.update(
                {name: _s(observation.get(name)) for name in _LAST_WINS_COLUMNS}
            )
            # min/max observed_at absorb duplicates: initialize with
            # this first (in-feed-order) observation.
            group["observed_first_at"] = observed_at
            group["observed_last_at"] = observed_at
            self._groups[pk] = group
            self._last[pk] = (observed_at, event_id)
            return
        last_at, last_id = self._last[pk]
        # Last-observed-wins, ordered by (observed_at, event_id) —
        # the same total order the batch fold sorts by, so feed
        # order does not matter.
        if (observed_at, event_id) >= (last_at, last_id):
            group.update(
                {name: _s(observation.get(name)) for name in _LAST_WINS_COLUMNS}
            )
            group["observed_last_at"] = observed_at
            self._last[pk] = (observed_at, event_id)
        # min/max absorb duplicates regardless of feed order.
        group["observed_first_at"] = min(group["observed_first_at"], observed_at)
        group["observed_last_at"] = max(group["observed_last_at"], observed_at)

    def rows(self) -> list[dict[str, str]]:
        """The consolidated rows so far, sorted by PK (deterministic)."""
        return [self._groups[pk] for pk in sorted(self._groups)]

    def write(
        self,
        dataset_dir: str | Path,
        *,
        merge_existing: bool = True,
    ) -> Path:
        """Write the consolidated table — atomic tmp + rename.

        With ``merge_existing=True`` (the streaming feeder) an
        already-written table is read back and folded into this
        instance's state first: every event merges into the durable
        table (RMW), so redelivered or cross-run observations
        accumulate instead of being lost. The batch feeder passes
        ``merge_existing=False`` — regenerate, never accumulate:
        the table is always exactly the fold of the given log.

        Note: a prior-table row carries no ``event_id`` (the table
        schema has none), so an exact ``(observed_at, event_id)``
        tie against a fed observation cannot be resolved the way the
        batch fold resolves it; the fed observation wins. The
        equality the invariant pins (``fold(log parquet) ==
        fold(stream)``) is unaffected: both paths carry the same
        event ids.
        """
        dataset_dir = Path(dataset_dir)
        output_path = dataset_dir / OUTPUT_FILENAME
        if merge_existing and output_path.exists():
            table = pq.read_table(output_path, schema=_CONSOLIDATED_SCHEMA)
            for i, row in enumerate(table.to_pylist()):
                observation = dict(row)
                # A prior row's fold-relevant timestamp is its last
                # observed_at (the max the group had absorbed).
                observation.setdefault(
                    "observed_at", row.get("observed_last_at")
                )
                self._absorb(observation, event_id=f"prior-table-row:{i}")
        return write_consolidated_table(self.rows(), dataset_dir=dataset_dir)


def fold_package_versions(
    events_dir: str | Path,
) -> tuple[list[dict[str, str]], FoldStats]:
    """Fold the event log into consolidated ``package_versions`` rows
    (the batch feeder: read → fold → the caller writes).

    Returns the rows (sorted by PK for deterministic output) and the
    read statistics.

    Raises:
        ConsolidateError: If the events dir holds no Parquet files, or
            the log carries no ``PackageVersionObserved`` events.
    """
    events_dir = Path(events_dir)
    log_rows, files_read = read_event_log(events_dir)

    merge = ConsolidationMerge()
    for row in log_rows:
        merge.feed(row)

    if merge.pvo_events == 0:
        raise ConsolidateError(
            f"No PackageVersionObserved events found in event log {events_dir}"
        )

    rows = merge.rows()
    stats = FoldStats(
        files_read=files_read,
        events_read=merge.events_read,
        pvo_events=merge.pvo_events,
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

    The full batch consumer: read → fold → atomic single-file write.
    Returns the output path (``dataset_dir / package_versions.parquet``).
    """
    rows, _ = fold_package_versions(events_dir)
    return write_consolidated_table(rows, dataset_dir=dataset_dir)