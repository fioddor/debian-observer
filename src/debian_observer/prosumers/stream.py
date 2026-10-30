"""Stream abstraction for prosumers.

Unify access to two transport formats:
- **File**: Parquet files on disk (the local/event-log format).
- **Stream**: NATS-like byte stream (placeholder: for now reads/writes
  newline-delimited JSON records over stdin/stdout).

Each prosumer decides which arguments it accepts (not all need all four).
The CLI helpers in ``prosumers.cli`` enforce the common flag names:
``--stream-in``, ``stream-out``, ``--file-in``, ``--file-out``.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Stream record: the universal envelope every prosumer sees.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Record:
    """One item travelling through the pipe.

    Fields:
        event_type: discriminates record kinds (e.g. PackageVersionObserved).
        payload:    the business payload as a flat dict.
        meta:       transport metadata (event_id, observed_at, …).
    """

    event_type: str
    payload: dict[str, Any]
    meta: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a plain dict (used by ND-JSON stream)."""
        return {
            "_event_type": self.event_type,
            "_meta": self.meta,
            **self.payload,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Record":
        """Rehydrate from a plain dict.

        The input dict is NOT mutated: the envelope keys (_event_type,
        _meta) are read from a shallow copy, so the caller keeps its
        dict intact (a library caller may rehydrate the same dict
        twice, or read it afterwards).
        """
        event_type = d.get("_event_type", "Unknown")
        meta = d.get("_meta", {})
        payload = {k: v for k, v in d.items() if k not in ("_event_type", "_meta")}
        return cls(event_type=event_type, payload=payload, meta=meta)


# ---------------------------------------------------------------------------
# Source / Sink abstractions
# ---------------------------------------------------------------------------


class Source:
    """Iterable of Records.  Context-manager for clean-up."""

    def __iter__(self) -> Iterator[Record]:
        raise NotImplementedError

    def __enter__(self) -> "Source":
        return self

    def __exit__(self, *exc: Any) -> None:
        pass


class Sink:
    """Accepts Records one at a time.  Context-manager for flush/close."""

    def emit(self, record: Record) -> None:
        raise NotImplementedError

    def __enter__(self) -> "Sink":
        return self

    def __exit__(self, *exc: Any) -> None:
        pass


# ---------------------------------------------------------------------------
# File implementations (Parquet)
# ---------------------------------------------------------------------------


class FileSource(Source):
    """Read a Parquet file as a sequence of Records.

    Column layout expected (ADR-011 event-log style):
        event_id, event_type, observed_at, package_id, …

    Every row becomes a Record where ``meta`` holds envelope columns
    and ``payload`` holds everything else.
    """

    _META_COLS = frozenset({
        "event_id", "event_type", "source", "entity_id",
        "observed_at", "effective_at",
        "ingestion_run_id",
    })

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._table: pa.Table | None = None

    def __iter__(self) -> Iterator[Record]:
        if not self.path.exists():
            raise FileNotFoundError(f"Source file not found: {self.path}")
        self._table = pq.read_table(self.path)
        df = self._table.to_pandas()
        for _, row in df.iterrows():
            d = row.to_dict()
            meta = {k: v for k, v in d.items() if k in self._META_COLS}
            payload = {k: v for k, v in d.items() if k not in self._META_COLS}
            yield Record(
                event_type=meta.get("event_type", "Unknown"),
                payload=payload,
                meta=meta,
            )


class FileSink(Sink):
    """Write Records to a Parquet file.

    Schema is inferred from the first emitted record (all-string fallback
    for None).  Atomic write via tmp+rename.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._rows: list[dict[str, Any]] = []

    def emit(self, record: Record) -> None:
        # Write in the flat format expected by FileSource:
        # meta fields at the top level + payload fields at the top level.
        self._rows.append(
            {
                "event_type": record.event_type,
                **record.meta,
                **record.payload,
            }
        )

    def __exit__(self, *exc: Any) -> None:
        if not self._rows:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        # Normalise: coerce None → ""
        clean = [
            {k: ("" if v is None else str(v)) for k, v in row.items()}
            for row in self._rows
        ]
        df = pd.DataFrame(clean)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, tmp)
        tmp.replace(self.path)


# ---------------------------------------------------------------------------
# Stream implementations (newline-delimited JSON over stdio)
# ---------------------------------------------------------------------------


class StreamSource(Source):
    """Read ND-JSON from an open text stream (default: stdin)."""

    def __init__(self, stream: Any = sys.stdin) -> None:
        self._stream = stream

    def __iter__(self) -> Iterator[Record]:
        for line in self._stream:
            line = line.strip()
            if not line:
                continue
            yield Record.from_dict(json.loads(line))


class StreamSink(Sink):
    """Write ND-JSON to an open text stream (default: stdout).

    Flushes after every emit so downstream prosumers see data immediately
    (true UNIX pipe semantics).
    """

    def __init__(self, stream: Any = sys.stdout) -> None:
        self._stream = stream

    def emit(self, record: Record) -> None:
        self._stream.write(json.dumps(record.to_dict(), default=str) + "\n")
        self._stream.flush()


# ---------------------------------------------------------------------------
# Factory helpers (used by CLI parsers)
# ---------------------------------------------------------------------------


def open_source(
    *,
    file_in: str | Path | None = None,
    stream_in: bool = False,
    stream: Any = sys.stdin,
) -> Source:
    """Return a Source based on the given flags.

    Exactly one of ``file_in`` or ``stream_in`` must be provided.
    """
    if file_in and stream_in:
        raise ValueError("Cannot use both --file-in and --stream-in")
    if file_in:
        return FileSource(file_in)
    if stream_in:
        return StreamSource(stream)
    raise ValueError("Must provide --file-in or --stream-in")


def open_sink(
    *,
    file_out: str | Path | None = None,
    stream_out: bool = False,
    stream: Any = sys.stdout,
) -> Sink:
    """Return a Sink based on the given flags.

    Exactly one of ``file_out`` or ``stream_out`` must be provided.
    """
    if file_out and stream_out:
        raise ValueError("Cannot use both --file-out and --stream-out")
    if file_out:
        return FileSink(file_out)
    if stream_out:
        return StreamSink(stream)
    raise ValueError("Must provide --file-out or --stream-out")
