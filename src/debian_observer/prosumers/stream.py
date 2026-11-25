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

def _glob_files(path: Path) -> list[Path]:
    """Expand a glob pattern (a plain path returns itself)."""
    import glob as _glob

    return [Path(p) for p in _glob.glob(str(path))]


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

        Two shapes are read: the flat prosumer record (``_event_type``,
        ``_meta`` and the payload fields at the top level, what a
        ``StreamSink`` writes) and the canonical event envelope
        (``event_type`` plus a nested ``payload`` dict, what
        ``normalize`` writes).

        The input dict is NOT mutated: the envelope keys are read from a
        shallow copy, so the caller keeps its dict intact (a library
        caller may rehydrate the same dict twice, or read it afterwards).
        """
        # Canonical event envelope (the ``normalize artifacts`` output):
        # top-level ``event_type`` plus a nested ``payload`` dict. The
        # envelope fields (event_id, observed_at, ...) are metadata for
        # the fold, so they land in ``meta`` next to the real payload.
        if "event_type" in d and isinstance(d.get("payload"), dict):
            meta = {k: v for k, v in d.items() if k != "payload"}
            return cls(
                event_type=d["event_type"],
                payload=dict(d["payload"]),
                meta=meta,
            )
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

    @staticmethod
    def _resolve_files(path: Path) -> list[Path]:
        """A file, a glob, or a directory of .parquet files."""
        if path.is_dir():
            files = sorted(path.glob("*.parquet"))
            if not files:
                raise FileNotFoundError(
                    f"No .parquet files in directory: {path}"
                )
            return files
        files = sorted(f for f in _glob_files(path) if f.is_file())
        return files or [path]

    def _iter_rows(self) -> Iterator[dict[str, Any]]:
        for file in self._resolve_files(self.path):
            if not file.exists():
                raise FileNotFoundError(f"Source file not found: {file}")
            table = pq.read_table(file)
            df = table.to_pandas()
            for _, row in df.iterrows():
                yield row.to_dict()

    def __iter__(self) -> Iterator[Record]:
        for d in self._iter_rows():
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

    def _target(self) -> Path:
        """A directory target writes one file inside it."""
        if self.path.is_dir():
            return self.path / "sink.parquet"
        return self.path

    def __exit__(self, *exc: Any) -> None:
        if not self._rows:
            return
        target = self._target()
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".tmp")
        # Normalise: coerce None → ""
        clean = [
            {k: ("" if v is None else str(v)) for k, v in row.items()}
            for row in self._rows
        ]
        df = pd.DataFrame(clean)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, tmp)
        tmp.replace(target)
        self.path = target


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
# NATS implementations (subject streams)
# ---------------------------------------------------------------------------


class NatsSource(Source):
    """Read Records from a NATS subject.

    A test injects ``_client`` with the same async surface
    (``async for msg in sub`` yielding JSON payloads); the real
    client connects lazily on first iteration.
    """

    def __init__(
        self,
        subject: str,
        url: str = "nats://localhost:4222",
        _client: Any = None,
    ) -> None:
        self.subject = subject
        self.url = url
        self._client = _client
        self._nc: Any = None

    async def _subscribe(self) -> Any:
        if self._client is not None:
            return self._client
        import nats

        self._nc = await nats.connect(servers=[self.url])
        return await self._nc.subscribe(self.subject)

    def __iter__(self) -> Iterator[Record]:
        import asyncio

        async def _run() -> list[Record]:
            sub = await self._subscribe()
            records = []
            async for msg in sub:
                data = msg.data if hasattr(msg, "data") else msg
                if isinstance(data, bytes):
                    data = data.decode()
                records.append(Record.from_dict(json.loads(data)))
            return records

        return iter(asyncio.run(_run()))

    def __enter__(self) -> "NatsSource":
        return self

    def __exit__(self, *exc: Any) -> None:
        if self._nc is not None:
            import asyncio

            asyncio.run(self._nc.drain())


class NatsSink(Sink):
    """Publish each Record as one NATS message (flushed per emit)."""

    def __init__(
        self,
        subject: str,
        url: str = "nats://localhost:4222",
        _client: Any = None,
    ) -> None:
        self.subject = subject
        self.url = url
        self._client = _client
        self._nc: Any = None

    def _ensure_client(self) -> Any:
        """The client to publish through (sync: tests inject one)."""
        return self._client

    def emit(self, record: Record) -> None:
        import asyncio

        async def _publish(client: Any) -> None:
            await client.publish(
                self.subject,
                json.dumps(record.to_dict(), default=str).encode(),
            )

        client = self._ensure_client()
        if client is None:
            import nats

            async def _connect_and_publish() -> None:
                self._nc = await nats.connect(servers=[self.url])
                await self._nc.publish(
                    self.subject,
                    json.dumps(record.to_dict(), default=str).encode(),
                )
                await self._nc.flush()

            asyncio.run(_connect_and_publish())
        else:
            asyncio.run(_publish(client))

    def __enter__(self) -> "NatsSink":
        return self

    def __exit__(self, *exc: Any) -> None:
        if self._nc is not None:
            import asyncio

            asyncio.run(self._nc.drain())


# ---------------------------------------------------------------------------
# Factory helpers (used by CLI parsers)
# ---------------------------------------------------------------------------


def open_source(
    *,
    file_in: str | Path | None = None,
    stream_in: bool = False,
    stream: Any = sys.stdin,
    nats_url: str | None = None,
    nats_subject: str | None = None,
) -> Source:
    """Return a Source based on the given flags.

    Exactly one of ``file_in`` or ``stream_in`` must be provided.
    ``--stream-in`` reads stdio ND-JSON, unless ``nats_url`` and
    ``nats_subject`` are given: then it reads a NATS subject.
    """
    if file_in and stream_in:
        raise ValueError("Cannot use both --file-in and --stream-in")
    if file_in:
        return FileSource(file_in)
    if stream_in:
        if nats_url and nats_subject:
            return NatsSource(subject=nats_subject, url=nats_url)
        return StreamSource(stream)
    raise ValueError("Must provide --file-in or --stream-in")


def open_sink(
    *,
    file_out: str | Path | None = None,
    stream_out: bool = False,
    stream: Any = sys.stdout,
    nats_url: str | None = None,
    nats_subject: str | None = None,
) -> Sink:
    """Return a Sink based on the given flags.

    Exactly one of ``file_out`` or ``stream_out`` must be provided.
    ``--stream-out`` writes stdio ND-JSON, unless ``nats_url`` and
    ``nats_subject`` are given: then it publishes to a NATS subject.
    """
    if file_out and stream_out:
        raise ValueError("Cannot use both --file-out and --stream-out")
    if file_out:
        return FileSink(file_out)
    if stream_out:
        if nats_url and nats_subject:
            return NatsSink(subject=nats_subject, url=nats_url)
        return StreamSink(stream)
    raise ValueError("Must provide --file-out or --stream-out")
