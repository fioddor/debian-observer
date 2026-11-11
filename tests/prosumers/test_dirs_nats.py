"""Tests for directory inputs and the NATS stream sources/sinks.

- ``--file-in``/``--file-out`` accept **directories**: a directory
  reads every ``.parquet`` file inside as one source.
- ``--stream-in``/``--stream-out`` accept **NATS** streams as
  parameters: with ``--nats-url``/``--nats-subject`` the stream is a
  NATS subject, not stdio. The pieces are reusable: every prosumer
  gets them from ``open_source``/``open_sink``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from debian_observer.prosumers.stream import (
    FileSink,
    FileSource,
    NatsSink,
    NatsSource,
    Record,
    open_sink,
    open_source,
)


def _write(path: Path, n: int, tag: str) -> None:
    schema = pa.schema(
        [
            ("event_type", pa.string()),
            ("package", pa.string()),
            ("clause_index", pa.int64()),
        ]
    )
    rows = [
        {
            "event_type": "DependencyObserved",
            "package": f"{tag}-{i}",
            "clause_index": i,
        }
        for i in range(n)
    ]
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


class TestDirectoryInput:
    def test_file_in_accepts_a_directory(self, tmp_path: Path) -> None:
        lake = tmp_path / "lake"
        lake.mkdir()
        _write(lake / "run-001.parquet", 3, "a")
        _write(lake / "run-002.parquet", 2, "b")
        source = FileSource(lake)
        records = list(source)
        assert len(records) == 5
        assert len({r.payload["package"] for r in records}) == 5

    def test_open_source_accepts_a_directory(self, tmp_path: Path) -> None:
        lake = tmp_path / "lake"
        lake.mkdir()
        _write(lake / "run-001.parquet", 1, "a")
        _write(lake / "run-002.parquet", 1, "b")
        source = open_source(file_in=lake)
        assert len(list(source)) == 2

    def test_file_out_accepts_a_directory(self, tmp_path: Path) -> None:
        out = tmp_path / "out"
        out.mkdir()
        sink = FileSink(out)
        with sink:
            sink.emit(
                Record(
                    event_type="X",
                    payload={"package": "p"},
                    meta={"event_id": "e1"},
                )
            )
        files = list(out.glob("*.parquet"))
        assert len(files) == 1


class TestNatsStream:
    def test_nats_source_reads_a_subject(self) -> None:
        # a fake NATS connection: yields JSON lines as messages
        class FakeSub:
            def __init__(self):
                self._msgs = [
                    json.dumps({"_event_type": "X", "package": "a"}),
                    json.dumps({"_event_type": "X", "package": "b"}),
                ]

            async def __aiter__(self):
                for m in self._msgs:
                    yield m

        source = NatsSource(subject="events.dep", _client=FakeSub())
        records = list(source)
        assert [r.payload["package"] for r in records] == ["a", "b"]

    def test_nats_sink_publishes_each_record(self) -> None:
        class FakeClient:
            def __init__(self):
                self.published = []

            async def publish(self, subject: str, payload: bytes):
                self.published.append((subject, json.loads(payload)))

        client = FakeClient()
        sink = NatsSink(subject="events.out", _client=client)
        with sink:
            sink.emit(
                Record(
                    event_type="X",
                    payload={"package": "p"},
                    meta={"event_id": "e1"},
                )
            )
        assert len(client.published) == 1
        subject, payload = client.published[0]
        assert subject == "events.out"
        assert payload["_event_type"] == "X"

    def test_open_source_with_nats(self) -> None:
        class FakeSub:
            async def __aiter__(self):
                return
                yield

        source = open_source(
            stream_in=True, nats_url="nats://x", nats_subject="s"
        )
        # constructed without connecting (lazy): it is a NatsSource
        from debian_observer.prosumers.stream import NatsSource

        assert isinstance(source, NatsSource)

    def test_open_sink_with_nats(self) -> None:
        sink = open_sink(
            stream_out=True, nats_url="nats://x", nats_subject="s"
        )
        from debian_observer.prosumers.stream import NatsSink

        assert isinstance(sink, NatsSink)