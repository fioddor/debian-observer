"""Tests for prosumers.stream abstractions.

Every researcher adding a new Source or Sink should look at these
tests to understand the ``Record`` contract and the expected junction
behaviour.
"""

from __future__ import annotations

from io import StringIO
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from debian_observer.prosumers.packages import fold_package_versions
from debian_observer.prosumers.stream import FileSink, FileSource, Record, StreamSink, StreamSource


def make_parquet(path: Path, rows: list[dict]) -> None:
    """Write *rows* as a Parquet file."""
    df = pd.DataFrame(rows)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)


def rows_to_records(rows: list[dict]) -> list[Record]:
    """Turn raw dict rows into ``Record`` objects."""
    records = []
    for row in rows:
        meta_cols = {"event_id", "event_type", "observed_at", "effective_at", "ingestion_run_id"}
        meta = {k: row[k] for k in meta_cols}
        payload = {k: v for k, v in row.items() if k not in meta_cols}
        records.append(Record(event_type=row["event_type"], payload=payload, meta=meta))
    return records


SAMPLE_ROWS = [
    {
        "event_id": "e1",
        "event_type": "PackageVersionObserved",
        "observed_at": "2026-10-01T10:00:00Z",
        "effective_at": "2026-10-01T10:00:00Z",
        "ingestion_run_id": "r1",
        "package_id": "pkg-a",
        "source_package": "pkg-a",
        "version": "1.0",
        "distribution": "debian",
        "release": "sid",
        "suite": None,
        "architecture": "amd64",
        "component": "main",
        "section": "utils",
        "priority": "optional",
        "source": "udd",
    },
    {
        "event_id": "e2",
        "event_type": "PackageVersionObserved",
        "observed_at": "2026-10-01T11:00:00Z",
        "effective_at": "2026-10-01T11:00:00Z",
        "ingestion_run_id": "r1",
        "package_id": "pkg-b",
        "source_package": "pkg-b",
        "version": "2.0",
        "distribution": "ubuntu",
        "release": "noble",
        "suite": "stable",
        "architecture": "all",
        "component": "universe",
        "section": "utils",
        "priority": "optional",
        "source": "mirror",
    },
]


class TestFileSource:
    """Reading Parquet into Records."""

    def test_reads_event_log_schema(self, tmp_path: Path) -> None:
        path = tmp_path / "events.parquet"
        make_parquet(path, SAMPLE_ROWS)

        with FileSource(path) as source:
            records = list(source)

        assert len(records) == 2
        assert records[0].event_type == "PackageVersionObserved"
        assert records[0].payload["package_id"] == "pkg-a"
        assert records[0].meta["event_id"] == "e1"
        assert records[0].meta["observed_at"] == "2026-10-01T10:00:00Z"

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            with FileSource(tmp_path / "missing.parquet") as source:
                list(source)


class TestFileSink:
    """Writing Records to Parquet."""

    def test_roundtrip(self, tmp_path: Path) -> None:
        path = tmp_path / "out.parquet"
        records = rows_to_records(SAMPLE_ROWS)

        with FileSink(path) as sink:
            for rec in records:
                sink.emit(rec)

        with FileSource(path) as source:
            read_back = list(source)

        assert len(read_back) == 2
        assert read_back[0].payload["package_id"] == "pkg-a"

    def test_empty_file_not_created(self, tmp_path: Path) -> None:
        path = tmp_path / "empty.parquet"
        with FileSink(path) as sink:
            pass
        assert not path.exists()


class TestStreamSource:
    """Reading ND-JSON from a text stream."""

    def test_reads_ndjson(self) -> None:
        records = rows_to_records(SAMPLE_ROWS)
        buf = StringIO()
        for rec in records:
            buf.write(rec.to_dict().__repr__() + "\n")
        # Actually use StreamSink to produce valid ND-JSON
        buf2 = StringIO()
        with StreamSink(buf2) as sink:
            for rec in records:
                sink.emit(rec)
        buf2.seek(0)

        with StreamSource(buf2) as source:
            read_back = list(source)

        assert len(read_back) == 2
        assert read_back[0].event_type == "PackageVersionObserved"
        assert read_back[0].payload["package_id"] == "pkg-a"

    def test_skips_blank_lines(self) -> None:
        buf = StringIO('\n{"_event_type":"T","_meta":{},"k":"v"}\n\n')
        with StreamSource(buf) as source:
            records = list(source)
        assert len(records) == 1


class TestStreamSink:
    """Writing ND-JSON to a text stream."""

    def test_writes_ndjson(self) -> None:
        buf = StringIO()
        rec = Record(event_type="T", payload={"k": "v"}, meta={"id": "1"})
        with StreamSink(buf) as sink:
            sink.emit(rec)
        buf.seek(0)
        line = buf.readline().strip()
        assert '"_event_type": "T"' in line
        assert '"k": "v"' in line


class TestRecordRoundtrip:
    """Record (de)serialisation behaviour (ND-JSON envelope)."""

    def _sample_dict(self) -> dict:
        return {
            "_event_type": "PackageVersionObserved",
            "_meta": {"event_id": "e1", "observed_at": "2026-10-01T10:00:00Z"},
            "package_id": "pkg-a",
            "version": "1.0",
        }

    def test_from_dict_does_not_mutate_input(self) -> None:
        d = self._sample_dict()
        expected = dict(d)
        rec = Record.from_dict(d)
        assert d == expected, "from_dict must not mutate the caller's dict"
        assert rec.event_type == "PackageVersionObserved"
        assert rec.meta == {"event_id": "e1", "observed_at": "2026-10-01T10:00:00Z"}
        assert rec.payload == {"package_id": "pkg-a", "version": "1.0"}

    def test_from_dict_missing_envelope_defaults(self) -> None:
        d = {"package_id": "pkg-b"}
        rec = Record.from_dict(d)
        assert rec.event_type == "Unknown"
        assert rec.meta == {}
        assert rec.payload == {"package_id": "pkg-b"}

    def test_from_dict_then_to_dict_roundtrip(self) -> None:
        d = self._sample_dict()
        rec = Record.from_dict(d)
        out = rec.to_dict()
        assert out["_event_type"] == "PackageVersionObserved"
        assert out["_meta"] == {"event_id": "e1", "observed_at": "2026-10-01T10:00:00Z"}
        assert out["package_id"] == "pkg-a"
        # roundtrip: the rehydrated record survives another from_dict
        rec2 = Record.from_dict(out)
        assert rec2 == rec or (rec2.event_type, rec2.payload) == (rec.event_type, rec.payload)


def _envelope(package_id: str, version: str) -> dict:
    """One canonical event envelope line, as ``normalize artifacts`` emits."""
    return {
        "event_id": f"evt-{package_id}-{version}",
        "event_type": "PackageVersionObserved",
        "schema_version": "1",
        "source": "mirror.example.org",
        "observed_at": "2026-10-01T10:00:00Z",
        "effective_at": "2026-10-01T10:00:00Z",
        "entity_id": f"{package_id}/{version}",
        "ingestion_run_id": "run-1",
        "payload": {
            "package_id": package_id,
            "version": version,
            "distribution": "debian",
            "release": "trixie",
            "suite": "main",
            "architecture": "amd64",
        },
    }


class TestRecordEventEnvelope:
    """Canonical event envelopes (the ``normalize artifacts`` output)."""

    def test_from_dict_reads_canonical_event_envelope(self) -> None:
        """An envelope line rehydrates with its real type and fields.

        ``debian-observer normalize artifacts`` pipes canonical envelope
        JSON lines (top-level ``event_type`` plus a nested ``payload``
        dict) into prosumers; before this contract was pinned every such
        line collapsed into one row of empty strings (event_type
        ``Unknown``, the whole envelope lost inside one opaque field),
        silently folding distinct observations into a single empty row.
        The envelope fields (event_id, observed_at, ...) must surface as
        record metadata next to the real payload.
        """
        d = _envelope("pkg-a", "1.0")
        expected = dict(d)
        rec = Record.from_dict(d)
        assert d == expected, "from_dict must not mutate the caller's dict"
        assert rec.event_type == "PackageVersionObserved"
        assert rec.payload["package_id"] == "pkg-a"
        assert rec.payload["version"] == "1.0"
        assert rec.meta["event_id"] == "evt-pkg-a-1.0"
        assert rec.meta["observed_at"] == "2026-10-01T10:00:00Z"

    def test_envelope_records_fold_into_real_rows(self) -> None:
        """Envelope lines fold into one row per observation, with values.

        This is the end-user contract:
        ``normalize artifacts ... | debian-observer-consolidate
        --stream-in --stream-out`` must yield one consolidated row per
        observation PK carrying the real package_id/version/... — not a
        single row of empty strings.
        """
        records = [
            Record.from_dict(_envelope("pkg-a", "1.0")),
            Record.from_dict(_envelope("pkg-a", "1.0-1")),
            Record.from_dict(_envelope("pkg-b", "2.0")),
        ]
        rows, stats = fold_package_versions(records)
        assert stats.pvo_events == 3
        assert len(rows) == 3
        by_pk = {(r["package_id"], r["version"]): r for r in rows}
        assert set(by_pk) == {
            ("pkg-a", "1.0"),
            ("pkg-a", "1.0-1"),
            ("pkg-b", "2.0"),
        }
        assert by_pk[("pkg-a", "1.0")]["architecture"] == "amd64"
        assert by_pk[("pkg-b", "2.0")]["source"] == "mirror.example.org"

    def test_flat_prosumer_records_still_fold(self) -> None:
        """The flat prosumer record shape keeps working unchanged.

        A record emitted by one prosumer (``_event_type`` / ``_meta``
        plus payload at top level) rehydrated through ``from_dict``
        still folds exactly as before the envelope support was added.
        """
        d = {
            "_event_type": "PackageVersionObserved",
            "_meta": {
                "event_id": "e1",
                "event_type": "PackageVersionObserved",
            },
            "package_id": "pkg-c",
            "version": "3.0",
            "distribution": "debian",
            "release": "trixie",
            "suite": "main",
            "architecture": "all",
        }
        rows, stats = fold_package_versions([Record.from_dict(d)])
        assert stats.pvo_events == 1
        assert len(rows) == 1
        assert rows[0]["package_id"] == "pkg-c"
        assert rows[0]["version"] == "3.0"
