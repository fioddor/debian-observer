"""Tests: enqueue one parse job per observed snapshot.

Pins the parse work queue (docs/release3.project.v0.1.md §11.2,
§7.3, ADR-027): the queue is its own stream ``britney-parse`` with
**work queue** retention — one job = one snapshot, a job done is a
message gone — distinct from the replayable ``debian-migrations``
stream, so neither retention evicts the other's. The enqueue side
(the ingestor) publishes one job per observed snapshot of an
ingested window, idempotently:

* the job id is **derived** — a uuid5 of the ``snapshot_id`` (the
  deterministic-identity pattern of ADR-025/ADR-019), so a re-run
  derives the same id;
* the message carries ``Nats-Msg-Id: <job_id>``, so JetStream
  absorbs a re-run server-side (an optimization — the guarantee is
  that the worker's writes are atomic and keyed by snapshot);
* the job names its **raw object** (§7.3): the ingestor puts each
  raw ``.yaml.gz`` in a NATS Object Store bucket, and the worker will
  read it from there — never from the source (§4.1.3).

Tests run against a fake NATS client (no network): the
``tests/messaging`` fakes, each carrying its own header below.
"""

from __future__ import annotations

import gzip
import json
import uuid
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from debian_observer.ingestion.britney import sha256_bytes
from debian_observer.messaging.parse_queue import (
    PARSE_JOB_NAMESPACE,
    PARSE_STREAM_NAME,
    PARSE_SUBJECT,
    RAW_BUCKET,
    EnqueueStats,
    ParseJob,
    ParseQueueConfig,
    RawSnapshot,
    enqueue_snapshots,
    ensure_parse_queue,
    parse_job_id,
    raw_object_name,
)

# ---------------------------------------------------------------------------
# Spec values (§11.2): the queue keeps its own work-queue retention
# ---------------------------------------------------------------------------

#: Spec §11.2: the stream is named `britney-parse`, its own stream.
SPEC_STREAM_NAME = "britney-parse"

#: Spec §11.2: work queue retention — one job = one snapshot, done is gone.
SPEC_RETENTION = "workqueue"

#: Spec §11.2: `max_deliver` 5, then a dead-letter record.
SPEC_MAX_DELIVER = 5


# ---------------------------------------------------------------------------
# A fake NATS: JetStream dedup + object stores (no network)
# ---------------------------------------------------------------------------


class FakeObjectStore:
    """Stand-in object store: objects kept by name."""

    def __init__(self, bucket: str) -> None:
        self.bucket = bucket
        self.objects: dict[str, bytes] = {}

    async def put(self, name: str, data: Any, meta: Any = None) -> Any:
        self.objects[name] = bytes(data)
        return SimpleNamespace(name=name, size=len(self.objects[name]), bucket=self.bucket)

    async def get(self, name: str, *args: Any, **kwargs: Any) -> Any:
        if name not in self.objects:
            raise KeyError(name)  # NotFoundError stand-in
        return SimpleNamespace(name=name, data=self.objects[name])


class FakeJetStream:
    """Stand-in JetStream: streams, dedup, and object stores."""

    def __init__(self) -> None:
        self.streams: dict[str, Any] = {}
        self.add_stream_calls: list[dict[str, Any]] = []
        self.msg_ids: dict[str, int] = {}
        self.published: list[tuple[str, bytes, dict[str, Any] | None]] = []
        self.stores: dict[str, FakeObjectStore] = {}
        self.create_store_calls: list[str] = []

    async def stream_info(self, name: str) -> Any:
        if name in self.streams:
            return SimpleNamespace(name=name)
        raise KeyError(name)  # NotFoundError stand-in

    async def add_stream(self, **config: Any) -> Any:
        self.add_stream_calls.append(config)
        self.streams[config["name"]] = config
        return SimpleNamespace(name=config["name"])

    async def publish(
        self,
        subject: str,
        payload: bytes,
        timeout: float | None = None,
        stream: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> Any:
        msg_id = (headers or {}).get("Nats-Msg-Id")
        duplicate = msg_id is not None and msg_id in self.msg_ids
        if msg_id is not None and not duplicate:
            self.msg_ids[msg_id] = len(self.msg_ids) + 1
        self.published.append((subject, payload, headers))
        return SimpleNamespace(seq=self.msg_ids.get(msg_id, 0), duplicate=duplicate)

    async def object_store(self, bucket: str) -> FakeObjectStore:
        if bucket not in self.stores:
            raise KeyError(bucket)  # NotFoundError stand-in
        return self.stores[bucket]

    async def create_object_store(
        self, bucket: str, config: Any = None, **params: Any
    ) -> FakeObjectStore:
        self.create_store_calls.append(bucket)
        if bucket not in self.stores:
            self.stores[bucket] = FakeObjectStore(bucket)
        return self.stores[bucket]


class FakeNATSClient:
    """Stand-in NATSClient: same surface, no network."""

    def __init__(self, servers: list[str] | None = None) -> None:
        self.servers = servers or ["nats://localhost:4222"]
        self.is_connected = False
        self._js = FakeJetStream()

    async def connect(self) -> None:
        self.is_connected = True

    async def close(self) -> None:
        self.is_connected = False

    @property
    def jetstream(self) -> FakeJetStream:
        return self._js


# ---------------------------------------------------------------------------
# Synthetic, deterministic observed snapshots
# ---------------------------------------------------------------------------


def _document(items: int, when: str = "2026-01-01T00:00:00") -> dict[str, Any]:
    sources = []
    for i in range(items):
        sources.append(
            {
                "item-name": f"item-{i}",
                "source": f"pkg-{i}",
                "old-version": f"1.{i}",
                "new-version": f"1.{i + 1}",
                "migration-policy-verdict": "PASS",
                "is-candidate": True,
                "reason": ["waiting for 5 days"],
                "dependencies": {"blocked-by": ["item-0"], "migrate-after": []},
            }
        )
    return {"generated-date": when, "sources": sources}


def _gz(document: dict[str, Any]) -> bytes:
    return gzip.compress(yaml.safe_dump(document).encode("utf-8"))


def _raw(snapshot_id: str, items: int = 2, **kwargs: Any) -> RawSnapshot:
    body = _gz(_document(items, **kwargs))
    return RawSnapshot(
        snapshot_id=snapshot_id,
        gz_bytes=body,
        url=f"https://release.debian.org/britney/update_excuses/2026/{snapshot_id}.yaml.gz",
    )


# ---------------------------------------------------------------------------
# The stream: a work queue of its own
# ---------------------------------------------------------------------------


class TestTheParseStream:
    """`britney-parse`: work queue retention, one job = one snapshot."""

    async def test_the_stream_is_created_as_a_work_queue(self) -> None:
        client = FakeNATSClient()
        info = await ensure_parse_queue(client)
        assert info.name == SPEC_STREAM_NAME
        assert len(client._js.add_stream_calls) == 1
        config = client._js.add_stream_calls[0]
        assert config["name"] == PARSE_STREAM_NAME == SPEC_STREAM_NAME
        assert config["retention"] == SPEC_RETENTION
        assert config["subjects"] == [PARSE_SUBJECT]
        assert config["storage"] == "file"

    async def test_ensuring_twice_adds_no_second_stream(self) -> None:
        client = FakeNATSClient()
        await ensure_parse_queue(client)
        await ensure_parse_queue(client)
        assert len(client._js.add_stream_calls) == 1

    def test_the_queue_config_pins_the_spec_values(self) -> None:
        config = ParseQueueConfig()
        assert config.stream_name == SPEC_STREAM_NAME
        assert config.retention == SPEC_RETENTION


# ---------------------------------------------------------------------------
# The job id: derived, deterministic (the ADR-025 pattern)
# ---------------------------------------------------------------------------


class TestTheJobId:
    """A job's id is a uuid5 of its snapshot_id: stable across runs."""

    def test_the_job_id_is_deterministic(self) -> None:
        first = parse_job_id("20260101T0000Z")
        second = parse_job_id("20260101T0000Z")
        assert first == second
        assert first != parse_job_id("20260101T0100Z")

    def test_the_job_id_is_a_uuid5_of_the_snapshot_id(self) -> None:
        snapshot_id = "20260101T0000Z"
        assert parse_job_id(snapshot_id) == uuid.uuid5(
            PARSE_JOB_NAMESPACE, snapshot_id
        )
        assert isinstance(parse_job_id(snapshot_id), uuid.UUID)


# ---------------------------------------------------------------------------
# The enqueue: one job per observed snapshot, idempotent
# ---------------------------------------------------------------------------


class TestEnqueueSnapshots:
    """The ingestor side: a job per snapshot, its raw object in the bucket."""

    async def test_one_job_per_observed_snapshot(self) -> None:
        client = FakeNATSClient()
        snapshots = [_raw(f"20260101T0{i:02d}Z", items=i + 1) for i in range(3)]
        stats = await enqueue_snapshots(client, snapshots)
        assert isinstance(stats, EnqueueStats)
        assert stats.jobs == 3
        assert stats.duplicates_absorbed == 0
        # Every job went to the queue's subject, one job per snapshot.
        subjects = [subject for subject, _, _ in client._js.published]
        assert subjects == [PARSE_SUBJECT] * 3
        payloads = [json.loads(payload) for _, payload, _ in client._js.published]
        assert [job["snapshot_id"] for job in payloads] == [
            s.snapshot_id for s in snapshots
        ]

    async def test_the_job_names_its_raw_object_and_carries_its_sha(self) -> None:
        client = FakeNATSClient()
        snapshot = _raw("20260101T0000Z", items=2)
        await enqueue_snapshots(client, [snapshot])
        _, payload, headers = client._js.published[0]
        job = json.loads(payload)
        # The raw object name: the worker reads it from the bucket and
        # never from the source (§7.3).
        assert job["raw_object"] == raw_object_name(snapshot.snapshot_id)
        assert job["raw_object"] == "20260101T0000Z.yaml.gz"
        assert job["sha256"] == sha256_bytes(snapshot.gz_bytes)
        assert job["url"] == snapshot.url
        # The message deduplicates by its derived job id.
        assert headers is not None
        assert headers["Nats-Msg-Id"] == str(parse_job_id(snapshot.snapshot_id))

    async def test_the_raw_bytes_are_in_the_bucket_under_the_object_name(self) -> None:
        client = FakeNATSClient()
        snapshot = _raw("20260101T0000Z", items=2)
        await enqueue_snapshots(client, [snapshot])
        store = client._js.stores[RAW_BUCKET]
        body = store.objects[raw_object_name(snapshot.snapshot_id)]
        assert body == snapshot.gz_bytes
        assert gzip.decompress(body)

    async def test_a_re_run_is_absorbed_server_side(self) -> None:
        client = FakeNATSClient()
        snapshots = [_raw(f"20260101T0{i:02d}Z") for i in range(3)]
        first = await enqueue_snapshots(client, snapshots)
        second = await enqueue_snapshots(client, snapshots)
        # The first run keeps every job; the re-run keeps none —
        # each job publishes again with the same derived id and the
        # stream absorbs it server-side.
        assert first.jobs == 3
        assert first.duplicates_absorbed == 0
        assert second.jobs == 0
        assert second.duplicates_absorbed == 3
        # The distinct ids the stream holds did not double.
        assert len(client._js.msg_ids) == 3

    async def test_the_job_round_trips_through_json(self) -> None:
        snapshot = _raw("20260101T0000Z")
        job = ParseJob(
            snapshot_id=snapshot.snapshot_id,
            raw_object=raw_object_name(snapshot.snapshot_id),
            sha256=sha256_bytes(snapshot.gz_bytes),
            url=snapshot.url,
        )
        clone = ParseJob.from_json_bytes(job.to_json_bytes())
        assert clone == job