"""Tests: work the parse queue politely.

Pins the worker side of the parse work queue
(docs/release3.project.v0.1.md §11.2, §15, ADR-027):

* a **durable pull** consumer — one durable, shared by the workers on
  every machine that reaches the NATS server;
* ``--concurrency N`` — the worker's slots: the pull batch and
  ``max_ack_pending`` (backpressure, §11.2);
* per job: read the **raw object** from the bucket (never the
  source, §7.3/§4.1.3), verify its SHA-256, parse it (the snapshot
  parse), put the **compact state** back — atomic and keyed by
  snapshot, so a redelivered job overwrites with identical content —
  then ack;
* **in-progress acks** every ~10 s while parsing (§11.2): a slow job
  keeps its delivery, a dead worker's job is redelivered after
  ``ack_wait`` (§15);
* ``max_deliver`` 5, then a **dead-letter record** (§15): a poisoned
  snapshot does not loop forever and the queue continues;
* ``--drain S``: seconds before the worker stops (0 = run until
  signalled).

Tests run against fakes (no network) — the ``tests/messaging``
fakes, each carrying its own header below — and an injected
clock and sleep, so the battery
never waits.
"""

from __future__ import annotations

import asyncio
import collections
import gzip
import json
from types import SimpleNamespace
from typing import Any

import pyarrow.parquet as pq
import pytest
import yaml

from debian_observer.ingestion.britney import sha256_bytes
from debian_observer.messaging.parse_queue import (
    IN_PROGRESS_ACK_SECONDS,
    PARSE_ACK_WAIT_SECONDS,
    PARSE_MAX_DELIVER,
    PARSE_QUEUE_DURABLE,
    PARSE_STREAM_NAME,
    PARSE_SUBJECT,
    RAW_BUCKET,
    RESULTS_BUCKET,
    ParseJob,
    raw_object_name,
    result_object_name,
)
from debian_observer.messaging.parse_worker import FetchTimeout, ParseWorker

# ---------------------------------------------------------------------------
# Spec values (§11.2 / §12)
# ---------------------------------------------------------------------------

#: Spec §12: the worker's slots knob.
SPEC_CONCURRENCY = 4

#: Spec §11.2: in-progress acks every ~10 s.
SPEC_IN_PROGRESS_SECONDS = 10

#: Spec §12: --drain is integer seconds; 0 = run until signalled.
SPEC_DRAIN_SECONDS = 5


# ---------------------------------------------------------------------------
# A fake NATS: pull subscription + object stores (no network)
# ---------------------------------------------------------------------------


class FakeMsg:
    """Stand-in JetStream message: the job body plus ack controls."""

    def __init__(self, data: bytes, num_delivered: int = 1) -> None:
        self.data = data
        self.metadata = SimpleNamespace(num_delivered=num_delivered)
        self.acks = 0
        self.naks = 0
        self.in_progress_acks = 0

    async def ack(self) -> None:
        self.acks += 1

    async def nak(self, delay: float | None = None) -> None:
        self.naks += 1

    async def in_progress(self) -> None:
        self.in_progress_acks += 1


class FakeClock:
    """Injectable clock + sleep: the battery never waits for real."""

    def __init__(self, start: float = 0.0, step: float = 1.0) -> None:
        self.now = start
        self.step = step
        self.slept: list[float] = []

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds * self.step
        # Yield once per fake second, so concurrent fake sleeps
        # interleave as real ones would (the in-progress ping must
        # fire ~10 s of fake time apart while the parse sleeps).
        for _ in range(max(1, int(seconds))):
            await asyncio.sleep(0)


class FakeObjectStore:
    """Stand-in object store: objects kept by name."""

    def __init__(self, bucket: str) -> None:
        self.bucket = bucket
        self.objects: dict[str, bytes] = {}

    async def put(self, name: str, data: Any, meta: Any = None) -> Any:
        self.objects[name] = bytes(data)
        return SimpleNamespace(name=name, size=len(self.objects[name]))

    async def get(self, name: str, *args: Any, **kwargs: Any) -> Any:
        if name not in self.objects:
            raise KeyError(name)  # NotFoundError stand-in
        return SimpleNamespace(name=name, data=self.objects[name])


class FakePullSubscription:
    """Stand-in pull subscription: a queue of messages to fetch."""

    def __init__(self, msgs: list[FakeMsg] | None = None) -> None:
        self.queue: collections.deque[FakeMsg] = collections.deque(msgs or [])
        self.fetch_calls: list[dict[str, Any]] = []
        self.block = False  # fetch never answers (the cancel test)

    async def fetch(self, batch: int, timeout: float | None = None) -> list[FakeMsg]:
        self.fetch_calls.append({"batch": batch, "timeout": timeout})
        if self.block:
            await asyncio.Future()
        out: list[FakeMsg] = []
        while self.queue and len(out) < batch:
            out.append(self.queue.popleft())
        if not out:
            raise FetchTimeout()
        return out

    async def unsubscribe(self) -> None:
        pass


class FakeJetStream:
    """Stand-in JetStream: pull subscribe + object stores."""

    def __init__(self) -> None:
        self.stores: dict[str, FakeObjectStore] = {}
        self.subscribe_calls: list[dict[str, Any]] = []
        self.subscription: FakePullSubscription | None = None

    async def object_store(self, bucket: str) -> FakeObjectStore:
        if bucket not in self.stores:
            raise KeyError(bucket)  # NotFoundError stand-in
        return self.stores[bucket]

    async def create_object_store(
        self, bucket: str, config: Any = None, **params: Any
    ) -> FakeObjectStore:
        if bucket not in self.stores:
            self.stores[bucket] = FakeObjectStore(bucket)
        return self.stores[bucket]

    async def pull_subscribe(
        self,
        subject: str,
        durable: str | None = None,
        stream: str | None = None,
        config: Any = None,
        **kwargs: Any,
    ) -> FakePullSubscription:
        self.subscribe_calls.append(
            {"subject": subject, "durable": durable, "stream": stream, "config": config}
        )
        if self.subscription is None:
            self.subscription = FakePullSubscription()
        return self.subscription


class FakeNATSClient:
    """Stand-in NATSClient: same surface, no network."""

    def __init__(self, subscription: FakePullSubscription | None = None) -> None:
        self.servers = ["nats://localhost:4222"]
        self.is_connected = True
        self._js = FakeJetStream()
        self._js.subscription = subscription

    @property
    def jetstream(self) -> FakeJetStream:
        return self._js


# ---------------------------------------------------------------------------
# Synthetic, deterministic snapshots and jobs
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
                "dependencies": {
                    "blocked-by": ["item-0"],
                    "migrate-after": [],
                },
            }
        )
    return {"generated-date": when, "sources": sources}


def _gz(document: dict[str, Any]) -> bytes:
    return gzip.compress(yaml.safe_dump(document).encode("utf-8"))


def _raw_bytes(snapshot_id: str, items: int = 2) -> bytes:
    return _gz(_document(items))


def _job_msg(
    snapshot_id: str,
    gz: bytes,
    num_delivered: int = 1,
    *,
    sha256: str | None = None,
) -> FakeMsg:
    job = ParseJob(
        snapshot_id=snapshot_id,
        raw_object=raw_object_name(snapshot_id),
        sha256=sha256 or sha256_bytes(gz),
        url=None,
    )
    return FakeMsg(job.to_json_bytes(), num_delivered=num_delivered)


def _seed_raw(js: FakeJetStream, snapshot_id: str, gz: bytes) -> None:
    if RAW_BUCKET not in js.stores:
        js.stores[RAW_BUCKET] = FakeObjectStore(RAW_BUCKET)
    js.stores[RAW_BUCKET].objects[raw_object_name(snapshot_id)] = gz


def _results(js: FakeJetStream) -> FakeObjectStore:
    return js.stores.get(RESULTS_BUCKET, FakeObjectStore(RESULTS_BUCKET))


def _worker(**kwargs: Any) -> ParseWorker:
    kwargs.setdefault("concurrency", 1)
    return ParseWorker(client=FakeNATSClient(), **kwargs)


# ---------------------------------------------------------------------------
# The durable pull (§11.2: one durable, pull; backpressure)
# ---------------------------------------------------------------------------


def _seconds(value: Any) -> int:
    """A duration as seconds — a timedelta or a plain number."""
    return int(value.total_seconds()) if hasattr(value, "total_seconds") else int(value)


class TestTheDurablePull:
    """The worker attaches as one shared durable pull consumer."""

    async def test_the_worker_pulls_one_shared_durable(self) -> None:
        clock = FakeClock()
        worker = _worker(drain_seconds=SPEC_DRAIN_SECONDS, clock=clock.clock, sleep=clock.sleep)
        await worker.run()
        assert len(worker.client.jetstream.subscribe_calls) == 1
        call = worker.client.jetstream.subscribe_calls[0]
        assert call["subject"] == PARSE_SUBJECT
        assert call["stream"] == PARSE_STREAM_NAME
        assert call["durable"] == PARSE_QUEUE_DURABLE
        config = call["config"]
        # Spec §11.2: ack_wait above the worst-case parse under
        # contention; max_deliver 5, then a dead-letter record.
        assert _seconds(config.ack_wait) == PARSE_ACK_WAIT_SECONDS == 120
        assert config.max_deliver == PARSE_MAX_DELIVER == 5

    async def test_concurrency_sets_the_worker_slots(self) -> None:
        # Spec §11.2: max_ack_pending is the total worker slots
        # (backpressure) — the slots are the worker's --concurrency.
        clock = FakeClock()
        worker = _worker(
            concurrency=SPEC_CONCURRENCY,
            drain_seconds=SPEC_DRAIN_SECONDS,
            clock=clock.clock,
            sleep=clock.sleep,
        )
        await worker.run()
        call = worker.client.jetstream.subscribe_calls[0]
        assert call["config"].max_ack_pending == SPEC_CONCURRENCY
        # The pull batch is the slots too: no more than the worker
        # can work at once is ever in flight.
        assert worker.client.jetstream.subscription.fetch_calls
        assert all(
            c["batch"] == SPEC_CONCURRENCY
            for c in worker.client.jetstream.subscription.fetch_calls
        )


# ---------------------------------------------------------------------------
# One job: read the object, parse, put the state back, ack
# ---------------------------------------------------------------------------


class TestWorkingOneJob:
    """Per job: raw object in, compact state out, ack."""

    async def test_a_job_reads_its_raw_object_and_puts_the_state_back(self) -> None:
        snapshot_id = "20260101T0000Z"
        gz = _raw_bytes(snapshot_id, items=2)
        worker = _worker()
        _seed_raw(worker.client.jetstream, snapshot_id, gz)
        msg = _job_msg(snapshot_id, gz)
        await worker.process_message(msg)
        # Acked once, never nacked: the job is done.
        assert msg.acks == 1
        assert msg.naks == 0
        # The compact state went back to the results bucket, keyed by
        # snapshot (§7.3, §11.2), as one Parquet table (§9.2).
        name = result_object_name(snapshot_id)
        assert name in _results(worker.client.jetstream).objects
        table = pq.read_table(__import__("io").BytesIO(
            _results(worker.client.jetstream).objects[name]
        ))
        assert table.column("item_name").to_pylist() == ["item-0", "item-1"]
        assert table.column("snapshot").to_pylist() == [snapshot_id, snapshot_id]
        assert table.column("verdict").to_pylist() == ["PASS", "PASS"]
        assert table.column("is_candidate").to_pylist() == [True, True]
        assert json.loads(table.column("blocked_by").to_pylist()[0]) == ["item-0"]
        # The snapshot metadata rides along (§9.2).
        assert table.column("sha256").to_pylist()[0] == sha256_bytes(gz)

    async def test_a_missing_raw_object_naks_for_redelivery(self) -> None:
        # The job names an object the bucket does not hold: a
        # redelivery-worthy failure, not a dead letter (§15).
        snapshot_id = "20260101T0000Z"
        worker = _worker()
        msg = _job_msg(snapshot_id, _raw_bytes(snapshot_id))
        await worker.process_message(msg)
        assert msg.acks == 0
        assert msg.naks == 1
        assert result_object_name(snapshot_id) not in _results(worker.client.jetstream).objects

    async def test_a_wrong_sha_fails_the_job(self) -> None:
        # The object read is not the object the job named (its
        # sha256 disagrees): the worker will not parse bytes it
        # cannot vouch for (ADR-002).
        snapshot_id = "20260101T0000Z"
        worker = _worker()
        _seed_raw(worker.client.jetstream, snapshot_id, b"other bytes")
        msg = _job_msg(snapshot_id, _raw_bytes(snapshot_id))
        await worker.process_message(msg)
        assert msg.acks == 0
        assert msg.naks == 1

    async def test_in_progress_acks_flow_while_parsing(self) -> None:
        # Spec §11.2: in-progress acks every ~10 s while parsing —
        # a slow job keeps its delivery alive.
        clock = FakeClock()
        snapshot_id = "20260101T0000Z"
        gz = _raw_bytes(snapshot_id, items=1)

        class SlowParseWorker(ParseWorker):
            async def _parse_job(self, job: ParseJob, raw: bytes) -> Any:
                await self._sleep(SLOW_PARSE_SECONDS)
                return await super()._parse_job(job, raw)

        worker = SlowParseWorker(
            client=FakeNATSClient(), clock=clock.clock, sleep=clock.sleep
        )
        _seed_raw(worker.client.jetstream, snapshot_id, gz)
        msg = _job_msg(snapshot_id, gz)
        await worker.process_message(msg)
        # The parse "took" 25 s of fake time; the ping task asked
        # for the ~10 s cadence at least twice before the parse
        # finished, and the job still acked.
        assert SLOW_PARSE_SECONDS in clock.slept
        assert clock.slept.count(SPEC_IN_PROGRESS_SECONDS) >= 2
        assert msg.in_progress_acks >= 2
        assert msg.acks == 1

    async def test_a_redelivered_job_overwrites_identical_content(self) -> None:
        # §11.2: worker writes are atomic and keyed by snapshot — a
        # redelivered job overwrites with identical content.
        snapshot_id = "20260101T0000Z"
        gz = _raw_bytes(snapshot_id, items=3)
        worker = _worker()
        _seed_raw(worker.client.jetstream, snapshot_id, gz)
        first = _job_msg(snapshot_id, gz, num_delivered=1)
        await worker.process_message(first)
        name = result_object_name(snapshot_id)
        once = _results(worker.client.jetstream).objects[name]
        # The redelivery (a worker died after the write, before the
        # ack): same job, second delivery, same bytes.
        second = _job_msg(snapshot_id, gz, num_delivered=2)
        await worker.process_message(second)
        assert _results(worker.client.jetstream).objects[name] == once
        assert second.acks == 1


# ---------------------------------------------------------------------------
# A poisoned snapshot (§15: max_deliver, then a dead-letter record)
# ---------------------------------------------------------------------------


class TestPoisonedSnapshots:
    """max_deliver 5, then a dead-letter record; the queue continues."""

    async def test_a_broken_raw_naks_until_max_deliver(self) -> None:
        snapshot_id = "20260101T0000Z"
        worker = _worker()
        _seed_raw(worker.client.jetstream, snapshot_id, b"not a gzip")
        for delivery in range(1, PARSE_MAX_DELIVER):
            msg = _job_msg(snapshot_id, b"not a gzip", num_delivered=delivery)
            await worker.process_message(msg)
            assert msg.acks == 0
            assert msg.naks == 1
        # No dead letter yet, no result either.
        results = _results(worker.client.jetstream)
        assert not [
            name for name in results.objects if name.startswith("dead-letter")
        ]
        assert result_object_name(snapshot_id) not in results.objects

    async def test_max_deliver_dead_letters_and_acks(self) -> None:
        snapshot_id = "20260101T0000Z"
        worker = _worker()
        _seed_raw(worker.client.jetstream, snapshot_id, b"not a gzip")
        msg = _job_msg(
            snapshot_id, b"not a gzip", num_delivered=PARSE_MAX_DELIVER
        )
        await worker.process_message(msg)
        # The 5th delivery does not loop: it is acked away and a
        # dead-letter record names the cause.
        assert msg.acks == 1
        assert msg.naks == 0
        record = json.loads(
            _results(worker.client.jetstream).objects[
                f"dead-letter-{snapshot_id}.json"
            ].decode("utf-8")
        )
        assert record["snapshot_id"] == snapshot_id
        assert record["deliveries"] == PARSE_MAX_DELIVER
        assert record["raw_object"] == raw_object_name(snapshot_id)
        assert record["error"]

    async def test_the_queue_continues_past_a_dead_letter(self) -> None:
        poisoned = "20260101T0000Z"
        healthy = "20260101T0100Z"
        worker = _worker()
        _seed_raw(worker.client.jetstream, poisoned, b"not a gzip")
        _seed_raw(worker.client.jetstream, healthy, _raw_bytes(healthy, items=1))
        await worker.process_message(
            _job_msg(poisoned, b"not a gzip", num_delivered=PARSE_MAX_DELIVER)
        )
        await worker.process_message(_job_msg(healthy, _raw_bytes(healthy, items=1)))
        assert result_object_name(healthy) in _results(worker.client.jetstream).objects


# ---------------------------------------------------------------------------
# --drain (§12: seconds, 0 = run until signalled)
# ---------------------------------------------------------------------------


class TestDrain:
    """The worker stops after --drain seconds; 0 = until signalled."""

    async def test_drain_stops_the_worker(self) -> None:
        snapshot_id = "20260101T0000Z"
        gz = _raw_bytes(snapshot_id, items=1)
        sub = FakePullSubscription([_job_msg(snapshot_id, gz)])
        clock = FakeClock()
        worker = ParseWorker(
            client=FakeNATSClient(subscription=sub),
            drain_seconds=SPEC_DRAIN_SECONDS,
            clock=clock.clock,
            sleep=clock.sleep,
        )
        _seed_raw(worker.client.jetstream, snapshot_id, gz)
        await worker.run()
        assert worker.done == [snapshot_id]
        # The run returned (no raise) once the drain deadline passed.
        assert clock.now >= SPEC_DRAIN_SECONDS

    async def test_drain_zero_runs_until_signalled(self) -> None:
        sub = FakePullSubscription([])
        sub.block = True  # fetch never answers: nothing to work
        worker = ParseWorker(
            client=FakeNATSClient(subscription=sub),
            drain_seconds=0,
            clock=lambda: 0.0,
            sleep=_never,
        )
        task = asyncio.create_task(worker.run())
        await asyncio.sleep(0)  # let it subscribe and start fetching
        assert sub.fetch_calls
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


#: The fake parse's duration (fake seconds) — above two in-progress
#: ack cadences, so the ping task must have fired at least twice.
SLOW_PARSE_SECONDS = 25


async def _never(seconds: float) -> None:
    await asyncio.Future()