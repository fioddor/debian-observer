"""Tests: measure the distributed parse.

Pins the worker's run statistics and its tracing
(docs/release3.project.v0.1.md §13, §16): every parse job opens an
OpenTelemetry span carrying the **worker host** as an attribute (the
traces cross machines), and a run surfaces what the benchmarks and
the metrics need:

* parse time **per snapshot** (and the host that parsed it);
* jobs done, jobs failed (redelivery-seeking), dead-lettered;
* **redeliveries** (deliveries beyond the first per job);
* wall seconds of the run, and the run's shape (concurrency, durable,
  stream, buckets, servers) — one JSON record the CLI echoes and the
  benchmarks file.

Tests run against fakes and an injected clock: no network, no real
waiting.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import platform
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from debian_observer.ingestion.britney import sha256_bytes
from debian_observer.messaging.parse_queue import (
    PARSE_MAX_DELIVER,
    PARSE_QUEUE_DURABLE,
    PARSE_STREAM_NAME,
    RAW_BUCKET,
    RESULTS_BUCKET,
    ParseJob,
    raw_object_name,
)
from debian_observer.messaging.parse_worker import ParseRunStats, ParseWorker

from tests.messaging.test_parse_worker import (  # the shared fakes
    FakeClock,
    FakeMsg,
    FakeNATSClient,
    FakeObjectStore,
    FakePullSubscription,
    _raw_bytes,
    _seed_raw,
)

# ---------------------------------------------------------------------------
# A fake tracer: spans recorded, attributes kept (no exporter)
# ---------------------------------------------------------------------------


class FakeSpan:
    """Stand-in OpenTelemetry span: attributes kept on it."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.attributes: dict[str, Any] = {}

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def set_status(self, *args: Any) -> None:
        pass

    def record_exception(self, *args: Any) -> None:
        pass


class FakeTracer:
    """Stand-in tracer: every span recorded for the asserts."""

    def __init__(self) -> None:
        self.spans: list[FakeSpan] = []

    @contextmanager
    def start_as_current_span(self, name: str, **kwargs: Any):
        span = FakeSpan(name)
        self.spans.append(span)
        try:
            yield span
        finally:
            pass


def _job_msg(
    snapshot_id: str, gz: bytes, num_delivered: int = 1
) -> FakeMsg:
    job = ParseJob(
        snapshot_id=snapshot_id,
        raw_object=raw_object_name(snapshot_id),
        sha256=sha256_bytes(gz),
        url=None,
    )
    return FakeMsg(job.to_json_bytes(), num_delivered=num_delivered)


def _worker_with(
    msgs: list[FakeMsg], clock: FakeClock, drain: int, *, concurrency: int = 1, **kwargs: Any
) -> ParseWorker:
    sub = FakePullSubscription(msgs)
    worker = ParseWorker(
        client=FakeNATSClient(subscription=sub),
        concurrency=concurrency,
        drain_seconds=drain,
        clock=clock.clock,
        sleep=clock.sleep,
        **kwargs,
    )
    js = worker.client.jetstream
    for msg in msgs:
        snapshot_id = json.loads(msg.data)["snapshot_id"]
        if snapshot_id != "poisoned":
            _seed_raw(js, snapshot_id, _raw_bytes(snapshot_id, items=1))
    return worker


# ---------------------------------------------------------------------------
# The run statistics (§13/§16: what the metrics and benchmarks need)
# ---------------------------------------------------------------------------


class TestParseRunStats:
    """A run surfaces per-snapshot parse times, counts and shape."""

    async def test_a_run_measures_each_parse(self) -> None:
        first, second = "20260101T0000Z", "20260101T0100Z"
        msgs = [
            _job_msg(first, _raw_bytes(first, items=1)),
            _job_msg(second, _raw_bytes(second, items=1)),
        ]
        clock = FakeClock()
        # One pull brings both jobs (the in-progress ping busy-loops
        # the fake clock forward, so the drain deadline is checked
        # only between pulls).
        worker = _worker_with(msgs, clock, drain=5, concurrency=2)
        stats = await worker.run()
        assert isinstance(stats, ParseRunStats)
        assert stats.jobs_done == 2
        assert stats.jobs_failed == 0
        assert stats.dead_lettered == 0
        assert stats.redeliveries == 0
        # Parse time per snapshot (§13), the host that parsed it.
        assert set(stats.parse_seconds) == {first, second}
        assert all(t >= 0 for t in stats.parse_seconds.values())
        assert stats.wall_seconds >= 0
        assert stats.host == platform.node()
        assert stats.host
        # The run's shape: one shared durable on the queue stream.
        assert stats.concurrency == 2  # the slots this run pulled with
        assert stats.durable_name == PARSE_QUEUE_DURABLE
        assert stats.stream_name == PARSE_STREAM_NAME
        assert stats.raw_bucket == RAW_BUCKET
        assert stats.results_bucket == RESULTS_BUCKET
        assert stats.servers == ("nats://localhost:4222",)

    async def test_redeliveries_are_counted(self) -> None:
        # A redelivered job (its first worker died) carries its
        # delivery count: the third delivery is two redeliveries.
        snapshot_id = "20260101T0000Z"
        msgs = [
            _job_msg(snapshot_id, _raw_bytes(snapshot_id, items=1), num_delivered=3),
        ]
        clock = FakeClock()
        worker = _worker_with(msgs, clock, drain=5)
        stats = await worker.run()
        assert stats.redeliveries == 2
        assert stats.jobs_done == 1

    async def test_failures_and_dead_letters_are_counted(self) -> None:
        poisoned = "poisoned"
        good = "20260101T0100Z"
        msgs = [
            _job_msg(poisoned, b"not a gzip", num_delivered=PARSE_MAX_DELIVER),
            _job_msg(good, _raw_bytes(good, items=1)),
        ]
        clock = FakeClock()
        worker = _worker_with(msgs, clock, drain=5, concurrency=2)
        stats = await worker.run()
        assert stats.dead_lettered == 1
        assert stats.jobs_done == 1
        assert poisoned not in stats.parse_seconds

    def test_stats_serialize_to_one_json_record(self) -> None:
        stats = ParseRunStats(
            jobs_done=2,
            jobs_failed=1,
            dead_lettered=1,
            redeliveries=2,
            parse_seconds={"20260101T0000Z": 10.5},
            wall_seconds=25.0,
            host="worker-1",
        )
        record = json.loads(stats.to_json())
        assert record["jobs_done"] == 2
        assert record["jobs_failed"] == 1
        assert record["dead_lettered"] == 1
        assert record["redeliveries"] == 2
        assert record["parse_seconds"] == {"20260101T0000Z": 10.5}
        assert record["wall_seconds"] == 25.0
        assert record["host"] == "worker-1"


# ---------------------------------------------------------------------------
# Tracing (§13: a span per parse job, the worker host as an attribute)
# ---------------------------------------------------------------------------


class TestParseJobSpans:
    """Each parse job opens a span carrying its snapshot and host."""

    async def test_each_parse_job_opens_a_span_with_the_host(self) -> None:
        tracer = FakeTracer()
        snapshot_id = "20260101T0000Z"
        gz = _raw_bytes(snapshot_id, items=1)
        worker = ParseWorker(client=FakeNATSClient(), tracer=tracer)
        _seed_raw(worker.client.jetstream, snapshot_id, gz)
        await worker.process_message(_job_msg(snapshot_id, gz))
        parse_spans = [s for s in tracer.spans if s.name == "messaging.parse_job"]
        assert len(parse_spans) == 1
        span = parse_spans[0]
        assert span.attributes["parse.snapshot"] == snapshot_id
        assert span.attributes["worker.host"] == platform.node()

    async def test_a_failed_job_opens_its_span_too(self) -> None:
        tracer = FakeTracer()
        worker = ParseWorker(client=FakeNATSClient(), tracer=tracer)
        msg = _job_msg("20260101T0000Z", _raw_bytes("20260101T0000Z"))
        # No raw object seeded: the job fails (redelivery), and the
        # span still opened — one per parse job, done or not.
        await worker.process_message(msg)
        assert [s.name for s in tracer.spans].count("messaging.parse_job") == 1