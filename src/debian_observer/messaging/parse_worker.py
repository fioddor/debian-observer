"""Messaging: the parse worker — work the queue politely.

The worker side of the parse work queue (ADR-027,
docs/release3.project.v0.1.md §11.2): a **durable pull** consumer of
``britney-parse`` — one durable, shared by the workers on every
machine that reaches the NATS server — whose every unit of work is
one job = one snapshot:

1. read the job's **raw object** from the Object Store bucket (§7.3)
   — the worker **never fetches from the source** (§4.1.3) — and
   verify its SHA-256 (ADR-002);
2. parse it — the snapshot parse of ``normalization.britney``
   (§9.2), the expensive unit worth distributing;
3. put the **compact state** back into the results bucket — one
   Parquet table per snapshot (§9.2, §10), atomic and keyed by
   snapshot, so a redelivered job overwrites with identical content
   (§11.2);
4. ack.

While a parse runs, the worker sends **in-progress acknowledgements**
every ~10 s (§11.2): a slow job keeps its delivery alive, a dead
worker's job still comes back after ``ack_wait`` (§15).
``max_ack_pending`` equals the worker's slots — ``--concurrency``,
backpressure — and ``max_deliver`` 5 bounds a poisoned snapshot: the
fifth delivery is acked away with a **dead-letter record** naming the
cause, and the queue continues (§15).

The pair eventizer stays sequential (ADR-027) and consumes the
compact states; it does not run here.
"""

from __future__ import annotations

import asyncio
import datetime
import io
import json
import platform
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable

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
    dead_letter_object_name,
    ensure_bucket,
    raw_object_name,
    result_object_name,
)
from debian_observer.normalization.britney import parse_snapshot
from debian_observer.observability.tracing import get_tracer

__all__ = [
    "FETCH_POLL_SECONDS",
    "FetchTimeout",
    "ParseRunStats",
    "ParseWorker",
    "compact_state_parquet_bytes",
]


# ---------------------------------------------------------------------------
# Errors and knobs
# ---------------------------------------------------------------------------


class FetchTimeout(Exception):
    """A pull batch came back empty: nothing to work right now.

    The worker's own stand-in for JetStream's fetch timeout — the
    run loop treats it (and the real ``FetchTimeoutError``, matched
    by name below) as a poll, never as a failure.
    """


#: Seconds one pull waits before coming back empty. A poll, not a
#: knob: it sizes how fast the worker notices --drain deadlines and
#: new jobs, and 1 s keeps both responsive without busy-spinning.
FETCH_POLL_SECONDS = 1.0


def _is_fetch_timeout(exc: BaseException) -> bool:
    """Is this exception an empty pull (in the battery or the wild)?

    The battery raises :class:`FetchTimeout`; nats-py raises
    ``nats.js.errors.FetchTimeoutError``. Matched by name so the
    module imports without nats-py and the loop treats both alike.
    """
    return "timeout" in type(exc).__name__.lower()


def _consumer_config(durable_name: str, slots: int) -> Any:
    """The durable pull's consumer configuration (§11.2).

    ``ack_wait`` above the worst-case parse under contention,
    ``max_ack_pending`` = the worker's slots (backpressure),
    ``max_deliver`` 5. Built with nats-py's ``ConsumerConfig`` when
    importable — this nats-py line takes ``ack_wait`` in **seconds**
    (it converts internally), never a timedelta — a plain stand-in
    otherwise (the battery's fakes read the same attributes).
    """
    try:
        from nats.js.api import ConsumerConfig

        return ConsumerConfig(
            durable_name=durable_name,
            ack_wait=PARSE_ACK_WAIT_SECONDS,
            max_ack_pending=slots,
            max_deliver=PARSE_MAX_DELIVER,
        )
    except ImportError:  # pragma: no cover - depends on nats-py
        class _Config:
            pass

        config = _Config()
        config.durable_name = durable_name
        config.ack_wait = PARSE_ACK_WAIT_SECONDS
        config.max_ack_pending = slots
        config.max_deliver = PARSE_MAX_DELIVER
        return config


# ---------------------------------------------------------------------------
# The compact state (§9.2, §10): one Parquet table per snapshot
# ---------------------------------------------------------------------------


def compact_state_parquet_bytes(parsed: Any) -> bytes:
    """Serialize one parsed snapshot as its compact Parquet table.

    The worker's result (§9.2): the ``ExcuseState`` columns plus the
    snapshot metadata — the pair eventizer's input, one table per
    snapshot (§10). Deterministic by construction: same bytes in,
    same Parquet bytes out, so a redelivered job overwrites the
    object with identical content (§11.2).
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    metadata = parsed.metadata
    states = parsed.states
    items = sorted(states)
    columns: dict[str, list[Any]] = {
        "item_name": items,
        "source_package": [states[name].source for name in items],
        "old_version": [states[name].old_version for name in items],
        "new_version": [states[name].new_version for name in items],
        "verdict": [states[name].verdict for name in items],
        "is_candidate": [states[name].is_candidate for name in items],
        "blocked_by": [json.dumps(list(states[name].blocked_by)) for name in items],
        "migrate_after": [
            json.dumps(list(states[name].migrate_after)) for name in items
        ],
        "reason": [json.dumps(list(states[name].reason)) for name in items],
        "snapshot": [metadata.snapshot_id] * len(items),
        "generated_date": [
            metadata.generated_date.isoformat() for _ in range(len(items))
        ],
        "sha256": [metadata.sha256] * len(items),
        "url": [metadata.url] * len(items),
        "item_count": [metadata.item_count] * len(items),
    }
    table = pa.table(columns)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


# ---------------------------------------------------------------------------
# The run statistics (§13: what the metrics and benchmarks need)
# ---------------------------------------------------------------------------


@dataclass
class ParseRunStats:
    """What one worker run did, as one JSON record.

    The surfaces §13 asks for — parse time per snapshot and host,
    redeliveries — plus the run's shape, so a benchmark run can file
    its numbers without re-measuring: the CLI echoes
    :meth:`to_json` and the ``benchmarks/`` drivers file it.
    """

    #: Snapshots parsed and acked (their ids are in ``parse_seconds``).
    jobs_done: int = 0
    #: Jobs nacked for redelivery (their worker is expected back).
    jobs_failed: int = 0
    #: Jobs acked away with a dead-letter record (§15).
    dead_lettered: int = 0
    #: Deliveries beyond the first, over all jobs (§13: redeliveries).
    redeliveries: int = 0
    #: Parse seconds per snapshot id (§13: parse time per snapshot).
    parse_seconds: dict[str, float] = field(default_factory=dict)
    #: Wall seconds of the run.
    wall_seconds: float = 0.0
    #: The host that parsed (§13: per snapshot **and host**).
    host: str = ""
    #: The worker's slots (§11.2 backpressure).
    concurrency: int = 1
    durable_name: str = PARSE_QUEUE_DURABLE
    stream_name: str = PARSE_STREAM_NAME
    raw_bucket: str = RAW_BUCKET
    results_bucket: str = RESULTS_BUCKET
    servers: tuple[str, ...] = ()

    def to_json(self) -> str:
        """The whole record as one JSON string (the CLI echo)."""
        return json.dumps(
            {
                "jobs_done": self.jobs_done,
                "jobs_failed": self.jobs_failed,
                "dead_lettered": self.dead_lettered,
                "redeliveries": self.redeliveries,
                "parse_seconds": dict(self.parse_seconds),
                "wall_seconds": self.wall_seconds,
                "host": self.host,
                "concurrency": self.concurrency,
                "durable_name": self.durable_name,
                "stream_name": self.stream_name,
                "raw_bucket": self.raw_bucket,
                "results_bucket": self.results_bucket,
                "servers": list(self.servers),
            },
            sort_keys=True,
        )


# ---------------------------------------------------------------------------
# The worker
# ---------------------------------------------------------------------------


class ParseWorker:
    """The durable pull that works the ``britney-parse`` queue.

    One instance is one machine's worker (or one process of several
    on one machine): all instances share the durable ``--durable``
    name, so the queue distributes the jobs across them and a dead
    one's jobs are redelivered to the others (§15).

    Usage::

        worker = ParseWorker(client=client, concurrency=4, drain_seconds=0)
        await worker.run()          # until signalled (cancel / SIGINT)
        worker.done                 # the snapshot ids parsed here
    """

    def __init__(
        self,
        *,
        client: Any,
        concurrency: int = 1,
        drain_seconds: int = 0,
        raw_bucket: str = RAW_BUCKET,
        results_bucket: str = RESULTS_BUCKET,
        durable_name: str = PARSE_QUEUE_DURABLE,
        parse: Callable[[str, bytes], Any] = parse_snapshot,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        tracer: Any = None,
    ) -> None:
        self.client = client
        self.concurrency = concurrency
        #: Spec §12: --drain S, integer seconds (0 = run until
        #: signalled).
        self.drain_seconds = drain_seconds
        self.raw_bucket = raw_bucket
        self.results_bucket = results_bucket
        self.durable_name = durable_name
        #: The snapshot ids this worker parsed and acked.
        self.done: list[str] = []
        self._parse = parse
        self._clock = clock
        self._sleep = sleep
        self._tracer = tracer if tracer is not None else get_tracer(
            "debian_observer.messaging"
        )
        #: The host that parses (§13: spans and stats carry it).
        self._host = platform.node()
        # Run tallies (§13): surfaced by ``run`` as ParseRunStats.
        self._jobs_failed = 0
        self._dead_lettered = 0
        self._redeliveries = 0
        self._parse_seconds: dict[str, float] = {}

    # -- the pull ------------------------------------------------------------

    async def _subscribe(self) -> Any:
        """Attach the shared durable pull (§11.2: one durable, pull)."""
        js = self.client.jetstream
        return await js.pull_subscribe(
            PARSE_SUBJECT,
            durable=self.durable_name,
            stream=PARSE_STREAM_NAME,
            config=_consumer_config(self.durable_name, self.concurrency),
        )

    async def run(self) -> ParseRunStats:
        """Work the queue until the drain deadline or a signal.

        ``--drain S`` seconds (``0`` = until the task is cancelled —
        the CLI turns SIGINT into that). Each pull brings at most the
        worker's slots in flight (``max_ack_pending`` keeps the
        server side honest, the batch size keeps this side honest).
        Returns the run's statistics (§13).
        """
        subscription = await self._subscribe()
        start = self._clock()
        while True:
            if self.drain_seconds and (self._clock() - start) >= self.drain_seconds:
                break
            try:
                messages = await subscription.fetch(
                    self.concurrency, timeout=FETCH_POLL_SECONDS
                )
            except Exception as exc:
                if _is_fetch_timeout(exc):
                    await self._sleep(FETCH_POLL_SECONDS)
                    continue
                raise
            await asyncio.gather(
                *(self.process_message(message) for message in messages)
            )
        try:
            await subscription.unsubscribe()
        except Exception:
            pass  # already gone or never fully attached
        return self._stats(wall_seconds=self._clock() - start)

    def _stats(self, *, wall_seconds: float) -> ParseRunStats:
        """The run's tallies as one record (§13)."""
        servers = tuple(
            self.client.servers
            if hasattr(self.client, "servers") and self.client.servers is not None
            else ()
        )
        return ParseRunStats(
            jobs_done=len(self.done),
            jobs_failed=self._jobs_failed,
            dead_lettered=self._dead_lettered,
            redeliveries=self._redeliveries,
            parse_seconds=dict(self._parse_seconds),
            wall_seconds=wall_seconds,
            host=self._host,
            concurrency=self.concurrency,
            durable_name=self.durable_name,
            stream_name=PARSE_STREAM_NAME,
            raw_bucket=self.raw_bucket,
            results_bucket=self.results_bucket,
            servers=servers,
        )

    # -- one job -------------------------------------------------------------

    async def process_message(self, message: Any) -> None:
        """Work one job: raw object in, compact state out, ack (§11.2).

        A failure naks for redelivery — after ``max_deliver``
        deliveries the job is acked away with a dead-letter record
        and the queue continues (§15). One span per parse job carries
        the snapshot and this worker's host (§13).
        """
        deliveries = getattr(
            getattr(message, "metadata", None), "num_delivered", 1
        )
        self._redeliveries += max(0, deliveries - 1)
        job: ParseJob | None = None
        error: BaseException | None = None
        with self._tracer.start_as_current_span("messaging.parse_job") as span:
            span.set_attribute("worker.host", self._host)
            span.set_attribute("parse.deliveries", deliveries)
            try:
                job = ParseJob.from_json_bytes(message.data)
                span.set_attribute("parse.snapshot", job.snapshot_id)
                span.set_attribute("parse.job_id", str(job.job_id))
                raw = await self._read_raw(job)
                parse_start = self._clock()
                parsed = await self._parse_with_acks(message, job, raw)
                self._parse_seconds[job.snapshot_id] = (
                    self._clock() - parse_start
                )
                await self._put_state(job, parsed)
            except Exception as exc:
                error = exc
                span.record_exception(exc)
        if error is None:
            self.done.append(job.snapshot_id)
            await self._ack(message)
            return
        await self._fail(message, job, error)

    async def _read_raw(self, job: ParseJob) -> bytes:
        """Read the job's raw object from the bucket, sha-verified.

        The worker reads the raw bytes from the same NATS it already
        uses (§7.3) — never from the source (§4.1.3) — and verifies
        them against the job's SHA-256 (ADR-002): it will not parse
        bytes it cannot vouch for.
        """
        store = await ensure_bucket(self.client, self.raw_bucket)
        result = await store.get(job.raw_object)
        raw = result.data
        if sha256_bytes(raw) != job.sha256:
            raise ValueError(
                f"raw object {job.raw_object} of snapshot "
                f"{job.snapshot_id} fails its job sha256"
            )
        return raw

    async def _parse_with_acks(self, message: Any, job: ParseJob, raw: bytes) -> Any:
        """Parse one snapshot while keeping the delivery alive.

        In-progress acks fire every ~10 s (:data:`IN_PROGRESS_ACK_SECONDS`)
        from a ping task beside the parse; the parse itself runs in a
        thread (``asyncio.to_thread``) so the loop stays free to send
        them (§11.2).
        """
        finished = asyncio.Event()

        async def _ping() -> None:
            while not finished.is_set():
                await self._sleep(IN_PROGRESS_ACK_SECONDS)
                if finished.is_set():
                    return
                try:
                    await message.in_progress()
                except Exception:
                    return  # the delivery is gone; the parse decides

        ping = asyncio.create_task(_ping())
        try:
            return await self._parse_job(job, raw)
        finally:
            finished.set()
            ping.cancel()
            try:
                await ping
            except (asyncio.CancelledError, Exception):
                pass

    async def _parse_job(self, job: ParseJob, raw: bytes) -> Any:
        """The snapshot parse (§9.2), off the event loop."""
        return await asyncio.to_thread(self._parse, job.snapshot_id, raw)

    async def _put_state(self, job: ParseJob, parsed: Any) -> None:
        """Put the compact state back, keyed by snapshot (§11.2).

        Atomic: an object ``put`` replaces whole — a redelivered job
        overwrites with identical content.
        """
        store = await ensure_bucket(self.client, self.results_bucket)
        await store.put(
            result_object_name(job.snapshot_id),
            compact_state_parquet_bytes(parsed),
        )

    async def _fail(
        self, message: Any, job: ParseJob | None, error: BaseException
    ) -> None:
        """A job that will not work: redeliver, or dead-letter at max.

        ``max_deliver`` deliveries is the bound (§15): the fifth
        failure is acked away with a dead-letter record in the
        results bucket, so a poisoned snapshot does not loop forever
        and the queue continues.
        """
        deliveries = getattr(getattr(message, "metadata", None), "num_delivered", 1)
        if deliveries < PARSE_MAX_DELIVER:
            self._jobs_failed += 1
            try:
                await message.nak()
            except Exception:
                pass  # the delivery is already gone; redelivery decides
            return
        self._dead_lettered += 1
        snapshot_id = job.snapshot_id if job is not None else "unknown"
        record = {
            "snapshot_id": snapshot_id,
            "raw_object": job.raw_object if job is not None else None,
            "deliveries": deliveries,
            "error": f"{type(error).__name__}: {error}",
            "dead_lettered_at": datetime.datetime.now(
                datetime.timezone.utc
            ).isoformat(),
        }
        store = await ensure_bucket(self.client, self.results_bucket)
        await store.put(
            dead_letter_object_name(snapshot_id),
            json.dumps(record, sort_keys=True).encode("utf-8"),
        )
        await self._ack(message)

    async def _ack(self, message: Any) -> None:
        """Ack, tolerating a delivery that is already gone."""
        try:
            await message.ack()
        except Exception:
            pass