"""Tests: a consumer crash mid-stream resumes to the same output.

Pins docs/release3.project.v0.1.md §15 (the "a consumer dies" row,
E4-style): a durable consumer that dies mid-stream and is resumed
produces the SAME output as a clean run — the durable resumes from
its ack frontier, JetStream redelivers the unacknowledged
messages, and the consumer deduplicates by `event_id` against the
state that survived the crash (§6.2: the dedup lives in the
consumers, not in the stream). The failure is injected mid-handler.

Tests run against a fake NATS client (no network): the fake models
what JetStream guarantees — a durable's acked set survives the
consumer, the unacked messages come back.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from debian_observer.events.envelope import EventEnvelope
from debian_observer.idempotency import FileIdempotencyStore
from debian_observer.messaging.consumer import EventConsumer
from debian_observer.messaging.migrations import (
    MIGRATIONS_STREAM_NAME,
    MIGRATIONS_SUBJECT_FILTER,
)


# ---------------------------------------------------------------------------
# Events as the migration stream carries them
# ---------------------------------------------------------------------------


def _event(n: int) -> EventEnvelope:
    return EventEnvelope(
        event_id=uuid4(),
        event_type="ExcuseVerdictChanged",
        source="britney",
        entity_id=f"item-{n}",
        payload={"item_name": f"item-{n}"},
    )


STREAM_EVENTS = [_event(n) for n in range(6)]


# ---------------------------------------------------------------------------
# The persisted dedup state (what survives the crash)
# ---------------------------------------------------------------------------


class TestFileIdempotencyStore:
    """The seen `event_id`s persist beside the consumer."""

    def test_marks_survive_a_new_instance(self, tmp_path: Path) -> None:
        path = tmp_path / "seen.json"
        first = FileIdempotencyStore(path)
        first.mark_processed("evt-1")
        first.mark_processed("evt-2")
        resumed = FileIdempotencyStore(path)
        assert resumed.is_processed("evt-1")
        assert resumed.is_processed("evt-2")
        assert not resumed.is_processed("evt-3")

    def test_a_missing_file_is_a_fresh_store(self, tmp_path: Path) -> None:
        store = FileIdempotencyStore(tmp_path / "never-written.json")
        assert not store.is_processed("evt-1")
        store.mark_processed("evt-1")
        assert len(store) == 1

    def test_the_file_is_plain_json_of_the_seen_ids(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "seen.json"
        store = FileIdempotencyStore(path)
        store.mark_processed("evt-1")
        record = json.loads(path.read_text(encoding="utf-8"))
        assert record["seen"] == ["evt-1"]

    def test_uuid_and_str_names_are_the_same_event(
        self, tmp_path: Path
    ) -> None:
        # Envelopes carry UUID event ids; the store keys by their
        # canonical string — a UUID and its str() are one event.
        event_id = uuid4()
        store = FileIdempotencyStore(tmp_path / "seen.json")
        store.mark_processed(event_id)
        assert store.is_processed(str(event_id))
        resumed = FileIdempotencyStore(tmp_path / "seen.json")
        assert resumed.is_processed(str(event_id))

    def test_marking_writes_atomically(self, tmp_path: Path) -> None:
        # A crash mid-`mark_processed` never leaves a half-written
        # file: the write lands via a temp file and a rename.
        path = tmp_path / "seen.json"
        store = FileIdempotencyStore(path)
        store.mark_processed("evt-1")
        assert not list(tmp_path.glob("*.tmp"))


# ---------------------------------------------------------------------------
# A fake NATS: what JetStream guarantees a durable
# ---------------------------------------------------------------------------


class FakeMsg:
    """Stand-in NATS message: the envelope as JSON bytes."""

    def __init__(self, event: EventEnvelope) -> None:
        self.data = event.to_json_bytes()
        self.event = event
        self.acked = 0
        self.naked = 0

    async def ack(self) -> None:
        self.acked += 1

    async def nak(self) -> None:
        self.naked += 1


class FakeDurableJetStream:
    """Stand-in JetStream with durable semantics.

    What the crash test needs of JetStream (§15): a durable's
    acked set survives the consumer process; the messages the
    crashed consumer did not ack come back on the next delivery
    round.
    """

    def __init__(self, messages: list[FakeMsg]) -> None:
        self.messages = messages
        self.acked: set[str] = set()
        self.subscribe_calls: list[dict[str, Any]] = []

    async def subscribe(self, subject: str, **kwargs: Any) -> Any:
        self.subscribe_calls.append({"subject": subject, **kwargs})
        return kwargs.get("cb")

    async def deliver(self, cb: Any, msg: FakeMsg) -> None:
        """One delivery round for one message (at-least-once)."""
        try:
            await cb(msg)
        except Exception:
            # The consumer died mid-handler: the message is not
            # acked — it comes back on the next round.
            raise

    def pending(self) -> list[FakeMsg]:
        """The messages a resumed consumer still receives."""
        return [
            m
            for m in self.messages
            if str(m.event.event_id) not in self.acked
        ]


def _cb(js: FakeDurableJetStream) -> Any:
    """The consumer callback the fake JetStream subscribed."""
    return js.subscribe_calls[-1]["cb"]


def _crashing_handler(
    store: FileIdempotencyStore,
    processed: list[str],
    fail_on: str,
    acked: set[str] | None = None,
) -> Any:
    """A dedup handler that crashes on the event named `fail_on`."""

    async def _handler(
        msg: Any, event: EventEnvelope, payload: dict, ack: Any
    ) -> None:
        event_id = str(event.event_id)
        if event_id == fail_on:
            raise RuntimeError("injected failure mid-stream")
        if store.is_processed(event_id):
            await ack()  # a duplicate is acked, not reprocessed
            return
        processed.append(event_id)
        store.mark_processed(event_id)
        await ack()
        if acked is not None:
            acked.add(event_id)

    return _handler


class _FakeClient:
    """Stand-in NATSClient over the fake durable JetStream."""

    def __init__(self, js: FakeDurableJetStream) -> None:
        self._js = js
        self.connected = True

    @property
    def is_connected(self) -> bool:
        return self.connected

    @property
    def jetstream(self) -> FakeDurableJetStream:
        return self._js


def _consumer(client: _FakeClient, handler: Any) -> EventConsumer:
    return EventConsumer(
        client=client,
        handler=handler,
        stream_name=MIGRATIONS_STREAM_NAME,
        durable_name="analytics",
    )


# ---------------------------------------------------------------------------
# The crash: mid-stream death, resume, parity with a clean run
# ---------------------------------------------------------------------------


class TestConsumerCrashMidStream:
    """A dead durable resumes to the clean run's output."""

    async def test_clean_run_processes_every_event_once(
        self, tmp_path: Path
    ) -> None:
        js = FakeDurableJetStream([FakeMsg(e) for e in STREAM_EVENTS])
        client = _FakeClient(js)
        processed: list[str] = []

        async def _handler(
            msg: Any, event: EventEnvelope, payload: dict, ack: Any
        ) -> None:
            event_id = str(event.event_id)
            if event_id in processed:
                await ack()
                return
            processed.append(event_id)
            await ack()
            js.acked.add(event_id)

        consumer = _consumer(client, _handler)
        await consumer.subscribe(MIGRATIONS_SUBJECT_FILTER)
        for msg in js.messages:
            await js.deliver(_cb(js), msg)
        assert processed == [str(e.event_id) for e in STREAM_EVENTS]
        assert js.pending() == []

    async def test_a_crash_mid_stream_resumes_to_the_same_output(
        self, tmp_path: Path
    ) -> None:
        # The consumer dies on the 4th event (an injected failure),
        # after 3 events were acked. The resumed consumer — same
        # durable, dedup state that survived — processes what the
        # stream redelivers and its output EQUALS the clean run's.
        msgs = [FakeMsg(e) for e in STREAM_EVENTS]
        js = FakeDurableJetStream(msgs)
        client = _FakeClient(js)
        store_path = tmp_path / "seen.json"
        store = FileIdempotencyStore(store_path)
        processed: list[str] = []
        fail_id = str(STREAM_EVENTS[3].event_id)

        crashing = _crashing_handler(store, processed, fail_id)
        consumer = _consumer(client, crashing)
        await consumer.subscribe(MIGRATIONS_SUBJECT_FILTER)

        # The doomed run: 3 events process and ack, the 4th kills
        # the consumer (nak, then the process is gone).
        for msg in msgs[:3]:
            await js.deliver(_cb(js), msg)
        with pytest.raises(RuntimeError, match="injected failure"):
            await js.deliver(_cb(js), msgs[3])
        assert processed == [str(e.event_id) for e in STREAM_EVENTS[:3]]
        assert js.pending()  # the crash left messages unacked

        # The resumed consumer: same durable name, the dedup state
        # that survived the crash (the store's file).
        resumed_store = FileIdempotencyStore(store_path)
        resumed_processed: list[str] = []

        async def _handler(
            msg: Any, event: EventEnvelope, payload: dict, ack: Any
        ) -> None:
            event_id = str(event.event_id)
            if resumed_store.is_processed(event_id):
                await ack()
                js.acked.add(event_id)
                return
            resumed_processed.append(event_id)
            resumed_store.mark_processed(event_id)
            await ack()
            js.acked.add(event_id)

        resumed = _consumer(_FakeClient(js), _handler)
        await resumed.subscribe(MIGRATIONS_SUBJECT_FILTER)
        for msg in js.pending():
            await js.deliver(_cb(js), msg)

        # Parity: crash + resume == the clean run (the first three
        # came from the state that survived, the rest from the
        # redeliveries) — and nothing processed twice.
        full_output = processed + resumed_processed
        clean = [str(e.event_id) for e in STREAM_EVENTS]
        assert full_output == clean
        assert len(full_output) == len(set(full_output))
        assert js.pending() == []

    async def test_a_redelivered_acked_event_is_not_reprocessed(
        self, tmp_path: Path
    ) -> None:
        # The ack was lost in the crash but the effect was durable:
        # the redelivery is deduped by the state that survived and
        # acked again — the stream stops redelivering it.
        msgs = [FakeMsg(e) for e in STREAM_EVENTS[:2]]
        js = FakeDurableJetStream(msgs)
        store_path = tmp_path / "seen.json"
        store = FileIdempotencyStore(store_path)
        processed: list[str] = []
        handler = _crashing_handler(
            store, processed, fail_on="never", acked=js.acked
        )
        consumer = _consumer(_FakeClient(js), handler)
        await consumer.subscribe(MIGRATIONS_SUBJECT_FILTER)
        for msg in msgs:
            await js.deliver(_cb(js), msg)
            js.acked.add(str(msg.event.event_id))
        # A redelivery of both (ack lost):
        for msg in msgs:
            await js.deliver(_cb(js), msg)
        assert processed == [
            str(msgs[0].event.event_id),
            str(msgs[1].event.event_id),
        ]
        # Both deliveries acknowledged (two acks each: the
        # handler's and the consumer's default after it).
        assert msgs[0].acked == 2 * 2
