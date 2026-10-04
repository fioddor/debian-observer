"""pytest-bdd step definitions for reliability feature files.

Adapts the raw-branch BDD scenarios to the current ConsumerApp / IdempotencyStore
API.  Steps are wired so they will PASS once the implementation behaves as
specified, but may FAIL now if the code under test deviates from expected
behaviour.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenario, then, when

from debian_observer.consumer_app import ConsumerApp
from debian_observer.events.envelope import EventEnvelope
from debian_observer.idempotency import IdempotencyStore

# =============================================================================
# Scenarios — Consumer Restart Recovery
# =============================================================================


@scenario("../consumer_restart_recovery.feature", "Clean shutdown and restart")
def test_clean_shutdown_and_restart() -> None:
    """Run: Clean shutdown and restart."""


@scenario(
    "../consumer_restart_recovery.feature",
    "Crash during processing with unacknowledged message",
)
def test_crash_during_processing_with_unacknowledged_message() -> None:
    """Run: Crash during processing with unacknowledged message."""


@scenario(
    "../consumer_restart_recovery.feature",
    "Replay from beginning after idempotency store reset",
)
def test_replay_from_beginning_after_idempotency_store_reset() -> None:
    """Run: Replay from beginning after idempotency store reset."""


@scenario(
    "../consumer_restart_recovery.feature",
    "Simultaneous duplicate delivery and restart",
)
def test_simultaneous_duplicate_delivery_and_restart() -> None:
    """Run: Simultaneous duplicate delivery and restart."""


# =============================================================================
# Scenarios — Event Processing
# =============================================================================


@scenario("../event_processing.feature", "Idempotent consumer processing")
def test_idempotent_consumer_processing() -> None:
    """Run: Idempotent consumer processing."""


@scenario("../event_processing.feature", "Consumer crashes mid-processing")
def test_consumer_crashes_mid_processing() -> None:
    """Run: Consumer crashes mid-processing."""


# =============================================================================
# Helpers
# =============================================================================


@dataclass
class TrackingHandler:
    """Test double that records every envelope it receives."""

    events_seen: list[str] = field(default_factory=list)
    crash_after: int | None = None
    acks: list[str] = field(default_factory=list)

    def __call__(self, envelope: EventEnvelope) -> None:
        if (
            self.crash_after is not None
            and len(self.events_seen) + 1 == self.crash_after
        ):
            raise RuntimeError(f"Simulated crash on event {self.crash_after}")
        self.events_seen.append(str(envelope.event_id))

    def ack(self, event_id: str) -> None:
        self.acks.append(event_id)

    def was_processed(self, event_id: str) -> bool:
        return event_id in self.events_seen

    def was_acked(self, event_id: str) -> bool:
        return event_id in self.acks

    @property
    def unique_count(self) -> int:
        return len(set(self.events_seen))


def _make_envelope(
    index: int, event_type: str = "PackageVersionObserved"
) -> EventEnvelope:
    """Build a deterministic EventEnvelope for test stability."""
    return EventEnvelope(
        event_id=uuid.UUID(int=index),
        event_type=event_type,
        source="udd",
        observed_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        effective_at=datetime(2025, 12, 31, 0, 0, 0, tzinfo=timezone.utc),
        entity_id=f"package:pkg-{index}",
        payload={"version": f"1.0.{index}"},
    )


def _process_with_ack_tracking(
    app: ConsumerApp,
    handler: TrackingHandler,
    envelope: EventEnvelope,
    bdd_context: dict[str, Any],
) -> bool:
    """Call app.process and track the implicit ack when it returns True."""
    result = app.process(envelope)
    if result:
        handler.ack(str(envelope.event_id))
    return result


# =============================================================================
# Shared fixtures / Given steps
# =============================================================================


@pytest.fixture
def bdd_context() -> dict[str, Any]:
    """Mutable context dict shared across steps in a single scenario."""
    return {}


@given("a consumer app with a tracking handler")
def given_consumer_app_with_tracking_handler(bdd_context: dict[str, Any]) -> None:
    """Initialise a ConsumerApp and a handler that records every event."""
    handler = TrackingHandler()
    app = ConsumerApp()
    app.register_handler("PackageVersionObserved", handler)
    bdd_context["handler"] = handler
    bdd_context["app"] = app
    bdd_context["store"] = app.idempotency_store


@given("the consumer has an idempotency store backed by in-memory storage")
def given_persistent_idempotency(bdd_context: dict[str, Any]) -> None:
    """Ensure the app's store is available in context."""
    # Already set by given_consumer_app_with_tracking_handler; noop here.
    pass


@given(parsers.parse('a durable stream named "{stream_name}" exists'))
def given_durable_stream(bdd_context: dict[str, Any], stream_name: str) -> None:
    """Stream exists for publishing events (conceptual only in unit tests)."""
    bdd_context["stream_name"] = stream_name


# =============================================================================
# Given steps — event preparation
# =============================================================================


@given(parsers.parse("{count:d} events published to the stream"))
def given_events_published(bdd_context: dict[str, Any], count: int) -> None:
    """Create N deterministic EventEnvelope objects."""
    events = [_make_envelope(i) for i in range(count)]
    bdd_context["events"] = events


@given(parsers.parse("{count:d} events already processed and acknowledged"))
def given_n_events_already_processed(bdd_context: dict[str, Any], count: int) -> None:
    """Pre-load idempotency store and handler with historical state."""
    app: ConsumerApp = bdd_context["app"]
    handler: TrackingHandler = bdd_context["handler"]
    events = [_make_envelope(i) for i in range(count)]
    bdd_context["events"] = events
    for ev in events:
        app.process(ev)
    # Verify preconditions
    assert len(handler.events_seen) == count
    bdd_context["pre_replay_count"] = handler.unique_count


@given(parsers.parse("the consumer has processed {count:d} events"))
def given_consumer_processed_n(bdd_context: dict[str, Any], count: int) -> None:
    """Pre-process N events through the consumer app."""
    app: ConsumerApp = bdd_context["app"]
    events = bdd_context["events"]
    for ev in events[:count]:
        app.process(ev)
    bdd_context["processed_before_shutdown"] = count


@given(parsers.parse("the consumer is processing event {n:d}"))
def given_consumer_processing_event_n(bdd_context: dict[str, Any], n: int) -> None:
    """Set up context pointing at the Nth event (1-indexed)."""
    events = bdd_context["events"]
    bdd_context["current_event"] = events[n - 1]
    bdd_context["current_event_index"] = n


@given("the consumer crashes before marking event 5 as processed")
def given_consumer_crashes_before_mark(bdd_context: dict[str, Any]) -> None:
    """Simulate crash before idempotency mark.

    Process events 1-4 normally, then crash on event 5 before mark_processed.
    """
    app: ConsumerApp = bdd_context["app"]
    events = bdd_context["events"]
    current_event = events[4]  # event 5 (1-indexed)

    # Process events 1-4 normally
    for ev in events[:4]:
        app.process(ev)

    # Now attempt event 5 with a crashing handler
    crashing_handler = TrackingHandler(crash_after=1)
    app.register_handler("PackageVersionObserved", crashing_handler)
    try:
        app.process(current_event)
    except RuntimeError:
        pass  # Expected crash before mark_processed
    bdd_context["crashed_before_mark"] = True


@given("event A published to the stream")
def given_event_a_published(bdd_context: dict[str, Any]) -> None:
    """Publish a single canonical event."""
    envelope = _make_envelope(42, event_type="PackageVersionObserved")
    bdd_context["event_a"] = envelope


@given("the consumer processes event A and marks it processed")
@when("the consumer processes event A and marks it processed")
def given_consumer_processes_and_marks_a(bdd_context: dict[str, Any]) -> None:
    """Fully process event A."""
    app: ConsumerApp = bdd_context["app"]
    event_a = bdd_context["event_a"]
    app.process(event_a)


@given("the consumer crashes immediately after marking")
@when("the consumer crashes immediately after marking")
def given_consumer_crashes_after_mark(bdd_context: dict[str, Any]) -> None:
    """Crash after mark but before full state flush (conceptual)."""
    # In-memory state is already marked; we simulate this by noting the crash.
    bdd_context["crashed_after_mark"] = True


@given("the stream redelivers event A due to slow propagation")
@when("the stream redelivers event A due to slow propagation")
def given_stream_redeliver_a(bdd_context: dict[str, Any]) -> None:
    """Simulate redelivery by placing the same envelope back in context."""
    bdd_context["redelivered_event"] = bdd_context["event_a"]


@given(parsers.parse('a "{event_type}" event in the stream'))
def given_event_in_stream(bdd_context: dict[str, Any], event_type: str) -> None:
    """Prepare a sample event."""
    envelope = _make_envelope(1, event_type=event_type)
    bdd_context["published_event"] = envelope


@given(parsers.parse("a batch of {count:d} events in the stream"))
def given_batch_of_events(bdd_context: dict[str, Any], count: int) -> None:
    """Create N synthetic events."""
    events = [_make_envelope(i) for i in range(count)]
    bdd_context["batch_events"] = events


@given(parsers.parse("the consumer is configured to crash on the {nth:d}th event"))
def given_consumer_configured_crash(bdd_context: dict[str, Any], nth: int) -> None:
    """Configure handler to raise after processing N events."""
    app: ConsumerApp = bdd_context["app"]
    handler: TrackingHandler = bdd_context["handler"]
    handler.crash_after = nth
    # Re-register so the same handler object is used
    app.register_handler("PackageVersionObserved", handler)
    bdd_context["crash_after"] = nth


@given("the idempotency store is intentionally reset")
@when("the idempotency store is intentionally reset")
def given_idempotency_reset(bdd_context: dict[str, Any]) -> None:
    """Reset to simulate replay scenario."""
    store: IdempotencyStore = bdd_context["store"]
    store.reset()


# =============================================================================
# When steps
# =============================================================================


@when("the consumer receives shutdown signal")
def when_consumer_shutdown(bdd_context: dict[str, Any]) -> None:
    """Initiate graceful shutdown."""
    app: ConsumerApp = bdd_context["app"]
    app.shutdown()
    bdd_context["shutdown_complete"] = True


@when("the consumer restarts with the same idempotency store")
def when_consumer_restarts_with_same_store(bdd_context: dict[str, Any]) -> None:
    """Launch new consumer app with the pre-existing store.

    Processes all events; the store skips already-processed ones.
    """
    store: IdempotencyStore = bdd_context["store"]
    handler = TrackingHandler()
    app = ConsumerApp(idempotency_store=store)
    app.register_handler("PackageVersionObserved", handler)
    for ev in bdd_context.get("events", []):
        app.process(ev)
    bdd_context["restarted_app"] = app
    bdd_context["restarted_handler"] = handler


@when("the consumer restarts with fresh state")
def when_consumer_restarts_fresh(bdd_context: dict[str, Any]) -> None:
    """Launch new consumer app with a brand-new idempotency store."""
    handler = TrackingHandler()
    app = ConsumerApp()
    app.register_handler("PackageVersionObserved", handler)
    for ev in bdd_context.get("events", []):
        app.process(ev)
    bdd_context["restarted_app"] = app
    bdd_context["restarted_handler"] = handler


@when("event A is received again")
@then("event A is received again")
def when_event_a_received_again(bdd_context: dict[str, Any]) -> None:
    """Redelivered event enters restarted consumer."""
    app: ConsumerApp = bdd_context.get("restarted_app") or bdd_context["app"]
    event = bdd_context["redelivered_event"]
    result = app.process(event)
    bdd_context["redelivery_result"] = result


@when(parsers.parse("a replay command replays all {count:d} events"))
def when_replay_all(bdd_context: dict[str, Any], count: int) -> None:
    """Replay all events from beginning through current app."""
    app: ConsumerApp = bdd_context["app"]
    events = bdd_context["events"]
    for ev in events:
        app.process(ev)
    bdd_context["replayed_count"] = count


@when("the consumer processes it for the first time")
def when_consumer_processes_first_time(bdd_context: dict[str, Any]) -> None:
    """First delivery attempt."""
    app: ConsumerApp = bdd_context["app"]
    envelope = bdd_context["published_event"]
    result = app.process(envelope)
    bdd_context["first_result"] = result


@when("the same event is redelivered")
def when_same_event_redelivered(bdd_context: dict[str, Any]) -> None:
    """Simulate NATS redelivery by processing the same envelope again."""
    app: ConsumerApp = bdd_context["app"]
    envelope = bdd_context["published_event"]
    result = app.process(envelope)
    bdd_context["second_result"] = result


@when("processing starts")
def when_processing_starts(bdd_context: dict[str, Any]) -> None:
    """Start batch consumption."""
    app: ConsumerApp = bdd_context["app"]
    handler: TrackingHandler = bdd_context["handler"]
    events = bdd_context["batch_events"]
    try:
        for ev in events:
            result = app.process(ev)
            if result:
                handler.ack(str(ev.event_id))
        bdd_context["batch_complete"] = True
    except RuntimeError:
        bdd_context["batch_complete"] = False


@when("the consumer recovers and restarts")
def when_consumer_recovers(bdd_context: dict[str, Any]) -> None:
    """Simulate restart with fresh consumer but shared idempotency."""
    store: IdempotencyStore = bdd_context["store"]
    recovered_handler = TrackingHandler()
    app = ConsumerApp(idempotency_store=store)
    app.register_handler("PackageVersionObserved", recovered_handler)
    events = bdd_context["batch_events"]
    for ev in events:
        result = app.process(ev)
        if result:
            recovered_handler.ack(str(ev.event_id))
    bdd_context["recovered_app"] = app
    bdd_context["recovered_handler"] = recovered_handler


# =============================================================================
# Then steps — Clean shutdown and restart
# =============================================================================


@then("it flushes its idempotency store state")
def then_flushes_idempotency(bdd_context: dict[str, Any]) -> None:
    """Graceful shutdown must leave store populated."""
    store: IdempotencyStore = bdd_context["store"]
    assert len(store) > 0, "Idempotency store should have entries after shutdown"


@then("all processed events are tracked")
def then_all_processed_events_tracked(bdd_context: dict[str, Any]) -> None:
    """Handler should have seen events processed before shutdown."""
    handler: TrackingHandler = bdd_context["handler"]
    processed_before = bdd_context.get("processed_before_shutdown", 0)
    assert len(handler.events_seen) == processed_before


@then("it resumes knowing 6 events are already processed")
def then_resumes_knowing_six(bdd_context: dict[str, Any]) -> None:
    """Restarted store should still know about pre-shutdown events."""
    store: IdempotencyStore = bdd_context["store"]
    events = bdd_context["events"]
    for ev in events[:6]:
        assert store.is_processed(ev.event_id) is True


@then("the remaining 4 events are processed by the handler")
def then_remaining_processed(bdd_context: dict[str, Any]) -> None:
    """All pending events handled after restart."""
    handler: TrackingHandler = bdd_context["restarted_handler"]
    events = bdd_context["events"]
    for ev in events[6:]:
        assert handler.was_processed(str(ev.event_id))


@then("no duplicate processing occurs")
def then_no_duplicates(bdd_context: dict[str, Any]) -> None:
    """Idempotency must prevent duplicates."""
    handler: TrackingHandler = bdd_context["restarted_handler"]
    events = bdd_context["events"]
    for ev in events[:6]:
        assert not handler.was_processed(str(ev.event_id))


# =============================================================================
# Then steps — Crash during processing
# =============================================================================


@then("event 5 is redelivered to the handler")
def then_event_5_redelivered(bdd_context: dict[str, Any]) -> None:
    """Unmarked event should be processed by restarted handler."""
    restarted_handler: TrackingHandler = bdd_context["restarted_handler"]
    event_5 = bdd_context["events"][4]
    assert restarted_handler.was_processed(str(event_5.event_id))


@then("the idempotency store prevents double-processing after mark")
def then_prevents_double_processing_after_mark(bdd_context: dict[str, Any]) -> None:
    """Once the restarted consumer marks it, duplicates are rejected."""
    restarted_app: ConsumerApp = bdd_context["restarted_app"]
    event_5 = bdd_context["events"][4]
    # Re-process the same event
    result = restarted_app.process(event_5)
    assert result is True  # ConsumerApp returns True for already-processed
    restarted_handler: TrackingHandler = bdd_context["restarted_handler"]
    # Handler should only have seen it once (from the restart, not twice)
    assert restarted_handler.events_seen.count(str(event_5.event_id)) == 1


@then(parsers.parse("events {start:d}–{end:d} are processed in order"))
def then_events_processed_in_order(
    bdd_context: dict[str, Any], start: int, end: int
) -> None:
    """Sequential processing after resume."""
    restarted_handler: TrackingHandler = bdd_context["restarted_handler"]
    events = bdd_context["events"]
    for ev in events[start - 1 : end]:
        assert restarted_handler.was_processed(str(ev.event_id))


# =============================================================================
# Then steps — Replay after reset
# =============================================================================


@then("every event is re-evaluated by the handler")
def then_every_event_re_evaluated(bdd_context: dict[str, Any]) -> None:
    """Replay must touch every message — handler sees them twice (original + replay)."""
    handler: TrackingHandler = bdd_context["handler"]
    events = bdd_context["events"]
    for ev in events:
        assert handler.events_seen.count(str(ev.event_id)) == 2


@then("the idempotency store repopulates with all 20 events")
def then_store_repopulates(bdd_context: dict[str, Any]) -> None:
    """All events back in ledger."""
    store: IdempotencyStore = bdd_context["store"]
    events = bdd_context["events"]
    for ev in events:
        assert store.is_processed(ev.event_id) is True


@then("the final processed count matches the pre-replay state")
def then_state_matches_pre_replay(bdd_context: dict[str, Any]) -> None:
    """Replay must be a safe idempotent operation."""
    handler: TrackingHandler = bdd_context["handler"]
    pre_replay = bdd_context["pre_replay_count"]
    assert handler.unique_count == pre_replay


# =============================================================================
# Then steps — Simultaneous duplicate delivery and restart
# =============================================================================


@then("the idempotency store rejects it as duplicate")
def then_store_rejects_redelivery(bdd_context: dict[str, Any]) -> None:
    """Duplicate rejected."""
    store: IdempotencyStore = bdd_context["store"]
    event_a = bdd_context["redelivered_event"]
    assert store.is_processed(event_a.event_id) is True


@then("handler state remains unchanged")
def then_state_unchanged(bdd_context: dict[str, Any]) -> None:
    """No mutation on duplicate.

    The original handler processed event A once before the crash.
    After restart, the duplicate was rejected by idempotency, so the
    restarted handler should never have seen event A.
    """
    original_handler: TrackingHandler = bdd_context["handler"]
    event_a_id = str(bdd_context["event_a"].event_id)
    # Original handler saw it once; restarted handler saw it zero times.
    assert original_handler.events_seen.count(event_a_id) == 1
    if "restarted_handler" in bdd_context:
        assert bdd_context["restarted_handler"].events_seen.count(event_a_id) == 0


@then("the duplicate is safely acknowledged")
def then_duplicate_safely_acked(bdd_context: dict[str, Any]) -> None:
    """Redelivered duplicate must be acked (process returns True)."""
    result = bdd_context.get("redelivery_result")
    assert result is True, "Duplicate must be acknowledged (process returns True)"


# =============================================================================
# Then steps — Idempotent consumer processing
# =============================================================================


@then("the event is stored in the idempotency store")
def then_event_in_ledger(bdd_context: dict[str, Any]) -> None:
    """Idempotency store must know about the event."""
    store: IdempotencyStore = bdd_context["store"]
    envelope = bdd_context["published_event"]
    assert store.is_processed(envelope.event_id) is True


@then("the handler output records the event")
def then_handler_records_event(bdd_context: dict[str, Any]) -> None:
    """Handler must have processed the event."""
    handler: TrackingHandler = bdd_context["handler"]
    envelope = bdd_context["published_event"]
    assert handler.was_processed(str(envelope.event_id))


@then("the event is acknowledged")
def then_event_acknowledged(bdd_context: dict[str, Any]) -> None:
    """Message must be acked (process returned True)."""
    result = bdd_context.get("first_result")
    assert result is True


@then("the consumer detects the duplicate via idempotency")
def then_consumer_detects_duplicate(bdd_context: dict[str, Any]) -> None:
    """Second processing must be rejected as duplicate."""
    result = bdd_context["second_result"]
    assert result is True  # Already processed => True, no handler called


@then("the handler state is not modified")
def then_handler_state_not_modified(bdd_context: dict[str, Any]) -> None:
    """Second pass must not change handler state."""
    handler: TrackingHandler = bdd_context["handler"]
    envelope = bdd_context["published_event"]
    # Should appear exactly once in the handler's seen list
    assert handler.events_seen.count(str(envelope.event_id)) == 1


@then("the event is acknowledged again")
def then_event_acknowledged_again(bdd_context: dict[str, Any]) -> None:
    """Duplicate must also return True (acked to clear from stream)."""
    result = bdd_context["second_result"]
    assert result is True


# =============================================================================
# Then steps — Consumer crashes mid-processing
# =============================================================================


@then(parsers.parse("the first {count:d} events are processed and acknowledged"))
def then_first_n_acked(bdd_context: dict[str, Any], count: int) -> None:
    """Exactly N messages acked before crash."""
    handler: TrackingHandler = bdd_context["handler"]
    assert len(handler.events_seen) == count
    assert len(handler.acks) == count


@then(parsers.parse("the {nth:d}th event fails"))
def then_nth_event_fails(bdd_context: dict[str, Any], nth: int) -> None:
    """Crash point reached."""
    assert bdd_context.get("batch_complete") is False


@then(parsers.parse("events {start:d}–{end:d} remain pending in the stream"))
def then_events_pending(bdd_context: dict[str, Any], start: int, end: int) -> None:
    """Unprocessed messages must still be pending (not acked)."""
    handler: TrackingHandler = bdd_context["handler"]
    events = bdd_context["batch_events"]
    for ev in events[start - 1 : end]:
        assert not handler.was_acked(str(ev.event_id))


@then(parsers.parse("events {start:d}–{end:d} are processed"))
def then_events_processed(bdd_context: dict[str, Any], start: int, end: int) -> None:
    """Remaining messages must be handled after restart."""
    recovered_handler: TrackingHandler = bdd_context["recovered_handler"]
    events = bdd_context["batch_events"]
    for ev in events[start - 1 : end]:
        assert recovered_handler.was_processed(str(ev.event_id))


@then(parsers.parse("events {start:d}–{end:d} remain idempotent"))
@then(
    parsers.parse(
        "events {start:d}–{end:d} remain idempotent (not re-processed by handler)"
    )
)
def then_events_idempotent(bdd_context: dict[str, Any], start: int, end: int) -> None:
    """Already-acked events must not be re-processed by handler."""
    recovered_handler: TrackingHandler = bdd_context["recovered_handler"]
    events = bdd_context["batch_events"]
    for ev in events[start - 1 : end]:
        assert not recovered_handler.was_processed(str(ev.event_id))


@then(
    parsers.parse("the final handler state contains exactly {count:d} unique records")
)
def then_final_state_count(bdd_context: dict[str, Any], count: int) -> None:
    """Exactly N unique records after recovery (union of original + recovered handlers)."""
    original = set(bdd_context["handler"].events_seen)
    recovered = set(bdd_context["recovered_handler"].events_seen)
    total_unique = len(original | recovered)
    assert total_unique == count
