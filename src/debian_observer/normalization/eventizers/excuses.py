"""Normalization: the britney pair eventizer (ADR-024, ADR-025, ADR-028).

Consecutive **observed** snapshots become canonical excuse events
(docs/release3.project.v0.1.md §9.3): the first snapshot of a window
is seeded complete (ADR-028), each following snapshot is eventized as
deltas against its predecessor (ADR-024), and every eventized
snapshot emits one ``SnapshotObserved`` frame that makes the pair
explicit — ``generated_date``, ``prev_snapshot``, ``elapsed``,
``gap_before``, item count — so a consumer never infers a duration
(§2.3).

Identity (ADR-025): every envelope carries a **derived**
``event_id`` — ``uuid5`` of the canonical logical key plus the payload
over the frozen :data:`EVENT_ID_NAMESPACE` — passed explicitly to
``EventEnvelope.from_payload``. A re-eventized pair yields the same
ids; the derivation is pinned by the fixed vectors of
``tests/contract/test_britney_event_id_vectors.py``. The envelope
timestamps are pinned to the snapshot's generated-date, so a
re-derive is byte-identical — idempotent by construction. JetStream
deduplication stays an optimization (C3): the guarantee lives in the
consumers, which deduplicate by ``event_id``.

Ordering (ADR-027): :meth:`ExcusesEventizer.eventize_window` yields
the seed first, then one pair per following snapshot, in
``generated_date`` order — the pair eventizer stays sequential. C2
produces to the lake only; publication (``Nats-Msg-Id = event_id``)
is C3 and is not here.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from debian_observer.ingestion.britney import (
    GAP_THRESHOLD_SECONDS,
    GeneratedDateTie,
)
from debian_observer.normalization.britney import (
    ExcuseState,
    SnapshotMetadata,
)
from debian_observer.events.envelope import EventEnvelope
from debian_observer.events.types import (
    ExcuseBlockedByChangedPayload,
    ExcuseEnteredPayload,
    ExcuseLeftPayload,
    ExcuseMigrateAfterChangedPayload,
    ExcuseObservedPayload,
    ExcuseVerdictChangedPayload,
    ExcuseVersionChangedPayload,
    ObservationMeta,
    SnapshotObservedPayload,
)

__all__ = [
    "EVENT_ID_NAMESPACE",
    "EXCUSE_EVENT_TYPES",
    "SUBJECTS",
    "EventizerCheckpoint",
    "ExcusesEventizer",
    "derive_event_id",
    "subject_for",
]

# ---------------------------------------------------------------------------
# Identity (ADR-025)
# ---------------------------------------------------------------------------

#: The frozen UUID namespace of ADR-025. Chosen once at implementation
#: time, frozen forever after: moving it would mint every derived id
#: anew and break every consumer's deduplication. Its value carries no
#: meaning — only its stability does.
EVENT_ID_NAMESPACE = uuid.UUID("9c6f4a8e-0b6e-4f6e-9c3f-1e2d3a4b5c6d")


def _canonical(value: Mapping[str, Any]) -> str:
    """The canonical JSON form of a logical key plus its payload.

    Sorted keys, no padding: one byte string per distinct content.
    Payload values must already be JSON-native (the payload models
    dump with ``mode="json"``, so datetimes are ISO strings).
    """
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def derive_event_id(
    *,
    event_type: str,
    entity: str,
    prev_snapshot: str | None,
    next_snapshot: str | None,
    payload: Mapping[str, Any],
) -> uuid.UUID:
    """Derive one event's identity (ADR-025).

    Logical key: ``(event_type, entity, prev_snapshot,
    next_snapshot)`` — for the seed ``(ExcuseObserved, entity,
    seed_snapshot)``. ``event_id = uuid5(namespace, canonical(logical
    key + payload))``: a re-eventized pair yields the same ids, a
    parser fix (a payload change) yields new ones — old and new stay
    distinguishable instead of colliding.
    """
    logical_key = {
        "event_type": event_type,
        "entity": entity,
        "prev_snapshot": prev_snapshot,
        "next_snapshot": next_snapshot,
    }
    canonical = _canonical({"logical_key": logical_key, "payload": payload})
    return uuid.uuid5(EVENT_ID_NAMESPACE, canonical)


# ---------------------------------------------------------------------------
# Subjects (§6.1)
# ---------------------------------------------------------------------------

#: The event types of the excuse event model (§6.1).
EXCUSE_EVENT_TYPES = (
    "ExcuseObserved",
    "ExcuseEntered",
    "ExcuseLeft",
    "ExcuseVerdictChanged",
    "ExcuseBlockedByChanged",
    "ExcuseMigrateAfterChanged",
    "ExcuseVersionChanged",
)

#: One NATS subject per event type (§6.1):
#: ``debian.migration.excuse.<kind>`` and
#: ``debian.migration.snapshot.observed``. The existing
#: ``debian.migration.observed`` (MigrationObserved, D8) is untouched.
SUBJECTS: dict[str, str] = {
    "ExcuseObserved": "debian.migration.excuse.observed",
    "ExcuseEntered": "debian.migration.excuse.entered",
    "ExcuseLeft": "debian.migration.excuse.left",
    "ExcuseVerdictChanged": "debian.migration.excuse.verdict_changed",
    "ExcuseBlockedByChanged": "debian.migration.excuse.blocked_by_changed",
    "ExcuseMigrateAfterChanged": (
        "debian.migration.excuse.migrate_after_changed"
    ),
    "ExcuseVersionChanged": "debian.migration.excuse.version_changed",
    "SnapshotObserved": "debian.migration.snapshot.observed",
}


def subject_for(event_type: str) -> str:
    """The NATS subject of one event type (§6.1).

    Unknown types raise ``KeyError`` naming the known types — C2 does
    not publish, but the mapping is the publication contract's single
    source (C3 reads it, never restates it).
    """
    if event_type not in SUBJECTS:
        known = ", ".join(sorted(SUBJECTS))
        raise KeyError(
            f"no subject for event type {event_type!r}; known types: {known}"
        )
    return SUBJECTS[event_type]


# ---------------------------------------------------------------------------
# The eventizer (ADR-024)
# ---------------------------------------------------------------------------


def _iso_z(moment: datetime) -> str:
    """A UTC timestamp as ISO 8601 with the Z suffix (the archive form)."""
    return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class ExcusesEventizer:
    """The pair eventizer of the ``britney`` source (ADR-024).

    ``name``/``event_type`` follow the ADR-014 registry contract; a
    state source adds ``eventize_seed`` and ``eventize_pair`` (§9.3).
    There is no ``shape`` and no row-wise ``eventize``: the unit of
    work is the snapshot pair, not a row.
    """

    name = "excuses"
    #: The primary event type (the registry's discriminator); a pair
    #: emits several types (§6.1).
    event_type = "ExcuseObserved"

    def __init__(self) -> None:
        self._source = "britney"

    # -- envelope construction -------------------------------------------

    def _envelope(
        self,
        *,
        event_type: str,
        entity_id: str,
        payload_model: Any,
        snapshot: SnapshotMetadata,
        prev_snapshot: str | None,
    ) -> EventEnvelope:
        """One envelope with a derived id, pinned to the snapshot.

        ``observed_at``/``effective_at`` are the snapshot's
        generated-date: the state was true when britney generated it,
        and pinning the timestamps keeps a re-derive byte-identical
        (idempotent by construction, ADR-025).
        """
        payload = payload_model.model_dump(mode="json")
        event_id = derive_event_id(
            event_type=event_type,
            entity=entity_id,
            prev_snapshot=prev_snapshot,
            next_snapshot=snapshot.snapshot_id,
            payload=payload,
        )
        return EventEnvelope.from_payload(
            event_type=event_type,
            source=self._source,
            entity_id=entity_id,
            payload_model=payload_model,
            event_id=event_id,
            observed_at=snapshot.generated_date,
            effective_at=snapshot.generated_date,
        )

    @staticmethod
    def _observation(snapshot: SnapshotMetadata) -> ObservationMeta:
        return ObservationMeta(
            source="britney",
            observed_at=snapshot.generated_date,
            effective_at=snapshot.generated_date,
        )

    @staticmethod
    def _frame_payload(
        snapshot: SnapshotMetadata,
        *,
        prev: SnapshotMetadata | None,
    ) -> SnapshotObservedPayload:
        """The frame of one eventized snapshot (§6.1).

        ``elapsed`` is the difference of the pair's generated-dates —
        the pair's duration is never assumed (§5.2); ``gap_before``
        marks a pair wider than the source's publication rhythm
        (:data:`GAP_THRESHOLD_SECONDS`, the acquisition's own gap
        notion, reused — not redefined).
        """
        elapsed: int | None = None
        gap_before: bool | None = None
        prev_id: str | None = None
        if prev is not None:
            elapsed = int(
                (snapshot.generated_date - prev.generated_date).total_seconds()
            )
            gap_before = elapsed > GAP_THRESHOLD_SECONDS
            prev_id = prev.snapshot_id
        return SnapshotObservedPayload(
            observation=ExcusesEventizer._observation(snapshot),
            snapshot=snapshot.snapshot_id,
            generated_date=_iso_z(snapshot.generated_date),
            prev_snapshot=prev_id,
            elapsed=elapsed,
            gap_before=gap_before,
            item_count=snapshot.item_count,
        )

    # -- the seed (ADR-028) -----------------------------------------------

    def eventize_seed(
        self,
        states: Mapping[str, ExcuseState],
        *,
        snapshot: SnapshotMetadata,
    ) -> Iterator[EventEnvelope]:
        """Seed the first observed snapshot complete (§9.4).

        One ``ExcuseObserved`` per item — the item's full
        :class:`ExcuseState` — plus the ``SnapshotObserved`` frame.
        """
        for item_name, state in sorted(states.items()):
            yield self._envelope(
                event_type="ExcuseObserved",
                entity_id=item_name,
                payload_model=ExcuseObservedPayload(
                    observation=self._observation(snapshot),
                    snapshot=snapshot.snapshot_id,
                    item_name=state.item_name,
                    source=state.source,
                    old_version=state.old_version,
                    new_version=state.new_version,
                    verdict=state.verdict,
                    is_candidate=state.is_candidate,
                    blocked_by=list(state.blocked_by),
                    migrate_after=list(state.migrate_after),
                    reason=list(state.reason),
                ),
                snapshot=snapshot,
                prev_snapshot=None,
            )
        yield self._envelope(
            event_type="SnapshotObserved",
            entity_id=snapshot.snapshot_id,
            payload_model=self._frame_payload(snapshot, prev=None),
            snapshot=snapshot,
            prev_snapshot=None,
        )

    # -- the pair (ADR-024) -------------------------------------------------

    def eventize_pair(
        self,
        prev_states: Mapping[str, ExcuseState],
        next_states: Mapping[str, ExcuseState],
        *,
        prev: SnapshotMetadata,
        next: SnapshotMetadata,  # noqa: A002 - the spec's own name (§9.3)
    ) -> Iterator[EventEnvelope]:
        """Eventize one pair of consecutive observed snapshots (§6.1).

        Deltas: entered (in next, not prev), left (in prev, not next,
        with its last state), verdict changed, blocked-by changed
        (compared as a **set**), migrate-after changed (compared as a
        **set** too — britney serves it in the item's dependencies),
        version changed (``new_version`` only — ``old_version`` is the
        testing side and changes when the item migrates, §9.3). Plus
        one ``SnapshotObserved`` frame for ``next``. No order within
        the pair (§6.3).
        """
        observation = self._observation(next)
        for item_name in sorted(set(prev_states) | set(next_states)):
            before = prev_states.get(item_name)
            after = next_states.get(item_name)
            if before is None and after is not None:
                yield self._envelope(
                    event_type="ExcuseEntered",
                    entity_id=item_name,
                    payload_model=ExcuseEnteredPayload(
                        observation=observation,
                        snapshot=next.snapshot_id,
                        prev_snapshot=prev.snapshot_id,
                        item_name=after.item_name,
                        source=after.source,
                        old_version=after.old_version,
                        new_version=after.new_version,
                        verdict=after.verdict,
                        is_candidate=after.is_candidate,
                        blocked_by=list(after.blocked_by),
                        migrate_after=list(after.migrate_after),
                        reason=list(after.reason),
                    ),
                    snapshot=next,
                    prev_snapshot=prev.snapshot_id,
                )
                continue
            if before is not None and after is None:
                # Left: ambiguous on purpose (migrated, removed or
                # superseded) — the last state travels with the event
                # and the consumer never reads "left" as "migrated"
                # (§6.1, stretch §22.1 adds the distinction).
                last = before
                yield self._envelope(
                    event_type="ExcuseLeft",
                    entity_id=item_name,
                    payload_model=ExcuseLeftPayload(
                        observation=observation,
                        snapshot=next.snapshot_id,
                        prev_snapshot=prev.snapshot_id,
                        item_name=last.item_name,
                        source=last.source,
                        old_version=last.old_version,
                        new_version=last.new_version,
                        verdict=last.verdict,
                        is_candidate=last.is_candidate,
                        blocked_by=list(last.blocked_by),
                        migrate_after=list(last.migrate_after),
                        reason=list(last.reason),
                    ),
                    snapshot=next,
                    prev_snapshot=prev.snapshot_id,
                )
                continue
            assert before is not None and after is not None
            if before.verdict != after.verdict:
                yield self._envelope(
                    event_type="ExcuseVerdictChanged",
                    entity_id=item_name,
                    payload_model=ExcuseVerdictChangedPayload(
                        observation=observation,
                        snapshot=next.snapshot_id,
                        prev_snapshot=prev.snapshot_id,
                        item_name=item_name,
                        before=before.verdict,
                        after=after.verdict,
                    ),
                    snapshot=next,
                    prev_snapshot=prev.snapshot_id,
                )
            # blocked-by compares as a set: order is not a change.
            if set(before.blocked_by) != set(after.blocked_by):
                yield self._envelope(
                    event_type="ExcuseBlockedByChanged",
                    entity_id=item_name,
                    payload_model=ExcuseBlockedByChangedPayload(
                        observation=observation,
                        snapshot=next.snapshot_id,
                        prev_snapshot=prev.snapshot_id,
                        item_name=item_name,
                        before=sorted(set(before.blocked_by)),
                        after=sorted(set(after.blocked_by)),
                    ),
                    snapshot=next,
                    prev_snapshot=prev.snapshot_id,
                )
            # migrate-after compares as a set too: order is not a
            # change (§9.3) — britney serves it in the item's
            # dependencies.
            if set(before.migrate_after) != set(after.migrate_after):
                yield self._envelope(
                    event_type="ExcuseMigrateAfterChanged",
                    entity_id=item_name,
                    payload_model=ExcuseMigrateAfterChangedPayload(
                        observation=observation,
                        snapshot=next.snapshot_id,
                        prev_snapshot=prev.snapshot_id,
                        item_name=item_name,
                        before=sorted(set(before.migrate_after)),
                        after=sorted(set(after.migrate_after)),
                    ),
                    snapshot=next,
                    prev_snapshot=prev.snapshot_id,
                )
            # new-version only: old-version is the testing side and
            # changes when the item migrates (§9.3).
            if before.new_version != after.new_version:
                yield self._envelope(
                    event_type="ExcuseVersionChanged",
                    entity_id=item_name,
                    payload_model=ExcuseVersionChangedPayload(
                        observation=observation,
                        snapshot=next.snapshot_id,
                        prev_snapshot=prev.snapshot_id,
                        item_name=item_name,
                        before=before.new_version,
                        after=after.new_version,
                    ),
                    snapshot=next,
                    prev_snapshot=prev.snapshot_id,
                )
        yield self._envelope(
            event_type="SnapshotObserved",
            entity_id=next.snapshot_id,
            payload_model=self._frame_payload(next, prev=prev),
            snapshot=next,
            prev_snapshot=prev.snapshot_id,
        )

    # -- the window (ADR-027: sequential) ------------------------------------

    def eventize_window(
        self,
        window: Sequence[Any],
    ) -> Iterator[EventEnvelope]:
        """Eventize a whole window: the seed, then one pair per snapshot.

        ``window`` is a sequence of parsed snapshots; it is ordered by
        ``generated_date`` (a tie is an error, §5.2 — never a silent
        pick). The events come pair by pair, in order: the events of
        pair n are yielded only after those of pair n-1 (ADR-027 —
        sequential by construction here; the checkpointed, resumable
        variant is the CLI's driver).
        """
        ordered = self.order_window(window)
        if not ordered:
            return
        seed = ordered[0]
        yield from self.eventize_seed(seed.states, snapshot=seed.metadata)
        for prev, nxt in zip(ordered, ordered[1:]):
            yield from self.eventize_pair(
                prev.states,
                nxt.states,
                prev=prev.metadata,
                next=nxt.metadata,
            )

    @staticmethod
    def order_window(window: Sequence[Any]) -> list[Any]:
        """Order parsed snapshots by generated-date; a tie is an error.

        Reuses the acquisition's :class:`GeneratedDateTie` (§5.2): the
        ordering error is the acquisition's, already named and tested
        there.
        """
        ordered = sorted(
            window, key=lambda parsed: parsed.metadata.generated_date
        )
        dates = [parsed.metadata.generated_date for parsed in ordered]
        if len(dates) != len(set(dates)):
            tied = sorted({d for d in dates if dates.count(d) > 1})
            raise GeneratedDateTie(
                "two snapshots of the window share a generated-date: "
                + ", ".join(_iso_z(d) for d in tied)
            )
        return ordered

    # -- the resumable window (§9.3: the checkpoint) ------------------------

    def iter_eventized(
        self,
        window: Sequence[Any],
        *,
        checkpoint: "EventizerCheckpoint | None" = None,
    ) -> Iterator[tuple[str, list[EventEnvelope]]]:
        """Yield ``(snapshot_id, events)`` per eventized snapshot, in order.

        The resumable form of :meth:`eventize_window`: with no
        checkpoint the first chunk is the seed of ``window[0]``, then
        one chunk per following snapshot, each paired against its
        predecessor — sequentially (ADR-027). With a checkpoint, the
        run resumes **after** the last eventized snapshot: its pairs
        are not re-derived, so a re-run of a published window yields
        zero chunks — zero new events. A checkpoint naming a snapshot
        outside the window is an error (the window is a different
        selection; guessing would re-seed or skip silently).

        The caller persists the checkpoint per chunk (the CLI does):
        a crash loses at most the pair in flight.
        """
        ordered = self.order_window(window)
        if not ordered:
            return
        ids = [parsed.metadata.snapshot_id for parsed in ordered]
        start = 0
        if checkpoint is not None and checkpoint.last_snapshot is not None:
            last = checkpoint.last_snapshot
            if last not in ids:
                raise ValueError(
                    f"checkpoint names snapshot {last!r} outside this "
                    f"window; the window holds: {', '.join(ids)}"
                )
            start = ids.index(last) + 1
        if start >= len(ordered):
            return
        if start == 0:
            seed = ordered[0]
            yield (
                seed.metadata.snapshot_id,
                list(self.eventize_seed(seed.states, snapshot=seed.metadata)),
            )
            start = 1
        for prev, nxt in zip(ordered[start - 1 :], ordered[start :]):
            yield (
                nxt.metadata.snapshot_id,
                list(
                    self.eventize_pair(
                        prev.states,
                        nxt.states,
                        prev=prev.metadata,
                        next=nxt.metadata,
                    )
                ),
            )


# ---------------------------------------------------------------------------
# The checkpoint (§9.3)
# ---------------------------------------------------------------------------


class EventizerCheckpoint:
    """Where the pair eventizer stopped: the last eventized snapshot.

    A small JSON record beside the lake: ``source``, ``eventizer``,
    ``last_snapshot`` and an ``updated_at`` timestamp. ``save`` writes
    atomically (a temp file, then rename), so a crash never leaves a
    half-written checkpoint that would resume mid-pair.

    A pathless checkpoint (``at``) is in-memory only: ``save`` is a
    no-op — useful for a dry run and for the battery.
    """

    _KIND = ("britney", "excuses")

    def __init__(self, path: Path | None = None) -> None:
        self._path = Path(path) if path is not None else None
        self._last: str | None = None

    @property
    def last_snapshot(self) -> str | None:
        """The last fully eventized snapshot id, or None (fresh)."""
        return self._last

    @classmethod
    def load(cls, path: Path | None) -> "EventizerCheckpoint":
        """Read the checkpoint from ``path`` (missing file: fresh)."""
        checkpoint = cls(path)
        if path is not None and Path(path).exists():
            record = json.loads(Path(path).read_text(encoding="utf-8"))
            source, eventizer = record.get("source"), record.get("eventizer")
            if (source, eventizer) != cls._KIND:
                raise ValueError(
                    f"checkpoint {path} belongs to source {source!r} "
                    f"eventizer {eventizer!r}, not {cls._KIND}"
                )
            checkpoint._last = record.get("last_snapshot")
        return checkpoint

    @classmethod
    def at(cls, last_snapshot: str) -> "EventizerCheckpoint":
        """An in-memory checkpoint as if ``last_snapshot`` was published."""
        checkpoint = cls(None)
        checkpoint._last = last_snapshot
        return checkpoint

    def save(self, snapshot_id: str) -> None:
        """Record ``snapshot_id`` as the last eventized snapshot."""
        self._last = snapshot_id
        if self._path is None:
            return
        record = {
            "source": self._KIND[0],
            "eventizer": self._KIND[1],
            "last_snapshot": snapshot_id,
            "updated_at": _iso_z(datetime.now(timezone.utc)),
        }
        tmp = self._path.with_suffix(".json.tmp")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(
            json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        os.replace(tmp, self._path)