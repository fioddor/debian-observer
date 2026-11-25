"""Normalization: the britney pair eventizer (ADR-024, ADR-025, ADR-028).

Pins docs/release3.project.v0.1.md §9.3 (the pair contract) and §6
(the event model): the first observed snapshot of a window is seeded
complete (one ``ExcuseObserved`` per item, ADR-028), each following
snapshot is eventized as deltas against its predecessor — entered,
left, verdict changed, blocked-by changed (compared as a set),
version changed — and every eventized snapshot publishes one
``SnapshotObserved`` frame carrying its ``generated_date``,
``prev_snapshot``, ``elapsed``, ``gap_before`` and item count, so a
consumer never infers a pair's duration (§2.3).

Identity (ADR-025): ``event_id`` is derived, not minted —
``uuid5(namespace, canonical(logical key + payload))`` — so a
re-eventized pair yields the same ids. The derivation is pinned by
the fixed vectors of ``tests/contract/test_britney_event_id_vectors.py``.

C2 produces to the lake only: publication (NATS, ``Nats-Msg-Id``)
is C3 and is NOT implemented here.
"""

from __future__ import annotations

import gzip
import uuid
from datetime import datetime, timezone
from typing import Any

import pytest
import yaml

from debian_observer.ingestion.britney import GeneratedDateTie
from debian_observer.normalization.britney import (
    ParsedSnapshot,
    parse_snapshot,
)
from debian_observer.normalization.eventizers.excuses import (
    EVENT_ID_NAMESPACE,
    SUBJECTS,
    ExcusesEventizer,
    derive_event_id,
    subject_for,
)

# ---------------------------------------------------------------------------
# Synthetic, deterministic window
# ---------------------------------------------------------------------------


def snapshot(
    snapshot_id: str,
    generated_date: str,
    items: list[dict[str, Any]],
) -> ParsedSnapshot:
    """One parsed snapshot from a synthetic document."""
    doc = {"generated-date": generated_date, "sources": items}
    return parse_snapshot(
        snapshot_id, gzip.compress(yaml.safe_dump(doc).encode())
    )


def item(name: str, **fields: Any) -> dict[str, Any]:
    """One source item: a name plus whatever britney wrote for it."""
    return {"item-name": name, **fields}


#: The seed: 01:00 UTC, two items. Keys are britney's own
#: (hyphenated) YAML keys (§4.1.1) — never Python-style names.
SEED_DATE = "2026-06-01T01:00:00Z"
SEED_ID = "20260601T010000Z_0001"
ALPHA = {
    "item-name": "alpha",
    "source": "alpha",
    "old-version": "1.0",
    "new-version": "1.1",
    "migration-policy-verdict": "PASS",
    "is-candidate": True,
    "reason": ["waiting for 5 days"],
    "dependencies": {"blocked-by": ["beta"], "migrate-after": []},
}
BETA_PASS = {
    "item-name": "beta",
    "source": "beta-src",
    "old-version": "2.0",
    "new-version": "2.1",
    "migration-policy-verdict": "REJECTED_TEMPORARILY",
}
SEED = snapshot(SEED_ID, SEED_DATE, [ALPHA, dict(BETA_PASS)])

#: Next snapshot: 02:00 UTC — beta's verdict changed, gamma entered,
#: alpha is unchanged (no event).
NEXT_DATE = "2026-06-01T02:00:00Z"
NEXT_ID = "20260601T020000Z_0001"
GAMMA = {
    "item-name": "gamma",
    "source": "gamma",
    "new-version": "3.0",
    "migration-policy-verdict": "REJECTED_BLOCKED_BY_ANOTHER_ITEM",
    "dependencies": {"blocked-by": ["alpha"]},
}
BETA_REJECTED = {
    "item-name": "beta",
    "source": "beta-src",
    "old-version": "2.0",
    "new-version": "2.1",
    "migration-policy-verdict": "REJECTED_BLOCKED_BY_ANOTHER_ITEM",
}
NEXT = snapshot(
    NEXT_ID,
    NEXT_DATE,
    [dict(ALPHA), dict(BETA_REJECTED), GAMMA],
)

#: Third snapshot: 03:00 UTC — beta left (absent), alpha's new-version
#: changed.
THIRD_DATE = "2026-06-01T03:00:00Z"
THIRD_ID = "20260601T030000Z_0001"
ALPHA_UPGRADED = {
    "item-name": "alpha",
    "source": "alpha",
    "old-version": "1.0",
    "new-version": "1.2",
    "migration-policy-verdict": "PASS",
    "is-candidate": True,
    "dependencies": {"blocked-by": ["beta"], "migrate-after": []},
}
THIRD = snapshot(THIRD_ID, THIRD_DATE, [dict(ALPHA_UPGRADED)])

WINDOW = [SEED, NEXT, THIRD]


@pytest.fixture()
def eventizer() -> ExcusesEventizer:
    return ExcusesEventizer()


def seed_events(eventizer: ExcusesEventizer) -> list[Any]:
    return list(
        eventizer.eventize_seed(SEED.states, snapshot=SEED.metadata)
    )


def pair_events(eventizer: ExcusesEventizer) -> list[Any]:
    return list(
        eventizer.eventize_pair(
            SEED.states, NEXT.states, prev=SEED.metadata, next=NEXT.metadata
        )
    )


# ---------------------------------------------------------------------------
# The seed (ADR-028)
# ---------------------------------------------------------------------------


class TestSeed:
    """The first observed snapshot is seeded complete (§9.4)."""

    def test_one_excuse_observed_per_item(self, eventizer) -> None:
        events = seed_events(eventizer)
        observed = [e for e in events if e.event_type == "ExcuseObserved"]
        assert sorted(e.entity_id for e in observed) == ["alpha", "beta"]

    def test_the_seed_carries_the_full_state(self, eventizer) -> None:
        observed = {
            e.entity_id: e for e in seed_events(eventizer)
            if e.event_type == "ExcuseObserved"
        }
        payload = observed["alpha"].payload
        assert payload["item_name"] == "alpha"
        assert payload["old_version"] == "1.0"
        assert payload["new_version"] == "1.1"
        assert payload["verdict"] == "PASS"
        assert payload["is_candidate"] is True
        assert payload["blocked_by"] == ["beta"]
        assert payload["migrate_after"] == []
        assert payload["reason"] == ["waiting for 5 days"]
        assert payload["snapshot"] == SEED_ID

    def test_the_seed_has_one_snapshot_frame(self, eventizer) -> None:
        frames = [
            e for e in seed_events(eventizer)
            if e.event_type == "SnapshotObserved"
        ]
        assert len(frames) == 1
        payload = frames[0].payload
        assert payload["snapshot"] == SEED_ID
        assert payload["generated_date"] == SEED_DATE
        assert payload["prev_snapshot"] is None
        assert payload["elapsed"] is None
        assert payload["gap_before"] is None
        assert payload["item_count"] == 2

    def test_the_seed_events_carry_the_snapshot_source(
        self, eventizer
    ) -> None:
        for event in seed_events(eventizer):
            assert event.source == "britney"
            assert event.ingestion_run_id is None


# ---------------------------------------------------------------------------
# The pair deltas (§9.3)
# ---------------------------------------------------------------------------


class TestPairDeltas:
    """Consecutive observed snapshots become canonical deltas (§6.1)."""

    def test_an_item_present_in_next_absent_in_prev_is_entered(
        self, eventizer
    ) -> None:
        entered = [
            e for e in pair_events(eventizer) if e.event_type == "ExcuseEntered"
        ]
        assert [e.entity_id for e in entered] == ["gamma"]
        assert entered[0].payload["item_name"] == "gamma"
        assert entered[0].payload["verdict"] == (
            "REJECTED_BLOCKED_BY_ANOTHER_ITEM"
        )
        assert entered[0].payload["blocked_by"] == ["alpha"]
        assert entered[0].payload["snapshot"] == NEXT_ID
        assert entered[0].payload["prev_snapshot"] == SEED_ID

    def test_an_item_present_in_prev_absent_in_next_is_left(
        self, eventizer
    ) -> None:
        events = list(
            eventizer.eventize_pair(
                NEXT.states,
                THIRD.states,
                prev=NEXT.metadata,
                next=THIRD.metadata,
            )
        )
        left = [e for e in events if e.event_type == "ExcuseLeft"]
        assert sorted(e.entity_id for e in left) == ["beta", "gamma"]
        # The last state travels with the event: the consumer never
        # reads "left" as "migrated" (§6.1).
        beta = [e for e in left if e.entity_id == "beta"][0]
        assert beta.payload["new_version"] == "2.1"
        assert beta.payload["verdict"] == (
            "REJECTED_BLOCKED_BY_ANOTHER_ITEM"
        )
        assert beta.payload["prev_snapshot"] == NEXT_ID
        assert beta.payload["snapshot"] == THIRD_ID

    def test_a_verdict_change_is_one_event_with_before_and_after(
        self, eventizer
    ) -> None:
        changed = [
            e for e in pair_events(eventizer)
            if e.event_type == "ExcuseVerdictChanged"
        ]
        assert [e.entity_id for e in changed] == ["beta"]
        assert changed[0].payload["before"] == "REJECTED_TEMPORARILY"
        assert changed[0].payload["after"] == (
            "REJECTED_BLOCKED_BY_ANOTHER_ITEM"
        )

    def test_an_unchanged_item_yields_no_event(self, eventizer) -> None:
        entities = {e.entity_id for e in pair_events(eventizer)} - {NEXT_ID}
        assert "alpha" not in entities

    def test_a_version_change_compares_new_version_only(
        self, eventizer
    ) -> None:
        events = list(
            eventizer.eventize_pair(
                NEXT.states,
                THIRD.states,
                prev=NEXT.metadata,
                next=THIRD.metadata,
            )
        )
        changed = [
            e for e in events if e.event_type == "ExcuseVersionChanged"
        ]
        assert [e.entity_id for e in changed] == ["alpha"]
        assert changed[0].payload["before"] == "1.1"
        assert changed[0].payload["after"] == "1.2"

    def test_blocked_by_compares_as_a_set(self, eventizer) -> None:
        """Order is not a change: [beta, gamma] == [gamma, beta]."""
        before = snapshot(
            "20260602T010000Z_0001",
            "2026-06-02T01:00:00Z",
            [item("alpha", dependencies={"blocked-by": ["beta", "gamma"]})],
        )
        after_same_set = snapshot(
            "20260602T020000Z_0001",
            "2026-06-02T02:00:00Z",
            [item("alpha", dependencies={"blocked-by": ["gamma", "beta"]})],
        )
        events = list(
            eventizer.eventize_pair(
                before.states,
                after_same_set.states,
                prev=before.metadata,
                next=after_same_set.metadata,
            )
        )
        assert not [
            e for e in events
            if e.event_type == "ExcuseBlockedByChanged"
        ]
        after_added = snapshot(
            "20260602T030000Z_0001",
            "2026-06-02T03:00:00Z",
            [item("alpha", dependencies={"blocked-by": ["beta"]})],
        )
        events = list(
            eventizer.eventize_pair(
                before.states,
                after_added.states,
                prev=before.metadata,
                next=after_added.metadata,
            )
        )
        changed = [
            e for e in events if e.event_type == "ExcuseBlockedByChanged"
        ]
        assert changed[0].payload["before"] == ["beta", "gamma"]
        assert changed[0].payload["after"] == ["beta"]

    def test_the_pair_has_one_snapshot_frame(self, eventizer) -> None:
        frames = [
            e for e in pair_events(eventizer)
            if e.event_type == "SnapshotObserved"
        ]
        assert len(frames) == 1
        payload = frames[0].payload
        assert payload["snapshot"] == NEXT_ID
        assert payload["generated_date"] == NEXT_DATE
        assert payload["prev_snapshot"] == SEED_ID
        assert payload["elapsed"] == 3600
        assert payload["gap_before"] is False
        assert payload["item_count"] == 3


# ---------------------------------------------------------------------------
# The frame's gap marker (§9.3)
# ---------------------------------------------------------------------------


class TestGapBefore:
    """A pair spans consecutive observed snapshots, however long."""

    def test_a_wide_pair_marks_its_gap_before(self, eventizer) -> None:
        prev = snapshot(
            "20260603T090000Z_0001", "2026-06-03T09:00:00Z", [dict(ALPHA)]
        )
        nxt = snapshot(
            "20260603T210000Z_0001", "2026-06-03T21:00:00Z", [dict(ALPHA)]
        )
        events = list(
            eventizer.eventize_pair(
                prev.states,
                nxt.states,
                prev=prev.metadata,
                next=nxt.metadata,
            )
        )
        frame = [
            e for e in events if e.event_type == "SnapshotObserved"
        ][0]
        assert frame.payload["gap_before"] is True
        assert frame.payload["elapsed"] == 12 * 3600


# ---------------------------------------------------------------------------
# Identity (ADR-025)
# ---------------------------------------------------------------------------


class TestIdentity:
    """event_id is derived from the logical key plus the payload."""

    def test_the_ids_are_uuid5_over_the_frozen_namespace(self) -> None:
        event_id = derive_event_id(
            event_type="ExcuseObserved",
            entity="alpha",
            prev_snapshot=None,
            next_snapshot=SEED_ID,
            payload={"item_name": "alpha", "snapshot": SEED_ID},
        )
        assert event_id.version == 5
        # uuid5 of the canonical form against the frozen namespace.
        import json

        canonical = json.dumps(
            {
                "logical_key": {
                    "event_type": "ExcuseObserved",
                    "entity": "alpha",
                    "prev_snapshot": None,
                    "next_snapshot": SEED_ID,
                },
                "payload": {"item_name": "alpha", "snapshot": SEED_ID},
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        assert event_id == uuid.uuid5(EVENT_ID_NAMESPACE, canonical)

    def test_a_re_eventized_pair_yields_the_same_ids(self, eventizer) -> None:
        first = [e.event_id for e in pair_events(eventizer)]
        second = [e.event_id for e in pair_events(eventizer)]
        assert first == second

    def test_a_payload_change_yields_new_ids(self, eventizer) -> None:
        """A parser fix changes the content: old and new stay distinct."""
        same_id = derive_event_id(
            event_type="ExcuseObserved",
            entity="alpha",
            prev_snapshot=None,
            next_snapshot=SEED_ID,
            payload={"item_name": "alpha", "snapshot": SEED_ID},
        )
        changed_id = derive_event_id(
            event_type="ExcuseObserved",
            entity="alpha",
            prev_snapshot=None,
            next_snapshot=SEED_ID,
            payload={"item_name": "alpha", "snapshot": SEED_ID, "x": 1},
        )
        assert same_id != changed_id

    def test_the_envelopes_carry_the_derived_ids(self, eventizer) -> None:
        event = [
            e for e in pair_events(eventizer)
            if e.event_type == "ExcuseEntered"
        ][0]
        expected = derive_event_id(
            event_type="ExcuseEntered",
            entity="gamma",
            prev_snapshot=SEED_ID,
            next_snapshot=NEXT_ID,
            payload=event.payload,
        )
        assert event.event_id == expected


# ---------------------------------------------------------------------------
# Subjects (§6.1)
# ---------------------------------------------------------------------------


class TestSubjects:
    """debian.migration.excuse.<kind> and the snapshot frame."""

    def test_every_event_type_has_a_subject(self) -> None:
        from debian_observer.normalization.eventizers.excuses import (
            EXCUSE_EVENT_TYPES,
        )

        assert set(SUBJECTS) == set(EXCUSE_EVENT_TYPES) | {"SnapshotObserved"}

    def test_the_subjects_follow_the_spec_kinds(self) -> None:
        assert SUBJECTS["SnapshotObserved"] == "debian.migration.snapshot.observed"
        for event_type in SUBJECTS:
            if event_type == "SnapshotObserved":
                continue
            assert SUBJECTS[event_type].startswith("debian.migration.excuse.")

    def test_the_existing_migration_subject_is_untouched(self) -> None:
        """debian.migration.observed belongs to MigrationObserved (D8)."""
        assert "debian.migration.observed" not in SUBJECTS.values()

    def test_subject_for_maps_each_type(self) -> None:
        for event_type, subject in SUBJECTS.items():
            assert subject_for(event_type) == subject


# ---------------------------------------------------------------------------
# The window (ADR-027: sequential)
# ---------------------------------------------------------------------------


class TestWindow:
    """Seed first, then one pair per following snapshot, in order."""

    def test_the_window_yields_the_seed_then_the_pairs(
        self, eventizer
    ) -> None:
        events = list(eventizer.eventize_window(WINDOW))
        types = [e.event_type for e in events]
        # The seed comes first: two ExcuseObserved plus its frame.
        assert types.count("ExcuseObserved") == 2
        assert types[0] == "ExcuseObserved"
        assert types.count("SnapshotObserved") == 3  # one per snapshot
        # Pair 1 (NEXT): beta's verdict, gamma's entry, the frame.
        assert types.count("ExcuseVerdictChanged") == 1
        assert types.count("ExcuseEntered") == 1
        # Pair 2 (THIRD): beta's and gamma's exits, alpha's version, the
        # frame.
        assert types.count("ExcuseLeft") == 2
        assert types.count("ExcuseVersionChanged") == 1

    def test_the_pairs_come_in_snapshot_order(self, eventizer) -> None:
        events = list(eventizer.eventize_window(WINDOW))
        frames = [e for e in events if e.event_type == "SnapshotObserved"]
        assert [f.payload["snapshot"] for f in frames] == [
            SEED_ID,
            NEXT_ID,
            THIRD_ID,
        ]

    def test_a_single_snapshot_window_is_just_the_seed(
        self, eventizer
    ) -> None:
        events = list(eventizer.eventize_window([SEED]))
        assert [e.event_type for e in events].count("SnapshotObserved") == 1

    def test_an_empty_window_yields_nothing(self, eventizer) -> None:
        assert list(eventizer.eventize_window([])) == []

    def test_a_generated_date_tie_is_an_error(self, eventizer) -> None:
        tied = snapshot("20260604T010000Z_0001", SEED_DATE, [dict(ALPHA)])
        with pytest.raises(GeneratedDateTie):
            list(eventizer.eventize_window([SEED, tied]))

    def test_the_window_is_sorted_by_generated_date(self, eventizer) -> None:
        events = list(eventizer.eventize_window(list(reversed(WINDOW))))
        frames = [e for e in events if e.event_type == "SnapshotObserved"]
        assert [f.payload["snapshot"] for f in frames] == [
            SEED_ID,
            NEXT_ID,
            THIRD_ID,
        ]