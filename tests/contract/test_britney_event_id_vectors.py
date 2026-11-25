"""Contract: ``event_id`` derivation of britney excuse events (ADR-025).

Fixed vectors pin the derivation
(docs/release3.project.v0.1.md §6.2, §19.4):
``event_id = uuid5(namespace, canonical(logical key + payload))``.
The namespace and the canonical form are frozen by these constants:
if the derivation ever changes, these tests go red — a re-eventized
pair must keep yielding the same ids, and a parser fix (a payload
change) must yield NEW ids so old and new stay distinguishable.
"""

from __future__ import annotations

import uuid

from debian_observer.normalization.eventizers.excuses import (
    EVENT_ID_NAMESPACE,
    derive_event_id,
)

# The frozen namespace of ADR-025, in uuid form.
NAMESPACE_HEX = "9c6f4a8e-0b6e-4f6e-9c3f-1e2d3a4b5c6d"


class TestNamespace:
    """The namespace is frozen: moving it would mint every id anew."""

    def test_the_namespace_is_the_frozen_uuid(self) -> None:
        assert str(EVENT_ID_NAMESPACE) == NAMESPACE_HEX


class TestDerivationVectors:
    """uuid5(namespace, canonical(logical key + payload))."""

    #: Vector 1 — a seed ``ExcuseObserved``: key
    #: (event_type, entity, seed_snapshot), payload = its full state.
    SEED_VECTOR = "be2016be-45f2-51e3-a488-6659774fa834"

    def test_a_seed_excuse_observed(self) -> None:
        event_id = derive_event_id(
            event_type="ExcuseObserved",
            entity="alpha",
            prev_snapshot=None,
            next_snapshot="20260601T010000Z_0001",
            payload={
                "item_name": "alpha",
                "source": "alpha",
                "old_version": "1.0",
                "new_version": "1.1",
                "verdict": "PASS",
                "is_candidate": True,
                "blocked_by": ["beta"],
                "migrate_after": [],
                "reason": ["waiting"],
                "snapshot": "20260601T010000Z_0001",
            },
        )
        assert event_id == uuid.UUID(self.SEED_VECTOR)
        assert event_id.version == 5

    #: Vector 2 — a delta ``ExcuseVerdictChanged``: key
    #: (event_type, entity, prev_snapshot, next_snapshot).
    VERDICT_VECTOR = "31be3dcc-dcc7-5eda-9d45-349b964b1aeb"

    def test_a_verdict_changed_delta(self) -> None:
        event_id = derive_event_id(
            event_type="ExcuseVerdictChanged",
            entity="alpha",
            prev_snapshot="20260601T010000Z_0001",
            next_snapshot="20260601T020000Z_0001",
            payload={
                "item_name": "alpha",
                "before": "PASS",
                "after": "REJECTED_TEMPORARILY",
                "prev_snapshot": "20260601T010000Z_0001",
                "snapshot": "20260601T020000Z_0001",
            },
        )
        assert event_id == uuid.UUID(self.VERDICT_VECTOR)

    #: Vector 3 — the ``SnapshotObserved`` frame: the entity is the
    #: snapshot itself.
    FRAME_VECTOR = "cc857f5b-a3dc-51b4-9d60-c959a167971e"

    def test_a_snapshot_observed_frame(self) -> None:
        event_id = derive_event_id(
            event_type="SnapshotObserved",
            entity="20260601T020000Z_0001",
            prev_snapshot="20260601T010000Z_0001",
            next_snapshot="20260601T020000Z_0001",
            payload={
                "snapshot": "20260601T020000Z_0001",
                "generated_date": "2026-06-01T02:00:00Z",
                "prev_snapshot": "20260601T010000Z_0001",
                "elapsed": 3600,
                "gap_before": False,
                "item_count": 1,
            },
        )
        assert event_id == uuid.UUID(self.FRAME_VECTOR)

    def test_a_payload_change_mints_a_new_id(self) -> None:
        base = {
            "item_name": "alpha",
            "before": "PASS",
            "after": "REJECTED_TEMPORARILY",
            "prev_snapshot": "20260601T010000Z_0001",
            "snapshot": "20260601T020000Z_0001",
        }
        key = {
            "event_type": "ExcuseVerdictChanged",
            "entity": "alpha",
            "prev_snapshot": "20260601T010000Z_0001",
            "next_snapshot": "20260601T020000Z_0001",
        }
        first = derive_event_id(payload=base, **key)
        # A parser fix changes the payload's content: new ids, so old
        # and new events stay distinguishable instead of colliding.
        changed = derive_event_id(payload={**base, "after": "REJECTED"}, **key)
        assert first != changed
        assert first == derive_event_id(payload=base, **key)