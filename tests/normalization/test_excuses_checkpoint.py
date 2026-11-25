"""Normalization: the britney checkpoint — a resumable eventizer.

Pins docs/release3.project.v0.1.md §9.3 (the checkpoint) and §15: the
eventizer records the last pair it published; a restart resumes after
it, and re-running a published window yields **zero** new events. The
already-derived events are never re-written: their ids are derived
(ADR-025), so a re-derive would be byte-identical — the checkpoint
makes even that unnecessary.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from debian_observer.normalization.britney import parse_snapshot
from debian_observer.normalization.eventizers.excuses import (
    EventizerCheckpoint,
    ExcusesEventizer,
)

# ---------------------------------------------------------------------------
# Synthetic, deterministic window (three snapshots, one item evolving)
# ---------------------------------------------------------------------------

ALPHA = {
    "item-name": "alpha",
    "source": "alpha",
    "new-version": "1.1",
    "migration-policy-verdict": "PASS",
}
ALPHA_NEXT = dict(ALPHA, **{"new-version": "1.2"})


def parsed(snapshot_id: str, generated_date: str, items: list[dict[str, Any]]):
    doc = {"generated-date": generated_date, "sources": items}
    return parse_snapshot(
        snapshot_id, gzip.compress(yaml.safe_dump(doc).encode())
    )


SEED = parsed("20260601T010000Z_0001", "2026-06-01T01:00:00Z", [ALPHA])
NEXT = parsed(
    "20260601T020000Z_0001",
    "2026-06-01T02:00:00Z",
    [dict(ALPHA), {"item-name": "beta"}],
)
THIRD = parsed("20260601T030000Z_0001", "2026-06-01T03:00:00Z", [ALPHA_NEXT])
WINDOW = [SEED, NEXT, THIRD]

SEED_ID = "20260601T010000Z_0001"
NEXT_ID = "20260601T020000Z_0001"
THIRD_ID = "20260601T030000Z_0001"


@pytest.fixture()
def eventizer() -> ExcusesEventizer:
    return ExcusesEventizer()


# ---------------------------------------------------------------------------
# The checkpoint record
# ---------------------------------------------------------------------------


class TestEventizerCheckpoint:
    """A checkpoint records up to which snapshot was eventized."""

    def test_a_missing_checkpoint_loads_fresh(self, tmp_path: Path) -> None:
        checkpoint = EventizerCheckpoint.load(
            tmp_path / "missing-checkpoint.json"
        )
        assert checkpoint.last_snapshot is None

    def test_save_then_load_roundtrips(self, tmp_path: Path) -> None:
        path = tmp_path / "checkpoint.json"
        EventizerCheckpoint(path).save(THIRD_ID)
        assert EventizerCheckpoint.load(path).last_snapshot == THIRD_ID

    def test_the_record_names_its_source_and_eventizer(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "checkpoint.json"
        EventizerCheckpoint(path).save(NEXT_ID)
        record = json.loads(path.read_text(encoding="utf-8"))
        assert record["source"] == "britney"
        assert record["eventizer"] == "excuses"
        assert record["last_snapshot"] == NEXT_ID

    def test_save_overwrites_the_previous_pair(self, tmp_path: Path) -> None:
        path = tmp_path / "checkpoint.json"
        checkpoint = EventizerCheckpoint(path)
        checkpoint.save(SEED_ID)
        checkpoint.save(NEXT_ID)
        assert EventizerCheckpoint.load(path).last_snapshot == NEXT_ID


# ---------------------------------------------------------------------------
# The resumable chunks
# ---------------------------------------------------------------------------


class TestResume:
    """The eventizer resumes after its checkpoint, pair by pair."""

    def test_a_fresh_run_chunks_seed_then_pairs(self, eventizer) -> None:
        chunks = list(eventizer.iter_eventized(WINDOW))
        assert [snapshot_id for snapshot_id, _ in chunks] == [
            SEED_ID,
            NEXT_ID,
            THIRD_ID,
        ]
        # The seed chunk seeds complete; the pairs carry deltas.
        assert [e.event_type for e in chunks[0][1]].count("ExcuseObserved") == 1
        assert [e.event_type for e in chunks[1][1]].count("ExcuseEntered") == 1

    def test_a_checkpointed_run_resumes_after_it(self, eventizer) -> None:
        # As if NEXT had just been published.
        chunks = list(
            eventizer.iter_eventized(
                WINDOW,
                checkpoint=EventizerCheckpoint.at(NEXT_ID),
            )
        )
        assert [snapshot_id for snapshot_id, _ in chunks] == [THIRD_ID]

    def test_a_completed_window_yields_zero_chunks(self, eventizer) -> None:
        chunks = list(
            eventizer.iter_eventized(
                WINDOW,
                checkpoint=EventizerCheckpoint.at(THIRD_ID),
            )
        )
        assert chunks == []

    def test_a_checkpoint_outside_the_window_is_an_error(
        self, eventizer
    ) -> None:
        with pytest.raises(ValueError, match="outside this window"):
            list(
                eventizer.iter_eventized(
                    WINDOW,
                    checkpoint=EventizerCheckpoint.at(
                        "20250101T000000Z_0001"
                    ),
                )
            )

    def test_a_window_with_one_snapshot_only_seeds(self, eventizer) -> None:
        chunks = list(eventizer.iter_eventized([SEED]))
        assert [snapshot_id for snapshot_id, _ in chunks] == [SEED_ID]