"""Storage: the britney event lake — Parquet, idempotent by fold.

Pins docs/release3.project.v0.1.md §10: one directory per event type,
one file per run, readable with a plain ``read_parquet`` glob without
``union_by_name`` and without ``hive_partitioning`` (ADR-016). The
lake's idempotency is by construction: consumers fold by key
(``event_id``) over deduplicated events (ADR-019) — writing the same
derived events twice changes nothing after the fold.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml

from debian_observer.normalization.britney import parse_snapshot
from debian_observer.normalization.eventizers import get_eventizer
from debian_observer.storage.parquet import (
    EVENTS_DIR_DEFAULT,
    ParquetWriter,
    event_type_directory,
    schema_for_event_type,
    writer_for_event_type,
)

# ---------------------------------------------------------------------------
# Synthetic, deterministic events
# ---------------------------------------------------------------------------


ALPHA = {
    "item-name": "alpha",
    "source": "alpha",
    "old-version": "1.0",
    "new-version": "1.1",
    "migration-policy-verdict": "PASS",
    "is-candidate": True,
    "reason": ["waiting for 5 days"],
    "dependencies": {"blocked-by": ["beta", "gamma"]},
}


def parsed(snapshot_id: str, generated_date: str, items: list[dict]):
    doc = {"generated-date": generated_date, "sources": items}
    return parse_snapshot(
        snapshot_id, gzip.compress(yaml.safe_dump(doc).encode())
    )


SEED = parsed("20260601T010000Z_0001", "2026-06-01T01:00:00Z", [ALPHA])
NEXT = parsed(
    "20260601T020000Z_0001",
    "2026-06-01T02:00:00Z",
    [dict(ALPHA, **{"new-version": "1.2"})],
)

EVENTIZER = get_eventizer("britney", "excuses")

#: All event types the window produces: every one needs a schema.
WINDOW_EVENT_TYPES = sorted(
    {e.event_type for e in EVENTIZER.eventize_window([SEED, NEXT])}
)


# ---------------------------------------------------------------------------
# The schemas (§10)
# ---------------------------------------------------------------------------


class TestSchemas:
    """Every event type of the window registers a Parquet schema."""

    def test_every_window_event_type_has_a_schema(self) -> None:
        for event_type in WINDOW_EVENT_TYPES:
            assert schema_for_event_type(event_type).names[0] == "event_id"

    def test_the_directory_names_are_snake_case(self) -> None:
        assert event_type_directory("SnapshotObserved") == (
            "snapshot_observed"
        )

    def test_the_lake_default_dir_is_the_prosumers_input(self) -> None:
        assert EVENTS_DIR_DEFAULT == "data/observed"


# ---------------------------------------------------------------------------
# The lake (§10)
# ---------------------------------------------------------------------------


def write_window(events: list, lake: Path, run: str) -> None:
    """One lake file per event type per run, as the run writes them."""
    by_type: dict[str, list] = {}
    for event in events:
        by_type.setdefault(event.event_type, []).append(event)
    for event_type, batch in by_type.items():
        writer_for_event_type(event_type, events_dir=lake).write_events(
            batch, ingestion_run_id=run
        )


class TestEventLake:
    """One directory per event type, readable with a plain glob."""

    def test_written_events_read_back_with_a_plain_glob(
        self, tmp_path: Path
    ) -> None:
        events = list(EVENTIZER.eventize_window([SEED, NEXT]))
        write_window(events, tmp_path / "lake", "run-lake")

        for event_type in WINDOW_EVENT_TYPES:
            directory = tmp_path / "lake" / event_type_directory(event_type)
            files = sorted(directory.glob("*.parquet"))
            assert files, f"no lake file for {event_type}"
            table = pq.read_table(files[0])
            assert "event_id" in table.column_names
            assert table.num_rows >= 1
            # No union_by_name, no hive_partitioning: a plain read of
            # one schema-homogeneous directory (§10, ADR-016).
            assert table.schema.equals(schema_for_event_type(event_type))

    def test_list_payload_fields_flatten_as_json_columns(
        self, tmp_path: Path
    ) -> None:
        observed = [
            e for e in EVENTIZER.eventize_seed(
                SEED.states, snapshot=SEED.metadata
            )
            if e.event_type == "ExcuseObserved"
        ][0]
        writer = writer_for_event_type(
            "ExcuseObserved", events_dir=tmp_path / "lake"
        )
        writer.write_events([observed], ingestion_run_id="run-lake")
        row = pq.read_table(
            next((tmp_path / "lake" / "excuse_observed").glob("*.parquet"))
        ).to_pylist()[0]
        assert row["event_id"] == str(observed.event_id)
        assert row["item_name"] == "alpha"
        assert json.loads(row["blocked_by"]) == ["beta", "gamma"]
        assert json.loads(row["reason"]) == ["waiting for 5 days"]

    def test_the_lake_keeps_the_migrate_after_change(
        self, tmp_path: Path
    ) -> None:
        """The migrate-after change lands in its own typed directory."""
        before = SEED
        after = parsed(
            "20260601T020000Z_0001",
            "2026-06-01T02:00:00Z",
            [
                dict(
                    ALPHA,
                    dependencies={
                        "blocked-by": ["beta", "gamma"],
                        "migrate-after": ["beta"],
                    },
                )
            ],
        )
        events = list(EVENTIZER.eventize_window([before, after]))
        changed = [
            e for e in events if e.event_type == "ExcuseMigrateAfterChanged"
        ]
        assert [e.entity_id for e in changed] == ["alpha"]
        assert changed[0].payload["before"] == []
        assert changed[0].payload["after"] == ["beta"]
        write_window(events, tmp_path / "lake", "run-lake")
        files = sorted(
            (tmp_path / "lake" / "excuse_migrate_after_changed").glob(
                "*.parquet"
            )
        )
        assert files, "no lake file for ExcuseMigrateAfterChanged"
        table = pq.read_table(files[0])
        assert table.schema.equals(
            schema_for_event_type("ExcuseMigrateAfterChanged")
        )
        assert table.column_names == schema_for_event_type(
            "ExcuseMigrateAfterChanged"
        ).names
        assert table.num_rows == 1

    def test_the_fold_by_event_id_absorbs_a_duplicate_run(
        self, tmp_path: Path
    ) -> None:
        """Idempotent by construction: fold by key (ADR-019)."""
        events = list(EVENTIZER.eventize_window([SEED, NEXT]))
        lake = tmp_path / "lake"
        # A clean run, then the same events written again — as a
        # replay beyond any dedup window would deliver them.
        for run in ("run-clean", "run-replayed"):
            write_window(events, lake, run)

        for event_type in WINDOW_EVENT_TYPES:
            directory = lake / event_type_directory(event_type)
            rows = []
            for path in sorted(directory.glob("*.parquet")):
                rows.extend(pq.read_table(path).to_pylist())
            expected = {
                str(e.event_id)
                for e in events
                if e.event_type == event_type
            }
            folded = {row["event_id"] for row in rows}
            assert folded == expected

    def test_writing_no_events_writes_no_file(self, tmp_path: Path) -> None:
        writer = ParquetWriter(
            output_dir=tmp_path / "lake" / "excuse_observed",
            schema=schema_for_event_type("ExcuseObserved"),
        )
        # The empty run must not create a file: a re-run with zero
        # new events rewrites nothing.
        with pytest.raises(ValueError, match="empty"):
            writer.write_events([], ingestion_run_id="run-empty")