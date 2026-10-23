"""Unit tests for storage.consolidation: the fold + the merge.

The consolidation fold (moved from consumer/packages on 2026-10-23 —
writer-owned, ADR-011) turns observations into the consolidated
``package_versions`` table. Two feeders share one semantics (spec
§15.1): the batch feeder (``consolidate`` CLI: fold the whole log)
and the streaming feeder (``ConsolidationMerge.feed`` + ``write``:
one event at a time, RMW of the existing table). The pinned
invariant: ``fold(log parquet) == fold(stream)`` — the same events
by either path produce the identical table.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq
import pytest

from debian_observer.cli.commands.publish import row_to_envelope
from debian_observer.events import EventEnvelope
from debian_observer.storage.consolidation import (
    CONSOLIDATED_COLUMNS,
    OUTPUT_FILENAME,
    ConsolidateError,
    ConsolidationMerge,
    FoldStats,
    build_consolidated_packages,
    fold_package_versions,
    read_event_log,
    row_from_envelope,
    write_consolidated_table,
)
from debian_observer.storage.parquet import ParquetWriter

NOW = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc)


def _pvo_event(
    *,
    event_id: str | None = None,
    package_id: str = "vim:amd64",
    source_package: str = "vim",
    version: str = "2:9.0-1",
    distribution: str = "debian",
    release: str | None = "sid",
    suite: str | None = None,
    architecture: str = "amd64",
    component: str = "main",
    section: str = "editors",
    priority: str = "optional",
    source: str = "udd",
    observed_at: datetime = NOW,
) -> EventEnvelope:
    return EventEnvelope(
        event_id=event_id or uuid4(),
        event_type="PackageVersionObserved",
        source=source,
        entity_id=package_id,
        payload={
            "package_id": package_id,
            "source_package": source_package,
            "version": version,
            "distribution": distribution,
            "release": release,
            "suite": suite,
            "architecture": architecture,
            "component": component,
            "section": section,
            "priority": priority,
        },
        observed_at=observed_at,
        effective_at=observed_at,
    )


def _other_event(observed_at: datetime = NOW) -> EventEnvelope:
    return EventEnvelope(
        event_id=uuid4(),
        event_type="PackageObserved",
        source="udd",
        entity_id="vim",
        payload={"package_id": "vim", "name": "vim"},
        observed_at=observed_at,
        effective_at=observed_at,
    )


def _write_log(
    events_dir: Path, batches: list[list[EventEnvelope]]
) -> list[Path]:
    """Write one Parquet event-log file per batch (the R1 layout)."""
    writer = ParquetWriter(output_dir=events_dir)
    return [
        writer.write_events(batch, ingestion_run_id=f"run-{i}")
        for i, batch in enumerate(batch for batch in batches if batch)
    ]


class TestFoldSemantics:
    def test_same_pk_folds_to_one_row(self, tmp_path: Path):
        events_dir = tmp_path / "events"
        events_dir.mkdir()
        later = NOW + timedelta(hours=1)
        _write_log(
            events_dir,
            [
                [
                    _pvo_event(observed_at=NOW, section="old"),
                    _pvo_event(observed_at=later, section="editors"),
                ]
            ],
        )

        rows, stats = fold_package_versions(events_dir)

        assert len(rows) == 1
        row = rows[0]
        # last-observed-wins: the later observation's attributes
        assert row["section"] == "editors"
        # min/max absorb the duplicate PK group
        assert row["observed_first_at"] == NOW.isoformat()
        assert row["observed_last_at"] == later.isoformat()
        assert stats.pvo_events == 2

    def test_distinct_releases_do_not_collapse(self, tmp_path: Path):
        """The PK carries the full coordinates: the same package
        version in two releases is two rows, not one."""
        events_dir = tmp_path / "events"
        events_dir.mkdir()
        _write_log(
            events_dir,
            [
                [
                    _pvo_event(release="sid"),
                    _pvo_event(release="bookworm"),
                ]
            ],
        )

        rows, _ = fold_package_versions(events_dir)

        assert len(rows) == 2
        assert {r["release"] for r in rows} == {"sid", "bookworm"}

    def test_full_pk_coordinates_distinguish_rows(self, tmp_path: Path):
        """suite/architecture are PK members: same version, same
        release, different suite or architecture → distinct rows."""
        events_dir = tmp_path / "events"
        events_dir.mkdir()
        _write_log(
            events_dir,
            [
                [
                    _pvo_event(suite="stable"),
                    _pvo_event(suite="unstable"),
                    _pvo_event(architecture="arm64"),
                ]
            ],
        )

        rows, _ = fold_package_versions(events_dir)
        assert len(rows) == 3

    def test_dedup_by_event_id(self, tmp_path: Path):
        """The same event_id in two log files is one observation."""
        events_dir = tmp_path / "events"
        events_dir.mkdir()
        event = _pvo_event(event_id="00000000-0000-4000-8000-000000000001")
        _write_log(events_dir, [[event], [event]])

        rows, stats = fold_package_versions(events_dir)

        assert len(rows) == 1
        assert stats.pvo_events == 1
        assert stats.events_read == 2

    def test_last_wins_orders_by_observed_at_then_event_id(
        self, tmp_path: Path
    ):
        """Ties on observed_at break by event_id — deterministic."""
        events_dir = tmp_path / "events"
        events_dir.mkdir()
        _write_log(
            events_dir,
            [
                [
                    _pvo_event(
                        event_id="00000000-0000-4000-8000-00000000000a",
                        observed_at=NOW,
                        component="old",
                    ),
                    _pvo_event(
                        event_id="00000000-0000-4000-8000-00000000000b",
                        observed_at=NOW,
                        component="contrib",
                    ),
                ]
            ],
        )

        rows, _ = fold_package_versions(events_dir)
        assert rows[0]["component"] == "contrib"

    def test_non_pvo_events_are_skipped(self, tmp_path: Path):
        events_dir = tmp_path / "events"
        events_dir.mkdir()
        _write_log(
            events_dir,
            [
                [
                    _pvo_event(),
                    _other_event(),
                    _other_event(NOW + timedelta(minutes=1)),
                ]
            ],
        )

        rows, stats = fold_package_versions(events_dir)

        assert len(rows) == 1
        assert stats.events_read == 3
        assert stats.pvo_events == 1

    def test_fold_is_idempotent_over_the_log(self, tmp_path: Path):
        """Fold(log) == Fold(log) — and fold(log + duplicated log)
        collapses back to the same rows (spec §15.1: A×3 == A)."""
        events_dir = tmp_path / "events"
        events_dir.mkdir()
        event = _pvo_event()
        _write_log(events_dir, [[event], [event]])

        rows_once, _ = fold_package_versions(events_dir)
        rows_twice, _ = fold_package_versions(events_dir)

        assert rows_once == rows_twice

    def test_rows_are_sorted_by_pk(self, tmp_path: Path):
        """Output order is deterministic: sorted by the PK tuple."""
        events_dir = tmp_path / "events"
        events_dir.mkdir()
        _write_log(
            events_dir,
            [
                [
                    _pvo_event(package_id="vim:amd64"),
                    _pvo_event(package_id="bash:amd64"),
                ]
            ],
        )

        rows, _ = fold_package_versions(events_dir)
        assert [r["package_id"] for r in rows] == ["bash:amd64", "vim:amd64"]


class TestFoldErrors:
    def test_events_dir_without_parquet_files(self, tmp_path: Path):
        events_dir = tmp_path / "events"
        events_dir.mkdir()

        with pytest.raises(ConsolidateError) as excinfo:
            fold_package_versions(events_dir)

        # The error names the directory, never a traceback.
        assert str(events_dir) in str(excinfo.value)

    def test_zero_pvo_events_in_the_log(self, tmp_path: Path):
        events_dir = tmp_path / "events"
        events_dir.mkdir()
        _write_log(events_dir, [[_other_event()]])

        with pytest.raises(ConsolidateError, match="PackageVersionObserved"):
            fold_package_versions(events_dir)

    def test_missing_events_dir_is_an_error(self, tmp_path: Path):
        with pytest.raises(ConsolidateError):
            fold_package_versions(tmp_path / "nope")


class TestWriteOutput:
    def test_build_writes_one_fixed_name_file(self, tmp_path: Path):
        events_dir = tmp_path / "events"
        events_dir.mkdir()
        dataset_dir = tmp_path / "dataset"
        _write_log(events_dir, [[_pvo_event()]])

        out = build_consolidated_packages(
            events_dir, dataset_dir=dataset_dir
        )

        assert out == dataset_dir / "package_versions.parquet"
        assert out.exists()
        # ONE file in the dataset dir — no per-run fragments.
        assert [p.name for p in dataset_dir.iterdir()] == [
            "package_versions.parquet"
        ]

    def test_schema_pins_13_columns(self, tmp_path: Path):
        events_dir = tmp_path / "events"
        events_dir.mkdir()
        dataset_dir = tmp_path / "dataset"
        _write_log(events_dir, [[_pvo_event()]])

        out = build_consolidated_packages(
            events_dir, dataset_dir=dataset_dir
        )

        table = pq.read_table(out)
        assert table.column_names == list(CONSOLIDATED_COLUMNS)
        assert len(CONSOLIDATED_COLUMNS) == 13

    def test_write_is_atomic_no_tmp_left_behind(self, tmp_path: Path):
        dataset_dir = tmp_path / "dataset"
        rows = [
            {
                "package_id": "vim:amd64",
                "source_package": "vim",
                "version": "2:9.0-1",
                "distribution": "debian",
                "release": "sid",
                "suite": "",
                "architecture": "amd64",
                "component": "main",
                "section": "editors",
                "priority": "optional",
                "observed_first_at": NOW.isoformat(),
                "observed_last_at": NOW.isoformat(),
                "source": "udd",
            }
        ]

        out = write_consolidated_table(rows, dataset_dir=dataset_dir)

        assert out.exists()
        assert list(dataset_dir.iterdir()) == [out]

    def test_write_failure_cleans_the_tmp_file(self, tmp_path: Path):
        dataset_dir = tmp_path / "dataset"

        with pytest.raises(Exception):
            # Non-serializable rows crash the writer mid-write.
            write_consolidated_table([{"package_id": object()}], dataset_dir=dataset_dir)

        assert not list(dataset_dir.glob("*.tmp"))
        assert not list(dataset_dir.glob(".*"))

    def test_regenerating_the_same_log_is_stable(self, tmp_path: Path):
        events_dir = tmp_path / "events"
        events_dir.mkdir()
        dataset_dir = tmp_path / "dataset"
        _write_log(events_dir, [[_pvo_event(), _pvo_event(release="bookworm")]])

        first = build_consolidated_packages(
            events_dir, dataset_dir=dataset_dir
        )
        first_rows = pq.read_table(first).to_pylist()
        second = build_consolidated_packages(
            events_dir, dataset_dir=dataset_dir
        )
        second_rows = pq.read_table(second).to_pylist()

        assert first == second
        assert first_rows == second_rows


class TestFoldStats:
    def test_stats_count_files_and_events(self, tmp_path: Path):
        events_dir = tmp_path / "events"
        events_dir.mkdir()
        _write_log(
            events_dir,
            [
                [_pvo_event()],
                [_pvo_event(release="bookworm"), _other_event()],
            ],
        )

        _, stats = fold_package_versions(events_dir)

        assert isinstance(stats, FoldStats)
        assert stats.files_read == 2
        assert stats.events_read == 3
        assert stats.pvo_events == 2
        assert stats.rows == 2


class TestIncrementalFeed:
    """The streaming feeder: feed → write → re-feed — the merge
    absorbs duplicates and rewrites the durable table."""

    def test_feed_absorbs_pvo_and_skips_other_types(self) -> None:
        merge = ConsolidationMerge()
        row = row_from_envelope(_pvo_event())
        other = row_from_envelope(_other_event())

        assert merge.feed(other) is False
        assert merge.feed(row) is True
        assert merge.events_read == 2
        assert merge.pvo_events == 1
        assert len(merge.rows()) == 1

    def test_feed_ignores_duplicate_event_id(self) -> None:
        merge = ConsolidationMerge()
        row = row_from_envelope(_pvo_event())

        assert merge.feed(row) is True
        assert merge.feed(dict(row)) is False
        assert merge.pvo_events == 1

    def test_feed_two_events_same_pk_one_row(self) -> None:
        """Two observations of the same PK fold to one row with the
        later observation's attributes and min/max timestamps."""
        merge = ConsolidationMerge()
        later = NOW + timedelta(hours=2)
        merge.feed(row_from_envelope(_pvo_event(observed_at=NOW, section="old")))
        merge.feed(
            row_from_envelope(_pvo_event(observed_at=later, section="editors"))
        )

        rows = merge.rows()
        assert len(rows) == 1
        assert rows[0]["section"] == "editors"
        assert rows[0]["observed_first_at"] == NOW.isoformat()
        assert rows[0]["observed_last_at"] == later.isoformat()

    def test_feed_out_of_order_observed_at(self) -> None:
        """Feed order does not matter: min/max and last-wins resolve
        by (observed_at, event_id), not by arrival."""
        merge = ConsolidationMerge()
        later = NOW + timedelta(hours=2)
        merge.feed(
            row_from_envelope(_pvo_event(observed_at=later, section="editors"))
        )
        merge.feed(row_from_envelope(_pvo_event(observed_at=NOW, section="old")))

        rows = merge.rows()
        assert len(rows) == 1
        assert rows[0]["section"] == "editors"
        assert rows[0]["observed_first_at"] == NOW.isoformat()
        assert rows[0]["observed_last_at"] == later.isoformat()

    def test_feed_write_refeed_same_event_is_idempotent(
        self, tmp_path: Path
    ) -> None:
        """The crash-test invariant at unit level: feed → write →
        re-feed the same event → write again ⇒ the identical table
        (a redelivered event is absorbed, not duplicated)."""
        dataset_dir = tmp_path / "dataset"
        row = row_from_envelope(_pvo_event())
        merge = ConsolidationMerge()

        merge.feed(row)
        out = merge.write(dataset_dir)
        first = pq.read_table(out).to_pylist()

        merge.feed(dict(row))  # redelivery after the durable write
        out = merge.write(dataset_dir)
        second = pq.read_table(out).to_pylist()

        assert first == second
        assert [p.name for p in dataset_dir.iterdir()] == [OUTPUT_FILENAME]

    def test_write_merges_with_the_existing_table(
        self, tmp_path: Path
    ) -> None:
        """RMW: a fresh merge folds a later observation of the same
        PK into the table a previous process wrote — first/min,
        last/max, later attributes win."""
        dataset_dir = tmp_path / "dataset"
        later = NOW + timedelta(days=1)

        first_merge = ConsolidationMerge()
        first_merge.feed(row_from_envelope(_pvo_event(observed_at=NOW)))
        first_merge.write(dataset_dir)

        second_merge = ConsolidationMerge()
        second_merge.feed(
            row_from_envelope(_pvo_event(observed_at=later, section="devel"))
        )
        out = second_merge.write(dataset_dir)

        rows = pq.read_table(out).to_pylist()
        assert len(rows) == 1
        assert rows[0]["section"] == "devel"
        assert rows[0]["observed_first_at"] == NOW.isoformat()
        assert rows[0]["observed_last_at"] == later.isoformat()

    def test_write_without_merge_existing_regenerates(
        self, tmp_path: Path
    ) -> None:
        """merge_existing=False (the batch feeder): the table is
        exactly the fold of what was fed — a stale prior table is
        replaced, never accumulated into."""
        dataset_dir = tmp_path / "dataset"
        write_consolidated_table(
            [
                {
                    "package_id": "stale:amd64",
                    "source_package": "stale",
                    "version": "1-1",
                    "distribution": "debian",
                    "release": "sid",
                    "suite": "",
                    "architecture": "amd64",
                    "component": "main",
                    "section": "oldlibs",
                    "priority": "extra",
                    "observed_first_at": NOW.isoformat(),
                    "observed_last_at": NOW.isoformat(),
                    "source": "udd",
                }
            ],
            dataset_dir=dataset_dir,
        )

        merge = ConsolidationMerge()
        merge.feed(row_from_envelope(_pvo_event()))
        out = merge.write(dataset_dir, merge_existing=False)

        rows = pq.read_table(out).to_pylist()
        assert [r["package_id"] for r in rows] == ["vim:amd64"]


class TestStreamParity:
    """The invariant (§19.5 reproducibility): fold(log parquet) ==
    fold(stream) — the same events by either path produce the
    identical table."""

    def test_row_from_envelope_roundtrip(self) -> None:
        """The row adapter flattens exactly the fold-relevant
        columns (the storage.parquet layout)."""
        event = _pvo_event()
        row = row_from_envelope(event)

        assert row["event_id"] == str(event.event_id)
        assert row["event_type"] == "PackageVersionObserved"
        assert row["observed_at"] == NOW.isoformat()
        assert row["package_id"] == "vim:amd64"
        assert row["release"] == "sid"
        # None stays None at the row level (the writer's layout keeps
        # NULL columns NULL); the fold coerces to the string schema.
        assert row["suite"] is None
        assert row["source"] == "udd"

    def test_fold_log_equals_fold_stream(self, tmp_path: Path) -> None:
        events_dir = tmp_path / "events"
        events_dir.mkdir()
        later = NOW + timedelta(hours=3)
        event = _pvo_event(event_id="00000000-0000-4000-8000-000000000001")
        _write_log(
            events_dir,
            [
                [
                    event,
                    _pvo_event(observed_at=later, section="devel"),
                    _other_event(),
                ],
                # the same event_id again in a second file: dedup
                [event],
            ],
        )

        # The batch path: fold the log.
        batch_rows, _ = fold_package_versions(events_dir)

        # The streaming path: the same log, published → consumed →
        # fed one event at a time.
        log_rows, _ = read_event_log(events_dir)
        merge = ConsolidationMerge()
        for log_row in log_rows:
            envelope = row_to_envelope(log_row)
            merge.feed(row_from_envelope(envelope))
        stream_rows = merge.rows()

        assert stream_rows == batch_rows

        # ...and the written tables are identical too.
        batch_dir = tmp_path / "batch"
        stream_dir = tmp_path / "stream"
        write_consolidated_table(batch_rows, dataset_dir=batch_dir)
        merge.write(stream_dir)
        assert (
            pq.read_table(batch_dir / OUTPUT_FILENAME).to_pylist()
            == pq.read_table(stream_dir / OUTPUT_FILENAME).to_pylist()
        )