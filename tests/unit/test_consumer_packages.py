"""Unit tests for consumer/packages: the consolidate fold.

The consumer folds the append-only event log (Parquet, one file per
ingestion run) into the consolidated ``package_versions`` table: one
row per full observation PK
``(package_id, version, distribution, release, suite, architecture)``,
attributes last-observed-wins, ``observed_first_at``/``observed_last_at``
as min/max of the group (spec §15.1 idempotency by construction).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq
import pytest

from debian_observer.consumer.packages import (
    CONSOLIDATED_COLUMNS,
    ConsolidateError,
    FoldStats,
    build_consolidated_packages,
    fold_package_versions,
    write_consolidated_table,
)
from debian_observer.events import EventEnvelope
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
        """Fold(log) == fold(log) — and fold(log + duplicated log)
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