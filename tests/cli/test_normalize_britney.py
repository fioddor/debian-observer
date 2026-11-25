"""CLI: `normalize britney` — parse, pair, checkpoint, keep the lake.

Pins docs/release3.project.v0.1.md §9.1 and §8→§9 hand-off: the
command reads a window manifest written by `ingest britney`, parses
each observed snapshot from the raw store (verifying SHA-256), seeds
the first snapshot, pairs the rest sequentially, writes the events to
the Parquet event lake (one directory per event type, one file per
run) and advances a checkpoint per eventized snapshot. A restart
resumes after the checkpoint; re-running a published window writes
zero new events and rewrites none of the old ones.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import yaml
from click.testing import CliRunner

from debian_observer.cli.main import cli
from debian_observer.ingestion.britney import sha256_bytes
from debian_observer.storage.parquet import event_type_directory

# ---------------------------------------------------------------------------
# Synthetic, deterministic window, as `ingest britney` would store it
# ---------------------------------------------------------------------------

ALPHA = {
    "item-name": "alpha",
    "source": "alpha",
    "old-version": "1.0",
    "new-version": "1.1",
    "migration-policy-verdict": "PASS",
    "is-candidate": True,
    "dependencies": {"blocked-by": ["beta"]},
}


def gz_doc(items: list[dict[str, Any]], generated_date: str) -> bytes:
    doc = {"generated-date": generated_date, "sources": items}
    return gzip.compress(yaml.safe_dump(doc).encode())


#: A three-snapshot window: seed, one entered item, one version change.
WINDOW: list[tuple[str, str, list[dict[str, Any]]]] = [
    ("20260601T010000Z_0001", "2026-06-01T01:00:00Z", [ALPHA]),
    (
        "20260601T020000Z_0001",
        "2026-06-01T02:00:00Z",
        [dict(ALPHA), {"item-name": "beta"}],
    ),
    (
        "20260601T030000Z_0001",
        "2026-06-01T03:00:00Z",
        [dict(ALPHA, **{"new-version": "1.2"}), {"item-name": "beta"}],
    ),
]
YEAR = 2026
EVENT_COUNT = 6  # seed: observed + frame; then one delta + frame per pair
LAST_ID = WINDOW[-1][0]


def stored_window(tmp_path: Path) -> tuple[Path, Path]:
    """A raw store plus its window manifest, as `ingest britney` writes."""
    raw = tmp_path / "raw"
    entries = []
    for snapshot_id, generated_date, items in WINDOW:
        body = gz_doc(items, generated_date)
        path = raw / "britney" / str(YEAR) / f"{snapshot_id}.yaml.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        entries.append(
            {
                "snapshot_id": snapshot_id,
                "url": (
                    "https://release.debian.org/britney/update_excuses/"
                    f"{YEAR}/{snapshot_id}.yaml.gz"
                ),
                "sha256": sha256_bytes(body),
                "generated_date": generated_date,
                "item_count": len(items),
                "top_level_keys": ["generated-date", "sources"],
            }
        )
    manifest = {
        "source": "britney",
        "run_id": "run-ingest",
        "year": YEAR,
        "snapshots": entries,
        "gaps": [],
        "failures": [],
    }
    manifest_path = tmp_path / f"manifest-run-ingest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return raw, manifest_path


def lake_files(lake: Path) -> list[Path]:
    return sorted(lake.rglob("*.parquet"))


def lake_event_ids(lake: Path) -> set[str]:
    ids: set[str] = set()
    for path in lake_files(lake):
        ids.update(pq.read_table(path).column("event_id").to_pylist())
    return ids


# ---------------------------------------------------------------------------
# The command
# ---------------------------------------------------------------------------


class TestNormalizeBritney:
    """`normalize britney` eventizes an ingested window, resumably."""

    def test_help_documents_the_perimeter(self) -> None:
        result = CliRunner().invoke(
            cli, ["normalize", "britney", "--help"]
        )
        assert result.exit_code == 0
        for word in (
            "--eventizer",
            "--events-dir",
            "--checkpoint",
            "excuses",
        ):
            assert word in result.output

    def test_a_window_run_eventizes_seed_and_pairs(self, tmp_path) -> None:
        raw, manifest = stored_window(tmp_path)
        lake = tmp_path / "lake"
        checkpoint = tmp_path / "checkpoint.json"
        result = CliRunner().invoke(
            cli,
            [
                "normalize", "britney", str(manifest),
                "--raw-dir", str(raw),
                "--events-dir", str(lake),
                "--checkpoint", str(checkpoint),
            ],
        )
        assert result.exit_code == 0, result.output
        assert f"{len(WINDOW)} snapshots" in result.output
        # The lake holds one directory per event type, with files.
        assert (lake / "excuse_observed").glob("*.parquet")
        assert (lake / "snapshot_observed").glob("*.parquet")
        assert len(lake_event_ids(lake)) == EVENT_COUNT

    def test_the_checkpoint_records_the_last_snapshot(
        self, tmp_path
    ) -> None:
        raw, manifest = stored_window(tmp_path)
        checkpoint = tmp_path / "checkpoint.json"
        result = CliRunner().invoke(
            cli,
            [
                "normalize", "britney", str(manifest),
                "--raw-dir", str(raw),
                "--events-dir", str(tmp_path / "lake"),
                "--checkpoint", str(checkpoint),
            ],
        )
        assert result.exit_code == 0
        record = json.loads(checkpoint.read_text(encoding="utf-8"))
        assert record["last_snapshot"] == LAST_ID
        assert record["eventizer"] == "excuses"

    def test_a_rerun_without_changes_writes_zero_new_events(
        self, tmp_path
    ) -> None:
        """Resumable AND idempotent: nothing re-derived, nothing
        re-written (§9.3, §15)."""
        raw, manifest = stored_window(tmp_path)
        lake = tmp_path / "lake"
        checkpoint = tmp_path / "checkpoint.json"
        args = [
            "normalize", "britney", str(manifest),
            "--raw-dir", str(raw),
            "--events-dir", str(lake),
            "--checkpoint", str(checkpoint),
        ]
        first = CliRunner().invoke(cli, args)
        assert first.exit_code == 0, first.output
        files_after_first = lake_files(lake)
        ids_after_first = lake_event_ids(lake)

        second = CliRunner().invoke(cli, args)
        assert second.exit_code == 0, second.output
        assert "0 new events" in second.output
        # Nothing re-written: the lake did not grow.
        assert lake_files(lake) == files_after_first
        assert lake_event_ids(lake) == ids_after_first

    def test_a_new_snapshot_resumes_after_the_checkpoint(
        self, tmp_path
    ) -> None:
        """A restart (or a window that grew) resumes after the last
        published pair: only the new pair is eventized."""
        raw, manifest = stored_window(tmp_path)
        lake = tmp_path / "lake"
        checkpoint = tmp_path / "checkpoint.json"
        args = [
            "normalize", "britney", str(manifest),
            "--raw-dir", str(raw),
            "--events-dir", str(lake),
            "--checkpoint", str(checkpoint),
        ]
        assert CliRunner().invoke(cli, args).exit_code == 0
        ids_first = lake_event_ids(lake)

        # The source publishes one more snapshot.
        snapshot_id = "20260601T040000Z_0001"
        body = gz_doc([dict(ALPHA)], "2026-06-01T04:00:00Z")
        path = raw / "britney" / str(YEAR) / f"{snapshot_id}.yaml.gz"
        path.write_bytes(body)
        window = json.loads(manifest.read_text(encoding="utf-8"))
        window["snapshots"].append(
            {
                "snapshot_id": snapshot_id,
                "url": f"https://x/{snapshot_id}.yaml.gz",
                "sha256": sha256_bytes(body),
                "generated_date": "2026-06-01T04:00:00Z",
                "item_count": 1,
                "top_level_keys": ["generated-date", "sources"],
            }
        )
        manifest.write_text(json.dumps(window), encoding="utf-8")

        result = CliRunner().invoke(cli, args)
        assert result.exit_code == 0, result.output
        # Only the new pair: the old events are still there, plus the
        # new deltas — and beta leaving is a delta against its
        # previous state, not a re-derivation of the seed.
        new_ids = lake_event_ids(lake) - ids_first
        assert new_ids
        # The frame of the resumed pair names its predecessor.
        frames = [
            row
            for path in (lake / "snapshot_observed").glob("*.parquet")
            for row in pq.read_table(path).to_pylist()
        ]
        resumed = [r for r in frames if r["snapshot"] == snapshot_id]
        assert resumed and resumed[-1]["prev_snapshot"] == LAST_ID

    def test_a_manifest_of_another_source_is_rejected(self, tmp_path) -> None:
        raw, _ = stored_window(tmp_path)
        other = tmp_path / "manifest-other.json"
        other.write_text(json.dumps({"source": "udddump"}), encoding="utf-8")
        result = CliRunner().invoke(
            cli,
            [
                "normalize", "britney", str(other),
                "--raw-dir", str(raw),
                "--events-dir", str(tmp_path / "lake"),
                "--checkpoint", str(tmp_path / "checkpoint.json"),
            ],
        )
        assert result.exit_code != 0
        assert "udddump" in result.output

    def test_a_corrupted_raw_file_is_rejected(self, tmp_path) -> None:
        raw, manifest = stored_window(tmp_path)
        victim = raw / "britney" / str(YEAR) / f"{WINDOW[0][0]}.yaml.gz"
        victim.write_bytes(b"not the published bytes")
        result = CliRunner().invoke(
            cli,
            [
                "normalize", "britney", str(manifest),
                "--raw-dir", str(raw),
                "--events-dir", str(tmp_path / "lake"),
                "--checkpoint", str(tmp_path / "checkpoint.json"),
            ],
        )
        assert result.exit_code != 0
        assert "sha256" in result.output.lower()