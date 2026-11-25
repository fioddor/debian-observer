"""Normalization: the britney parse — snapshot YAML to compact states.

Pins docs/release3.project.v0.1.md §9.2 (the parse) and §5.1 (the
entities): one observed snapshot becomes one ``SnapshotMetadata`` plus
one compact ``ExcuseState`` per item. The parse extends — without
duplicating — the acquisition's minimal metadata read (C1,
``ingestion.britney.read_snapshot_metadata``): the safe loader choice
and the raw-byte helpers come from the ingestion module, only the full
document read lives here.

Synthetic, deterministic snapshots only: the battery never reaches
release.debian.org and never sleeps.
"""

from __future__ import annotations

import gzip
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from debian_observer.ingestion.britney import (
    DuplicateItemName,
    sha256_bytes,
)
from debian_observer.normalization.britney import (
    ExcuseState,
    ParsedSnapshot,
    REQUIRED_TOP_LEVEL_KEYS,
    SnapshotMetadata,
    TopLevelKeyDrift,
    parse_snapshot,
)

# ---------------------------------------------------------------------------
# Synthetic, deterministic snapshots
# ---------------------------------------------------------------------------

#: The item fields britney publishes (§4.1.1), in one full item.
ALPHA_ITEM: dict[str, Any] = {
    "item-name": "alpha",
    "source": "alpha",
    "old-version": "1.0",
    "new-version": "1.1",
    "migration-policy-verdict": "PASS",
    "is-candidate": True,
    "reason": ["waiting for 5 days"],
    "dependencies": {
        "blocked-by": ["beta", "gamma"],
        "migrate-after": ["delta"],
    },
}

#: An item without ``dependencies``: absent in ~33 % of items — the
#: absence is a valid value (§4.1.1), never an error.
BETA_ITEM: dict[str, Any] = {
    "item-name": "beta",
    "source": "beta-src",
    "old-version": "2.0",
    "new-version": "2.1",
    "migration-policy-verdict": "REJECTED_TEMPORARILY",
    "is-candidate": False,
    "reason": "not a list in the wild",
}

#: The verdict set is OPEN (§4.1.1): an unknown verdict is kept
#: verbatim and counted, never rejected.
UNKNOWN_VERDICT = "REJECTED_SOMETHING_NOBODY_SAW_YET"
GAMMA_ITEM: dict[str, Any] = {
    "item-name": "gamma",
    "source": "gamma",
    "old-version": "3.0",
    "new-version": "3.2",
    "migration-policy-verdict": UNKNOWN_VERDICT,
    "is-candidate": True,
    "reason": ["blocked from source"],
    "dependencies": {"blocked-by": []},
}

GENERATED_DATE = datetime(2026, 6, 1, 1, 0, 0, tzinfo=timezone.utc)
SNAPSHOT_ID = "20260601T010000Z_0001"
SNAPSHOT_URL = "https://release.debian.org/britney/update_excuses/2026/20260601T010000Z_0001.yaml.gz"


def document(items: list[dict[str, Any]], **extra: Any) -> dict[str, Any]:
    """One excuses document as britney publishes it (§4.1.1)."""
    return {"generated-date": GENERATED_DATE, "sources": items, **extra}


def gzipped(doc: dict[str, Any]) -> bytes:
    """The document as a raw archive file: YAML compressed with gzip."""
    return gzip.compress(yaml.safe_dump(doc).encode())


def parsed(items: list[dict[str, Any]], **extra: Any) -> ParsedSnapshot:
    """A parsed snapshot of one document, straight from its raw bytes."""
    return parse_snapshot(
        SNAPSHOT_ID, gzipped(document(items, **extra)), url=SNAPSHOT_URL
    )


# ---------------------------------------------------------------------------
# The snapshot metadata (§5.1 Snapshot)
# ---------------------------------------------------------------------------


class TestSnapshotMetadata:
    """One britney run becomes one Snapshot (§5.1)."""

    def test_the_metadata_carries_the_run_identity(self) -> None:
        metadata = parsed([ALPHA_ITEM]).metadata
        assert isinstance(metadata, SnapshotMetadata)
        assert metadata.snapshot_id == SNAPSHOT_ID
        assert metadata.url == SNAPSHOT_URL
        assert metadata.generated_date == GENERATED_DATE

    def test_the_sha256_is_of_the_raw_gz_bytes(self) -> None:
        body = gzipped(document([ALPHA_ITEM]))
        metadata = parse_snapshot(SNAPSHOT_ID, body).metadata
        assert metadata.sha256 == sha256_bytes(body)
        assert metadata.sha256 == hashlib.sha256(body).hexdigest()

    def test_the_item_count_counts_the_items(self) -> None:
        assert parsed([ALPHA_ITEM, BETA_ITEM]).metadata.item_count == 2

    def test_the_top_level_keys_are_sorted(self) -> None:
        keys = parsed([ALPHA_ITEM], extra_key=1).metadata.top_level_keys
        assert keys == tuple(sorted(("generated-date", "sources", "extra_key")))

    def test_the_required_top_level_keys_are_the_known_minimum(self) -> None:
        """The drift gate compares against this constant, never a copy."""
        assert REQUIRED_TOP_LEVEL_KEYS == ("generated-date", "sources")


# ---------------------------------------------------------------------------
# The compact states (§5.1 ExcuseState)
# ---------------------------------------------------------------------------


class TestExcuseState:
    """One item in one snapshot becomes one compact ExcuseState."""

    def test_one_state_per_item_keyed_by_item_name(self) -> None:
        snapshot = parsed([ALPHA_ITEM, BETA_ITEM])
        assert set(snapshot.states) == {"alpha", "beta"}
        assert snapshot.metadata.item_count == len(snapshot.states)

    def test_the_state_keeps_the_fields_it_needs(self) -> None:
        state = snapshot_state = parsed([ALPHA_ITEM]).states["alpha"]
        assert isinstance(state, ExcuseState)
        assert state.item_name == "alpha"
        assert state.source == "alpha"
        assert state.old_version == "1.0"
        assert state.new_version == "1.1"
        assert state.verdict == "PASS"
        assert state.is_candidate is True

    def test_dependencies_read_as_tuples(self) -> None:
        state = parsed([ALPHA_ITEM]).states["alpha"]
        assert tuple(state.blocked_by) == ("beta", "gamma")
        assert tuple(state.migrate_after) == ("delta",)
        assert tuple(state.reason) == ("waiting for 5 days",)

    def test_absent_dependencies_are_a_valid_empty_value(self) -> None:
        """~33 % of items carry no dependencies (§4.1.1): no error."""
        state = parsed([BETA_ITEM]).states["beta"]
        assert state.blocked_by == ()
        assert state.migrate_after == ()

    def test_a_scalar_reason_is_carried_as_a_one_element_list(self) -> None:
        state = parsed([BETA_ITEM]).states["beta"]
        assert tuple(state.reason) == ("not a list in the wild",)

    def test_an_absent_reason_is_an_empty_value(self) -> None:
        item = dict(BETA_ITEM)
        item.pop("reason")
        assert parsed([item]).states["beta"].reason == ()

    def test_an_absent_is_candidate_defaults_to_false(self) -> None:
        item = dict(BETA_ITEM)
        item.pop("is-candidate")
        assert parsed([item]).states["beta"].is_candidate is False

    def test_an_unknown_verdict_is_kept_verbatim(self) -> None:
        """The verdict set is open: never rejected, never renamed."""
        state = parsed([GAMMA_ITEM]).states["gamma"]
        assert state.verdict == UNKNOWN_VERDICT

    def test_an_empty_blocked_by_is_kept_verbatim(self) -> None:
        assert parsed([GAMMA_ITEM]).states["gamma"].blocked_by == ()


# ---------------------------------------------------------------------------
# Invalid records (§5.2)
# ---------------------------------------------------------------------------


class TestInvalidRecords:
    """A duplicate is an invalid record with its cause (§5.2)."""

    def test_a_duplicate_item_name_is_an_error_with_cause(self) -> None:
        with pytest.raises(DuplicateItemName, match="alpha"):
            parsed([ALPHA_ITEM, dict(ALPHA_ITEM)])

    def test_the_duplicate_names_the_snapshot(self) -> None:
        with pytest.raises(DuplicateItemName, match=SNAPSHOT_ID):
            parsed([ALPHA_ITEM, dict(ALPHA_ITEM)])

    def test_an_item_without_a_name_is_an_error(self) -> None:
        item = dict(ALPHA_ITEM)
        item.pop("item-name")
        with pytest.raises(ValueError, match=SNAPSHOT_ID):
            parsed([item])

    def test_a_non_mapping_item_is_an_error(self) -> None:
        with pytest.raises(ValueError, match=SNAPSHOT_ID):
            parsed(["not-a-mapping"])


class TestTopLevelKeyDrift:
    """A format change is detected, not silently absorbed (§4.1.1)."""

    def test_a_missing_generated_date_is_reported(self) -> None:
        doc = {"sources": [ALPHA_ITEM]}
        with pytest.raises(TopLevelKeyDrift, match="generated-date"):
            parse_snapshot(SNAPSHOT_ID, gzipped(doc))

    def test_a_missing_sources_key_is_reported(self) -> None:
        doc = {"generated-date": GENERATED_DATE}
        with pytest.raises(TopLevelKeyDrift, match="sources"):
            parse_snapshot(SNAPSHOT_ID, gzipped(doc))

    def test_the_drift_reports_the_observed_keys(self) -> None:
        doc = {"generated-date": GENERATED_DATE, "renamed-sources": []}
        with pytest.raises(TopLevelKeyDrift, match="renamed-sources"):
            parse_snapshot(SNAPSHOT_ID, gzipped(doc))

    def test_a_non_mapping_document_is_an_error(self) -> None:
        with pytest.raises(ValueError, match=SNAPSHOT_ID):
            parse_snapshot(SNAPSHOT_ID, gzipped("just a string"))

    def test_a_truncated_gzip_is_an_error(self) -> None:
        body = gzipped(document([ALPHA_ITEM]))[:-4]
        with pytest.raises(ValueError, match="decompress"):
            parse_snapshot(SNAPSHOT_ID, body)