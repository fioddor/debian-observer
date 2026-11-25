"""Normalization: the britney parse — snapshot YAML to compact states.

The expensive half of ``normalize britney``
(docs/release3.project.v0.1.md §9.2): one observed snapshot becomes one
:class:`SnapshotMetadata` (§5.1 Snapshot) plus one compact
:class:`ExcuseState` per item (§5.1 ExcuseState). This is the job the
parse workers run (§11.2); the pair eventizer (ADR-024) consumes its
output.

This parse extends — never duplicates — the acquisition's minimal
metadata read (``ingestion.britney.read_snapshot_metadata``, C1): the
safe loader choice, the raw-byte helpers and the invalid-record
errors come from there; only the full document read lives here.

britney's output is not a stable interface (§4.1.1): the parse keeps
the fields it needs and records each snapshot's top-level keys, so a
format change is detected (a loud :class:`TopLevelKeyDrift`) rather
than silently absorbed.
"""

from __future__ import annotations

import gzip
from datetime import date, datetime, timezone
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator

from debian_observer.ingestion.britney import (
    DuplicateItemName,
    TruncatedGzip,
    _SafeLoader,
    sha256_bytes,
)
import yaml

__all__ = [
    "ExcuseState",
    "ParsedSnapshot",
    "REQUIRED_TOP_LEVEL_KEYS",
    "SnapshotMetadata",
    "TopLevelKeyDrift",
    "parse_snapshot",
]

# ---------------------------------------------------------------------------
# Invalid records and drift
# ---------------------------------------------------------------------------


class TopLevelKeyDrift(ValueError):
    """The document's top-level keys drifted from the known minimum (§4.1.1).

    The source is external YAML whose shape is not a contract: the
    parse requires the keys it needs (:data:`REQUIRED_TOP_LEVEL_KEYS`)
    and reports the observed keys instead of guessing a rename. A drift
    fails the run loudly — it is never silently absorbed.
    """


# ---------------------------------------------------------------------------
# Entities (§5.1)
# ---------------------------------------------------------------------------


class SnapshotMetadata(BaseModel):
    """One britney run (§5.1 Snapshot).

    Attributes:
        snapshot_id: the archive file stem, the run identity across
            the platform.
        generated_date: when britney generated the state (UTC).
        url: the archive URL the raw bytes were fetched from.
        sha256: SHA-256 of the raw ``.yaml.gz`` bytes (ADR-002).
        item_count: how many items the snapshot carries.
        top_level_keys: the document's top-level keys, sorted — the
            format-change detector (§4.1.1).
    """

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    snapshot_id: str
    generated_date: datetime
    url: str | None = None
    sha256: str
    item_count: int = Field(ge=0)
    top_level_keys: tuple[str, ...] = ()

    @field_validator("snapshot_id", "sha256")
    @classmethod
    def _non_empty_str(cls, value: Any) -> Any:
        if not value or not isinstance(value, str):
            raise ValueError("Field must be a non-empty string")
        return value

    @field_validator("generated_date")
    @classmethod
    def _ensure_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:  # YAML timestamps are naive; UTC
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class ExcuseState(BaseModel):
    """One item in one snapshot, compact (§5.1 ExcuseState).

    Attributes:
        item_name: the item's name, unique within its snapshot (§5.2).
        source: the source package the item is an excuse for.
        old_version: the version in testing.
        new_version: the version in unstable.
        verdict: britney's migration-policy verdict, verbatim — the
            set is OPEN (§4.1.1): an unknown verdict is kept and
            counted, never rejected.
        is_candidate: whether the item is a migration candidate.
        blocked_by: the items blocking it (a flat list of item names).
        migrate_after: the items that must migrate first.
        reason: britney's human-readable reasons.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    item_name: str
    source: str | None = None
    old_version: str | None = None
    new_version: str | None = None
    verdict: str | None = None
    is_candidate: bool = False
    blocked_by: tuple[str, ...] = ()
    migrate_after: tuple[str, ...] = ()
    reason: tuple[str, ...] = ()

    @field_validator("item_name")
    @classmethod
    def _item_name_required(cls, value: Any) -> Any:
        if not value or not isinstance(value, str):
            raise ValueError("item-name is required")
        return value


class ParsedSnapshot(BaseModel):
    """The parse of one observed snapshot: metadata plus states.

    ``states`` is keyed by ``item_name``; the acquisition already
    rejects a duplicate item-name as an invalid record (§5.2), so the
    mapping is total over the snapshot's items.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    metadata: SnapshotMetadata
    states: Mapping[str, ExcuseState]


# ---------------------------------------------------------------------------
# The parse (§9.2)
# ---------------------------------------------------------------------------

#: The top-level keys the parse needs (§4.1.1). The observed keys are
#: recorded per snapshot; a document missing one of these is a format
#: change and raises :class:`TopLevelKeyDrift`.
REQUIRED_TOP_LEVEL_KEYS: tuple[str, ...] = ("generated-date", "sources")


def _as_reason_list(value: Any) -> tuple[str, ...]:
    """A reason field as a tuple of strings (§5.1 ``reason`` is a list).

    britney writes a single string in the wild; a scalar is carried as
    a one-element list, everything else must already be one.
    """
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value)
    raise ValueError(f"reason must be a string or a list, got {type(value).__name__}")


def _as_name_list(value: Any) -> tuple[str, ...]:
    """A ``blocked-by``/``migrate-after`` value as a tuple of names.

    The field is a flat list of item names (§4.1.1); an absent or null
    field is an empty tuple — a valid value, never an error.
    """
    if value is None:
        return ()
    if isinstance(value, str):
        raise ValueError(f"expected a list of item names, got the string {value!r}")
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value)
    raise ValueError(
        f"expected a list of item names, got {type(value).__name__}"
    )


def _as_utc_datetime(value: Any) -> datetime:
    """A generated-date as a UTC datetime (naive means UTC, §8)."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    raise ValueError(f"generated-date is not a date: {value!r}")


def _as_state(snapshot_id: str, item: Any) -> ExcuseState:
    """One YAML source entry as a compact :class:`ExcuseState`."""
    if not isinstance(item, Mapping):
        raise ValueError(
            f"snapshot {snapshot_id} has a non-mapping source entry: "
            f"{type(item).__name__}"
        )
    name = item.get("item-name")
    if not name or not isinstance(name, str):
        raise ValueError(
            f"snapshot {snapshot_id} has a source entry without an item-name"
        )
    dependencies = item.get("dependencies") or {}
    if not isinstance(dependencies, Mapping):
        raise ValueError(
            f"snapshot {snapshot_id} item {name!r} has non-mapping dependencies"
        )
    return ExcuseState(
        item_name=name,
        source=item.get("source"),
        old_version=item.get("old-version"),
        new_version=item.get("new-version"),
        verdict=item.get("migration-policy-verdict"),
        is_candidate=bool(item.get("is-candidate", False)),
        blocked_by=_as_name_list(dependencies.get("blocked-by")),
        migrate_after=_as_name_list(dependencies.get("migrate-after")),
        reason=_as_reason_list(item.get("reason")),
    )


def parse_snapshot(
    snapshot_id: str,
    gz_bytes: bytes,
    *,
    url: str | None = None,
) -> ParsedSnapshot:
    """Parse one raw ``.yaml.gz`` snapshot into metadata plus states.

    Safe loaders only (§9.2, §17 — never ``FullLoader`` or ``Loader``).
    ``item_name`` is unique within the snapshot: a duplicate raises
    :class:`DuplicateItemName` with its cause (§5.2) — an invalid
    record, never a silent overwrite. A document whose top-level keys
    miss :data:`REQUIRED_TOP_LEVEL_KEYS` raises
    :class:`TopLevelKeyDrift` listing the observed keys. Returns the
    snapshot's :class:`SnapshotMetadata` (SHA-256 of the raw bytes,
    generated-date, item count, observed top-level keys) plus one
    :class:`ExcuseState` per item.
    """
    try:
        text = gzip.decompress(gz_bytes).decode("utf-8", errors="replace")
    except (OSError, EOFError) as error:
        raise TruncatedGzip(
            f"snapshot {snapshot_id} does not decompress: {error}"
        ) from error
    document = yaml.load(text, Loader=_SafeLoader)
    if not isinstance(document, Mapping):
        raise ValueError(
            f"snapshot {snapshot_id} is not a YAML mapping: "
            f"{type(document).__name__}"
        )
    observed_keys = sorted(str(key) for key in document.keys())
    missing = [key for key in REQUIRED_TOP_LEVEL_KEYS if key not in document]
    if missing:
        raise TopLevelKeyDrift(
            f"snapshot {snapshot_id} is missing the top-level key(s) "
            f"{', '.join(missing)}; observed keys: {', '.join(observed_keys)}"
        )
    items = document["sources"]
    if not isinstance(items, list):
        raise ValueError(
            f"snapshot {snapshot_id} has a non-list sources: "
            f"{type(items).__name__}"
        )
    states: dict[str, ExcuseState] = {}
    for item in items:
        state = _as_state(snapshot_id, item)
        if state.item_name in states:
            raise DuplicateItemName(
                f"duplicate item-name in snapshot {snapshot_id}: "
                f"{state.item_name}"
            )
        states[state.item_name] = state
    metadata = SnapshotMetadata(
        snapshot_id=snapshot_id,
        generated_date=_as_utc_datetime(document["generated-date"]),
        url=url,
        sha256=sha256_bytes(gz_bytes),
        item_count=len(items),
        top_level_keys=tuple(observed_keys),
    )
    return ParsedSnapshot(metadata=metadata, states=states)