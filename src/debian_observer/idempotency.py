"""Idempotency store for event processing.

Prevents duplicate event processing by tracking seen event IDs,
in memory and per process.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


class IdempotencyStore:
    """In-memory idempotency tracker.

    A per-process set of seen event IDs; there is no persistent
    backend. Durable effects stay idempotent by construction
    (deterministic output keys, idempotent folds), not by this store.

    Attributes:
        _seen: Set of processed event IDs.
    """

    def __init__(self) -> None:
        self._seen: set[str] = set()

    def is_processed(self, event_id: str) -> bool:
        """Check whether an event has already been processed.

        Args:
            event_id: Unique event identifier.

        Returns:
            ``True`` if the event was previously seen.
        """
        return event_id in self._seen

    def mark_processed(self, event_id: str, **kwargs: Any) -> None:
        """Mark an event as processed.

        Args:
            event_id: Unique event identifier.
            **kwargs: Reserved for future metadata (timestamp, TTL, etc.).
        """
        self._seen.add(event_id)

    def reset(self) -> None:
        """Clear all tracked event IDs."""
        self._seen.clear()

    def __len__(self) -> int:
        return len(self._seen)


class FileIdempotencyStore(IdempotencyStore):
    """Idempotency tracker whose seen IDs survive the process.

    The dedup that a durable consumer needs across a crash: the
    seen `event_id`s persist as plain JSON beside the consumer
    (a "seen" list), loaded at start, written atomically on every
    mark (a temp file, then rename — a crash mid-``mark_processed``
    never leaves a half-written file that would drop or corrupt
    the frontier). Keys are the id's canonical string: an
    ``EventEnvelope`` carries a UUID ``event_id``, and the UUID and
    its ``str()`` are one event.

    The in-memory parent's behaviour is unchanged: this subclass
    only redefines what "seen" means (a set that is loaded and
    persisted), not the contract.
    """

    _KIND = "idempotency-seen-v1"

    def __init__(self, path: Path) -> None:
        # Not `super().__init__()`: the set is loaded, not empty.
        self._path = Path(path)
        self._seen: set[str] = set()
        if self._path.exists():
            record = json.loads(self._path.read_text(encoding="utf-8"))
            if record.get("kind") != self._KIND:
                raise ValueError(
                    f"idempotency store {self._path} is of kind "
                    f"{record.get('kind')!r}, not {self._KIND!r}"
                )
            self._seen = {str(event_id) for event_id in record.get("seen", [])}

    def is_processed(self, event_id: Any) -> bool:
        """Check whether an event has already been processed.

        Args:
            event_id: Unique event identifier (UUID or its string).

        Returns:
            ``True`` if the event was previously seen.
        """
        return str(event_id) in self._seen

    def mark_processed(self, event_id: Any, **kwargs: Any) -> None:
        """Mark an event as processed and persist the seen set.

        Args:
            event_id: Unique event identifier (UUID or its string).
            **kwargs: Reserved for future metadata (timestamp, TTL, etc.).
        """
        self._seen.add(str(event_id))
        record = {"kind": self._KIND, "seen": sorted(self._seen)}
        tmp = self._path.with_suffix(".json.tmp")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(
            json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        os.replace(tmp, self._path)
