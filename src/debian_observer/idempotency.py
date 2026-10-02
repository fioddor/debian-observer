"""Idempotency store for event processing.

Prevents duplicate event processing by tracking seen event IDs
with optional TTL-based expiration.
"""

from __future__ import annotations

from typing import Any


class IdempotencyStore:
    """In-memory idempotency tracker for R1.

    R1 uses a simple in-memory store. R2 will replace this with
    a persistent backend (DuckDB, Redis, or similar).

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
