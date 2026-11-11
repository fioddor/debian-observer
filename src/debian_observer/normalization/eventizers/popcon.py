"""Normalization: the ``popcon`` eventizer.

One ``PopconObserved`` per ``popcon`` row: the popularity counters
of a binary package name (no version, no coordinate). Used
downstream as an impact weight.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterator

from debian_observer.events.envelope import EventEnvelope
from debian_observer.normalization.events import EventFactory
from debian_observer.normalization.udddump import TableShape


class PopconEventizer:
    """``popcon`` rows → ``PopconObserved`` events."""

    name = "popcon"
    event_type = "PopconObserved"
    shape = TableShape(
        name="popcon",
        required_columns=frozenset({"package", "insts"}),
    )

    #: Counter columns copied from the row (missing ones read as 0).
    COUNTERS = ("insts", "vote", "olde", "recent", "nofiles")

    def __init__(self) -> None:
        self._factory: EventFactory | None = None

    def with_provenance(
        self, *, source_name: str, ingestion_run_id: str
    ) -> "PopconEventizer":
        """Bind the manifest provenance (source, run id) of this run."""
        self._factory = EventFactory(source_name, ingestion_run_id)
        return self

    def eventize(
        self, row: dict[str, Any]
    ) -> Iterator[Any]:
        """Full row → one ``PopconObserved``."""
        if self._factory is None:
            self._factory = EventFactory("udddump", "")
        factory = self._factory
        from debian_observer.events.types import (
            ObservationMeta,
            PopconObservedPayload,
        )

        now = datetime.now(timezone.utc)
        package = row.get("package") or "unknown"
        observation = ObservationMeta(
            source=factory.source_name,
            observed_at=now,
            effective_at=now,
            ingestion_run_id=factory.ingestion_run_id,
        )
        counters = {}
        for name in self.COUNTERS:
            value = row.get(name)
            if value is None or value == "":
                counters[name] = 0
            else:
                try:
                    counters[name] = int(value)
                except (TypeError, ValueError):
                    counters[name] = 0
        payload = PopconObservedPayload(
            observation=observation,
            package=package,
            **counters,
        )
        yield EventEnvelope.from_payload(
            event_type="PopconObserved",
            source=factory.source_name,
            entity_id=package,
            payload_model=payload,
            ingestion_run_id=factory.ingestion_run_id,
            observed_at=now,
            effective_at=now,
        )