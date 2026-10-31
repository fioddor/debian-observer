"""API: Load events from Parquet into the in-memory store.

Provides ``EventLoader`` to populate :class:`InMemoryStore` from
Parquet files written by ingestion or the consumer pipeline.

Usage::

    from debian_observer.api.app import create_app
    from debian_observer.api.loader import EventLoader
    from debian_observer.api.store import InMemoryStore

    store = InMemoryStore()
    loader = EventLoader(store)
    loader.load_from_parquet("data/parquet")

    app = create_app(store=store)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from debian_observer.api.store import InMemoryStore
from debian_observer.domain.models import Package, PackageVersion
from debian_observer.events import EventEnvelope
from debian_observer.storage.analytical import AnalyticalStorage


class EventLoader:
    """Loads Parquet events into an in-memory API store."""

    def __init__(self, store: InMemoryStore) -> None:
        self.store = store

    def load_from_parquet(self, parquet_dir: str | Path) -> int:
        """Load all events from Parquet files into the store.

        Args:
            parquet_dir: Directory containing Parquet files.

        Returns:
            Number of events loaded.
        """
        storage = AnalyticalStorage(parquet_dir)
        # Bind by NAME, never by position: the events view is read_parquet(...,
        # union_by_name=true) — column ORDER (and presence, for legacy 13/16-col
        # files) is an implementation detail of each file, not a contract.
        # SELECT * plus the cursor description gives the REAL names of what was
        # read; the canonical dict lookup below fills None for legacy misses.
        try:
            cursor = storage.query("SELECT * FROM events")
            rows = cursor.fetchall()
            columns = [d[0] for d in cursor.description]
        except RuntimeError:
            return 0

        count = 0
        for row in rows:
            record = dict(zip(columns, row))
            event = self._row_to_event(record)
            if event:
                self._ingest_event(event)
                count += 1
        return count

    def _row_to_event(self, record: dict[str, Any]) -> EventEnvelope | None:
        """Convert a row (bound by column name) back to an EventEnvelope.

        The row arrives as a dict keyed by the canonical column names
        (see load_from_parquet); legacy files miss columns (13/16-col
        layouts) and get None via the same SELECT.
        """
        if not record.get("event_id"):
            return None
        g = record.get
        return EventEnvelope(
            event_id=g("event_id"),
            event_type=g("event_type"),
            source=g("source"),
            entity_id=g("entity_id"),
            observed_at=g("observed_at"),
            effective_at=g("effective_at"),
            ingestion_run_id=g("ingestion_run_id"),
            payload={
                "package_id": g("package_id") or "",
                "source_package": g("source_package"),
                "version": g("version") or "",
                "distribution": g("distribution") or "",
                "release": g("release"),
                "suite": g("suite"),
                "architecture": g("architecture") or "",
                "component": g("component"),
                "section": g("section"),
                "priority": g("priority"),
                "observation": {
                    "source": g("source") or "",
                    "observed_at": g("observed_at") or "",
                    "effective_at": g("effective_at") or "",
                    "schema_version": 1,
                    "ingestion_run_id": g("ingestion_run_id") or "",
                },
            },
        )

    def _ingest_event(self, event: EventEnvelope) -> None:
        """Add an event to the store, deriving domain models when possible."""
        self.store.add_event(event)

        if event.event_type == "PackageVersionObserved":
            payload = event.payload
            pkg_id = payload.get("package_id", "")
            parts = pkg_id.rsplit(":", 1)
            pkg_name = parts[0] if parts else pkg_id
            arch = parts[1] if len(parts) > 1 else "all"

            # Ensure package exists
            if self.store.get_package_by_id(pkg_id) is None:
                self.store.add_package(
                    Package(
                        package_id=pkg_id,
                        name=pkg_name,
                        source_package=pkg_name,
                        section=payload.get("section") or "unknown",
                        architecture=arch,
                    )
                )

            # Add version
            self.store.add_package_version(
                PackageVersion(
                    package_id=pkg_id,
                    source_package=payload.get("source_package"),
                    version=payload.get("version", ""),
                    distribution=payload.get("distribution"),
                    release=payload.get("release"),
                    suite=payload.get("suite"),
                    architecture=payload.get("architecture"),
                    component=payload.get("component"),
                    section=payload.get("section"),
                    priority=payload.get("priority"),
                    source=event.source,
                )
            )
