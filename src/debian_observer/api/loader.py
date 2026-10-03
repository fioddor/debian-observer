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
        try:
            rows = storage.query("SELECT * FROM events").fetchall()
        except RuntimeError:
            return 0

        count = 0
        for row in rows:
            event = self._row_to_event(row)
            if event:
                self._ingest_event(event)
                count += 1
        return count

    def _row_to_event(self, row: tuple[Any, ...]) -> EventEnvelope | None:
        """Convert a DuckDB result row back to an EventEnvelope."""
        # DuckDB column order matches Parquet schema:
        # event_id, event_type, source, entity_id, observed_at, effective_at,
        # ingestion_run_id, package_id, version, distribution, architecture,
        # component, priority
        if len(row) < 13:
            return None
        return EventEnvelope(
            event_id=row[0],
            event_type=row[1],
            source=row[2],
            entity_id=row[3],
            observed_at=row[4],
            effective_at=row[5],
            ingestion_run_id=row[6],
            payload={
                "package_id": row[7] or "",
                "version": row[8] or "",
                "distribution": row[9] or "",
                "architecture": row[10] or "",
                "component": row[11],
                "priority": row[12],
                "observation": {
                    "source": row[2] or "",
                    "observed_at": row[4] or "",
                    "effective_at": row[5] or "",
                    "schema_version": 1,
                    "ingestion_run_id": row[6] or "",
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
                        section="unknown",
                        architecture=arch,
                    )
                )

            # Add version
            self.store.add_package_version(
                PackageVersion(
                    package_id=pkg_id,
                    version=payload.get("version", ""),
                    distribution=payload.get("distribution"),
                    architecture=payload.get("architecture"),
                    component=payload.get("component"),
                    priority=payload.get("priority"),
                    source=event.source,
                )
            )
