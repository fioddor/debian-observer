"""Event models for the Debian Observatory.

This package defines:

- ``EventEnvelope``      — the canonical event wrapper used for all messages
- Event payload models   — strongly-typed payloads for each event type
- ``DatasetManifest``    — ingestion-run metadata and provenance

All events are immutable and serializable to both JSON (for messaging)
and Parquet (for analytical storage).
"""

from debian_observer.events.envelope import EventEnvelope
from debian_observer.events.manifest import DatasetManifest
from debian_observer.events.types import (
    MigrationObservedPayload,
    PackageObservedPayload,
    PackageVersionObservedPayload,
    ReleaseObservedPayload,
)

__all__ = [
    "EventEnvelope",
    "DatasetManifest",
    "PackageObservedPayload",
    "PackageVersionObservedPayload",
    "ReleaseObservedPayload",
    "MigrationObservedPayload",
]
