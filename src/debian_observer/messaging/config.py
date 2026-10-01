"""Messaging: Stream configuration.

Defines the NATS JetStream stream topology for Debian Observatory events.

The configuration is deliberately simple in R1. One stream carries
all domain events, with subjects organized by entity type. Future
releases may split into multiple streams if volume or retention
requirements differ by event type.
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Stream configuration
# ---------------------------------------------------------------------------

DEFAULT_STREAM_NAME = "debian-events"
DEFAULT_SUBJECTS = [
    "debian.package.observed",
    "debian.package_version.observed",
    "debian.release.observed",
    "debian.migration.observed",
]

# Default stream limits. Adjusted for small-scale R1 deployment.
DEFAULT_MAX_MESSAGES = 100_000
DEFAULT_MAX_AGE_DAYS = 30
DEFAULT_MAX_BYTES = 1 << 30  # 1 GB


class StreamConfig:
    """Declarative configuration for a NATS JetStream stream.

    Attributes:
        stream_name: Name of the JetStream stream.
        subjects: List of subject filters this stream captures.
        storage: Storage backend ("file" or "memory").
        retention: Retention policy ("limits", "interest", "work queue").
        max_messages: Maximum number of messages to retain.
        max_age_days: Maximum age of messages in days.
        max_bytes: Maximum total bytes to retain.
        replication_factor: Number of stream replicas (R1 uses 1).
    """

    def __init__(
        self,
        *,
        stream_name: str = DEFAULT_STREAM_NAME,
        subjects: list[str] | None = None,
        storage: str = "file",
        retention: str = "limits",
        max_messages: int = DEFAULT_MAX_MESSAGES,
        max_age_days: int = DEFAULT_MAX_AGE_DAYS,
        max_bytes: int = DEFAULT_MAX_BYTES,
        replication_factor: int = 1,
    ) -> None:
        self.stream_name = stream_name
        self.subjects = subjects if subjects is not None else list(DEFAULT_SUBJECTS)
        self.storage = storage
        self.retention = retention
        self.max_messages = max_messages
        self.max_age_days = max_age_days
        self.max_bytes = max_bytes
        self.replication_factor = replication_factor

    @property
    def subject_prefix(self) -> str:
        """Return the common subject prefix shared by all stream subjects."""
        return "debian."

    def subject_for(self, event_type: str) -> str | None:
        """Map an event type to its NATS subject.

        Args:
            event_type: Canonical event type name (e.g. "PackageObserved").

        Returns:
            The NATS subject string, or None if the event type is unknown.
        """
        mapping: dict[str, str] = {
            "PackageObserved": "debian.package.observed",
            "PackageVersionObserved": "debian.package_version.observed",
            "ReleaseObserved": "debian.release.observed",
            "MigrationObserved": "debian.migration.observed",
        }
        return mapping.get(event_type)

    def to_nats_config(self) -> dict[str, Any]:
        """Serialize to a dict suitable for ``js.add_stream()``.

        Returns:
            A dict with NATS JetStream configuration keys.
        """
        import datetime

        max_age = datetime.timedelta(days=self.max_age_days)

        return {
            "name": self.stream_name,
            "subjects": self.subjects,
            "storage": self.storage,
            "retention": self.retention,
            "max_msgs": self.max_messages,
            "max_age": int(max_age.total_seconds()),
            "max_bytes": self.max_bytes,
            "num_replicas": self.replication_factor,
        }
