"""Messaging: Stream configuration.

Defines the NATS JetStream stream topology for Debian Observatory events.

The configuration is deliberately simple: one stream carries
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
    "debian.dependency.observed",
    "debian.popcon.observed",
]

# Default stream limits. Adjusted for small-scale deployment.
DEFAULT_MAX_MESSAGES = 100_000
DEFAULT_MAX_AGE_DAYS = 30
DEFAULT_MAX_BYTES = 1 << 30  # 1 GB

# ---------------------------------------------------------------------------
# The migration stream (ADR-026, docs/release3.project.v0.1.md §11.1)
# ---------------------------------------------------------------------------

#: The dedicated migration stream: separate from `debian-events`, so
#: neither stream's retention evicts the other's.
MIGRATIONS_STREAM_NAME = "debian-migrations"

#: The stream captures both migration subject families (§6.1):
#: `debian.migration.excuse.>` and `debian.migration.snapshot.>`.
#: Wildcards — not the literal per-type subjects — so a new event kind
#: is captured without touching the stream configuration. The
#: per-event-type subject map stays single-sourced in the pair
#: eventizer's ``SUBJECTS``
#: (``normalization.eventizers.excuses``).
MIGRATIONS_SUBJECTS = [
    "debian.migration.excuse.>",
    "debian.migration.snapshot.>",
]

#: Spec §11.1 (D3): consumers replay from the start, so retention is
#: limits with 400 days of age — revisited once the measured volume
#: lands (the probe saw ~2.5 MB a day, so 400 days fit the 2 GB budget).
MIGRATIONS_MAX_AGE_DAYS = 400

#: Spec §11.1 (D3): 2 GB — the disk budget that pairs with the age.
MIGRATIONS_MAX_BYTES = 2 * 1024**3

#: No message-count budget: age and bytes are the disk budget, and
#: the `debian-events` default (100,000) would evict in under a month
#: at the measured ~4,200 events a day (ADR-026 evidence).
MIGRATIONS_MAX_MESSAGES = -1

#: Spec §11.1: a duplicate window of "hours, not minutes" — it must
#: cover re-publishing a window (a re-run of `normalize britney
#: --publish`); the guarantee stays in the consumers, which
#: deduplicate by `event_id` (§6.2). 4 h covers a re-publish after a
#: same-day restart with margin; revisited with the measured volume.
MIGRATIONS_DUPLICATE_WINDOW_HOURS = 4


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
        replication_factor: Number of stream replicas.
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
            "DependencyObserved": "debian.dependency.observed",
            "PopconObserved": "debian.popcon.observed",
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


class MigrationStreamConfig(StreamConfig):
    """The declarative configuration of the ``debian-migrations`` stream.

    ADR-026: migration events keep their own retention so the
    continuous flow (~4,200 events a day) neither evicts nor is
    evicted by ``debian-events``. File storage and limits retention:
    consumers replay the stream from the start. The subject routing
    per event type is NOT restated here —
    :meth:`subject_for` reads the pair eventizer's ``SUBJECTS``
    (``normalization.eventizers.excuses``), the publication
    contract's single source.
    """

    def __init__(
        self,
        *,
        stream_name: str = MIGRATIONS_STREAM_NAME,
        subjects: list[str] | None = None,
        storage: str = "file",
        retention: str = "limits",
        max_age_days: int = MIGRATIONS_MAX_AGE_DAYS,
        max_bytes: int = MIGRATIONS_MAX_BYTES,
        replication_factor: int = 1,
    ) -> None:
        super().__init__(
            stream_name=stream_name,
            subjects=subjects
            if subjects is not None
            else list(MIGRATIONS_SUBJECTS),
            storage=storage,
            retention=retention,
            max_messages=MIGRATIONS_MAX_MESSAGES,
            max_age_days=max_age_days,
            max_bytes=max_bytes,
            replication_factor=replication_factor,
        )
        #: Spec §11.1: a duplicate window of hours, not minutes.
        self.duplicate_window_hours = MIGRATIONS_DUPLICATE_WINDOW_HOURS

    def subject_for(self, event_type: str) -> str:
        """Map an event type to its migration subject (§6.1).

        The mapping is the pair eventizer's ``SUBJECTS`` — read, never
        restated: a new event kind lands there once and every consumer
        of this configuration follows.

        Raises:
            KeyError: naming the unknown type and the known ones.
        """
        from debian_observer.normalization.eventizers.excuses import (
            subject_for,
        )

        return subject_for(event_type)

    def to_nats_config(self) -> dict[str, Any]:
        """Serialize to ``js.add_stream()`` keys, duplicate window included."""
        import datetime

        config = dict(super().to_nats_config())
        config["duplicate_window"] = int(
            datetime.timedelta(
                hours=self.duplicate_window_hours
            ).total_seconds()
        )
        return config
