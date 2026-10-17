"""Normalization: EventFactory.

Creates canonical EventEnvelope instances from domain objects.
"""

from __future__ import annotations

from datetime import datetime, timezone

from debian_observer.domain.models import PackageVersion
from debian_observer.events import EventEnvelope
from debian_observer.events.types import ObservationMeta, PackageVersionObservedPayload


class EventFactory:
    """Produces domain events from normalized models.

    Usage::

        factory = EventFactory(source_name="udd", ingestion_run_id="run-001")
        event = factory.from_package_version(version)
    """

    def __init__(self, source_name: str, ingestion_run_id: str) -> None:
        """Initialize the factory.

        Args:
            source_name: Identifier of the data source.
            ingestion_run_id: Run identifier for provenance.
        """
        self.source_name = source_name
        self.ingestion_run_id = ingestion_run_id

    def from_package_version(self, version: PackageVersion) -> EventEnvelope:
        """Create a PackageVersionObserved event from a domain model.

        Args:
            version: A canonical PackageVersion instance.

        Returns:
            An EventEnvelope wrapping the observation.
        """
        now = datetime.now(timezone.utc)
        observation = ObservationMeta(
            source=self.source_name,
            observed_at=now,
            effective_at=now,
            ingestion_run_id=self.ingestion_run_id,
        )
        payload = PackageVersionObservedPayload(
            package_id=version.package_id,
            source_package=version.source_package,
            version=version.version,
            distribution=version.distribution,
            release=version.release,
            suite=version.suite,
            architecture=version.architecture,
            component=version.component,
            section=version.section,
            priority=version.priority,
            observation=observation,
        )
        return EventEnvelope.from_payload(
            event_type="PackageVersionObserved",
            source=self.source_name,
            entity_id=version.package_id,
            payload_model=payload,
            ingestion_run_id=self.ingestion_run_id,
            observed_at=now,
            effective_at=now,
        )

    def from_package_versions(
        self,
        versions: list[PackageVersion],
    ) -> list[EventEnvelope]:
        """Create events from a batch of models.

        Returns:
            List of EventEnvelope instances.
        """
        return [self.from_package_version(v) for v in versions]
