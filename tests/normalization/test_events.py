"""Tests for normalization/events module."""

from __future__ import annotations

from datetime import datetime, timezone

from debian_observer.domain.models import PackageVersion
from debian_observer.events import EventEnvelope
from debian_observer.normalization.events import EventFactory


class TestEventFactory:
    def test_from_package_version(self):
        factory = EventFactory(source_name="udd", ingestion_run_id="run-001")
        version = PackageVersion(
            package_id="vim:amd64",
            version="2:9.0-1",
            distribution="sid",
            architecture="amd64",
            component="main",
            priority="optional",
            source="udd",
            observed_at=datetime.now(timezone.utc),
        )
        event = factory.from_package_version(version)
        assert isinstance(event, EventEnvelope)
        assert event.event_type == "PackageVersionObserved"
        assert event.entity_id == "vim:amd64"
        assert event.payload["package_id"] == "vim:amd64"
        assert event.payload["version"] == "2:9.0-1"
        assert event.ingestion_run_id == "run-001"

    def test_from_package_versions(self):
        factory = EventFactory(source_name="udd", ingestion_run_id="run-002")
        versions = [
            PackageVersion(
                package_id="a:all",
                version="1.0",
                source="udd",
                observed_at=datetime.now(timezone.utc),
            ),
            PackageVersion(
                package_id="b:all",
                version="2.0",
                source="udd",
                observed_at=datetime.now(timezone.utc),
            ),
        ]
        events = factory.from_package_versions(versions)
        assert len(events) == 2
        assert events[0].entity_id == "a:all"
        assert events[1].entity_id == "b:all"
