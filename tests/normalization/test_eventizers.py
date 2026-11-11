"""Tests for the eventizer registry and the ``package-versions``
eventizer (ADR-014).

A source owns a registry of eventizers; ``normalize <source>
--eventizer NAME`` selects one (default ``package-versions``,
retro-compatible behaviour). Each eventizer declares its ``name``,
``event_type``, ``shape`` (a ``TableShape``) and ``eventize(row)`` —
the **full row** (all COPY columns), not a pre-mapped record.
"""

from __future__ import annotations

import pytest

from debian_observer.normalization.eventizers import (
    EVENTIZERS,
    get_eventizer,
    list_eventizers,
)
from debian_observer.normalization.eventizers.package_versions import (
    PackageVersionsEventizer,
)
from debian_observer.normalization.udddump import TableShape


@pytest.fixture()
def eventizer() -> PackageVersionsEventizer:
    return PackageVersionsEventizer()


class TestRegistry:
    """The registry maps each source name to its eventizers."""

    def test_registry_has_udddump(self) -> None:
        assert "udddump" in EVENTIZERS

    def test_udddump_has_package_versions(self) -> None:
        assert "package-versions" in EVENTIZERS["udddump"]

    def test_package_versions_is_the_r1_packages_shape(self) -> None:
        eventizer = EVENTIZERS["udddump"]["package-versions"]
        assert eventizer.shape.name == "packages"

    def test_get_eventizer_returns_the_named_one(self) -> None:
        eventizer = get_eventizer("udddump", "package-versions")
        assert eventizer.name == "package-versions"

    def test_get_eventizer_default_is_package_versions(self) -> None:
        eventizer = get_eventizer("udddump", None)
        assert eventizer.name == "package-versions"

    def test_get_eventizer_unknown_source_is_an_error(self) -> None:
        with pytest.raises(KeyError, match="no-such-source"):
            get_eventizer("no-such-source", "package-versions")

    def test_get_eventizer_unknown_name_lists_the_available(self) -> None:
        with pytest.raises(ValueError, match="package-versions"):
            get_eventizer("udddump", "no-such-eventizer")

    def test_list_eventizers_names_the_perimeter(self) -> None:
        assert "package-versions" in list_eventizers("udddump")


class TestPackageVersionsEventizer:
    """The fixed field mapping, moved into an eventizer."""

    FULL_ROW = {
        "id": "1",
        "package": "libhello",
        "source": "hello",
        "version": "1.0",
        "distribution": "debian",
        "release": "trixie",
        "architecture": "amd64",
        "section": "libs",
        "priority": None,
        "component": "main",
    }

    def test_declares_name_event_type_and_shape(self, eventizer) -> None:
        assert eventizer.name == "package-versions"
        assert eventizer.event_type == "PackageVersionObserved"
        assert isinstance(eventizer.shape, TableShape)
        assert eventizer.shape.name == "packages"

    def test_eventize_takes_the_full_row(self, eventizer) -> None:
        events = list(eventizer.eventize(self.FULL_ROW))
        assert len(events) == 1
        payload = events[0].payload
        assert payload["package_id"] == "libhello:amd64"
        assert payload["version"] == "1.0"
        assert payload["distribution"] == "debian"
        assert payload["release"] == "trixie"
        assert payload.get("suite") is None

    def test_eventize_yields_events_not_records(self, eventizer) -> None:
        for event in eventizer.eventize(self.FULL_ROW):
            assert event.event_type == "PackageVersionObserved"

    def test_eventize_invalid_row_yields_invalid_record(self, eventizer) -> None:
        # version missing and package unknown: fails the
        # validator (version + any of package/source required).
        row = dict(self.FULL_ROW)
        row["package"] = None
        row["source"] = None
        row["version"] = None
        out = list(eventizer.eventize(row))
        assert len(out) == 1
        assert isinstance(out[0], dict)
        assert out[0]["reason"] == "invalid"
        assert "cause" in out[0]