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

    def test_package_versions_declares_the_packages_shape(self) -> None:
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


    def test_udddump_offers_dependencies_and_popcon(self) -> None:
        names = set(EVENTIZERS["udddump"])
        assert {"package-versions", "dependencies", "popcon"} <= names

    def test_get_eventizer_selects_them(self) -> None:
        assert get_eventizer("udddump", "dependencies").name == (
            "dependencies"
        )
        assert get_eventizer("udddump", "popcon").name == "popcon"

    def test_shapes_declared(self, dependencies_eventizer, popcon_eventizer) -> None:
        required = dependencies_eventizer.shape.required_columns
        assert {
            "package",
            "version",
            "architecture",
            "distribution",
            "release",
            "depends",
            "pre_depends",
        } <= set(required)
        assert popcon_eventizer.shape.required_columns >= {
            "package",
            "insts",
        }

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


"""Tests for the ``dependencies`` and ``popcon`` eventizers.

The eventizer **observes, nothing else**: one event per clause with
the condition verbatim; no grammar parsing inside a condition, no
alternative picking, no target resolution. Minimum validation only:
a malformed or deprecated clause becomes an invalid record with the
cause, never a silent drop.
"""


from datetime import datetime, timezone

import pytest

from debian_observer.normalization.eventizers import (
    EVENTIZERS,
    get_eventizer,
)
from debian_observer.normalization.eventizers.dependencies import (
    DependenciesEventizer,
)
from debian_observer.normalization.eventizers.popcon import (
    PopconEventizer,
)


@pytest.fixture()
def dependencies_eventizer() -> DependenciesEventizer:
    ev = EVENTIZERS["udddump"]["dependencies"]
    return ev.with_provenance(
        source_name="udddump", ingestion_run_id="run-001"
    )


@pytest.fixture()
def popcon_eventizer() -> PopconEventizer:
    ev = EVENTIZERS["udddump"]["popcon"]
    return ev.with_provenance(
        source_name="udddump", ingestion_run_id="run-001"
    )


def _packages_row(depends: str | None, pre_depends: str | None = None):
    return {
        "id": "1",
        "package": "libhello",
        "source": "hello",
        "version": "1.0-1",
        "distribution": "debian",
        "release": "trixie",
        "architecture": "amd64",
        "component": "main",
        "depends": depends,
        "pre_depends": pre_depends,
    }


class TestDependenciesEventizer:
    def test_declares_name_and_type(self) -> None:
        ev = DependenciesEventizer()
        assert ev.name == "dependencies"
        assert ev.event_type == "DependencyObserved"

    def test_simple_clause_is_one_event(self, dependencies_eventizer) -> None:
        events = [
            out
            for out in dependencies_eventizer.eventize(_packages_row("libc6 (>= 2.36)"))
            if not isinstance(out, dict)
        ]
        assert len(events) == 1
        payload = events[0].payload
        assert payload["kind"] == "depends"
        assert payload["clause_index"] == 0
        assert payload["condition"] == "libc6 (>= 2.36)"
        assert payload["is_complex"] is False
        assert payload["alternatives"] == 1
        assert payload["package"] == "libhello"
        assert payload["version"] == "1.0-1"

    def test_two_clauses_carry_their_index(self, dependencies_eventizer) -> None:
        row = _packages_row("libc6 (>= 2.36), libc-bin")
        events = [
            out
            for out in dependencies_eventizer.eventize(row)
            if not isinstance(out, dict)
        ]
        assert len(events) == 2
        assert [e.payload["clause_index"] for e in events] == [0, 1]
        assert events[1].payload["condition"] == "libc-bin"

    def test_complex_clause_counts_alternatives(self, dependencies_eventizer) -> None:
        events = [
            out
            for out in dependencies_eventizer.eventize(_packages_row("a | b | c"))
            if not isinstance(out, dict)
        ]
        assert len(events) == 1
        payload = events[0].payload
        assert payload["is_complex"] is True
        assert payload["alternatives"] == 3
        # verbatim: the OR structure travels with the condition
        assert payload["condition"] == "a | b | c"

    def test_pre_depends_is_a_kind(self, dependencies_eventizer) -> None:
        events = [
            out
            for out in dependencies_eventizer.eventize(
                _packages_row(None, pre_depends="libc6")
            )
            if not isinstance(out, dict)
        ]
        assert len(events) == 1
        assert events[0].payload["kind"] == "pre-depends"

    def test_null_and_empty_fields_emit_nothing(self, dependencies_eventizer) -> None:
        for depends in (None, ""):
            row = _packages_row(depends)
            row["pre_depends"] = None
            assert list(dependencies_eventizer.eventize(row)) == []

    def test_deprecated_operators_are_invalid_not_events(
        self, dependencies_eventizer
    ) -> None:
        row = _packages_row("libc6 < 2.36")
        out = list(dependencies_eventizer.eventize(row))
        assert len(out) == 1
        assert isinstance(out[0], dict)
        assert out[0]["reason"] == "invalid"
        assert "deprecated" in out[0]["cause"]

    def test_whitespace_variants(self, dependencies_eventizer) -> None:
        events = [
            out
            for out in dependencies_eventizer.eventize(
                _packages_row("  libc6 (>= 2.36) ,  libc-bin ")
            )
            if not isinstance(out, dict)
        ]
        assert len(events) == 2
        assert events[0].payload["condition"] == "libc6 (>= 2.36)"
        assert events[1].payload["condition"] == "libc-bin"

    def test_empty_clause_parts_are_skipped(self, dependencies_eventizer) -> None:
        # "a, , b": empty parts between separators do not become
        # events nor invalid records.
        events = [
            out
            for out in dependencies_eventizer.eventize(_packages_row("a,,b"))
            if not isinstance(out, dict)
        ]
        assert len(events) == 2


class TestPopconEventizer:
    def test_declares_name_and_type(self) -> None:
        ev = PopconEventizer()
        assert ev.name == "popcon"
        assert ev.event_type == "PopconObserved"

    def test_one_event_per_row(self, popcon_eventizer) -> None:
        row = {
            "package": "libhello",
            "insts": "100",
            "vote": "50",
            "olde": "10",
            "recent": "5",
            "nofiles": "1",
        }
        events = [
            out
            for out in popcon_eventizer.eventize(row)
            if not isinstance(out, dict)
        ]
        assert len(events) == 1
        payload = events[0].payload
        assert payload["package"] == "libhello"
        assert payload["insts"] == 100
        assert payload["vote"] == 50
        assert payload["recent"] == 5

    def test_entity_id_is_the_package(self, popcon_eventizer) -> None:
        row = {
            "package": "libhello",
            "insts": "1",
        }
        events = [
            out
            for out in popcon_eventizer.eventize(row)
            if not isinstance(out, dict)
        ]
        assert events[0].entity_id == "libhello"

    def test_missing_counters_default_to_zero(self, popcon_eventizer) -> None:
        # The shape requires only package+insts; the other
        # counters read as 0 when the row does not carry them.
        row = {"package": "libhello", "insts": "7"}
        events = [
            out
            for out in popcon_eventizer.eventize(row)
            if not isinstance(out, dict)
        ]
        assert events[0].payload["vote"] == 0
