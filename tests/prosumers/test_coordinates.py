"""Tests for the reusable coordinate filter (``prosumers/coordinates.py``).

The coordinate selectors are one piece, reused by every consumer of
the event log. The selectors are **optional**: a selector that is not
given does not filter — an absent ``release`` lets every release
through.
"""

from __future__ import annotations

import pytest

from debian_observer.prosumers.coordinates import CoordinateFilter


@pytest.fixture()
def row() -> dict:
    return {
        "distribution": "debian",
        "release": "trixie",
        "architecture": "amd64",
        "component": "main",
    }


class TestCoordinateFilter:
    def test_full_coordinate_selects(self, row: dict) -> None:
        f = CoordinateFilter(
            distribution="debian", release="trixie", architecture="amd64"
        )
        assert f.selects(row) is True

    def test_all_architecture_rides_along(self, row: dict) -> None:
        f = CoordinateFilter(
            distribution="debian", release="trixie", architecture="amd64"
        )
        row_all = {**row, "architecture": "all"}
        assert f.selects(row_all) is True

    def test_wrong_release_rejected(self, row: dict) -> None:
        f = CoordinateFilter(
            distribution="debian", release="trixie", architecture="amd64"
        )
        assert f.selects({**row, "release": "bookworm"}) is False

    def test_absent_selector_does_not_filter(self, row: dict) -> None:
        # no release given: every release passes
        f = CoordinateFilter(distribution="debian", architecture="amd64")
        assert f.selects({**row, "release": "bookworm"}) is True
        assert f.selects({**row, "release": "trixie"}) is True

    def test_no_selectors_at_all_selects_everything(self, row: dict) -> None:
        f = CoordinateFilter()
        assert f.selects({**row, "release": "sid"}) is True
        assert f.selects({**row, "distribution": "ubuntu"}) is True

    def test_architecture_none_means_any(self, row: dict) -> None:
        f = CoordinateFilter(distribution="debian", release="trixie")
        assert f.selects({**row, "architecture": "arm64"}) is True

    def test_missing_column_selects_when_not_filtered(
        self, row: dict
    ) -> None:
        # a row without the column: the absent selector passes it
        f = CoordinateFilter(distribution="debian")
        assert f.selects({"distribution": "debian"}) is True
        # but a given selector rejects the missing column
        g = CoordinateFilter(distribution="debian", release="trixie")
        assert g.selects({"distribution": "debian"}) is False

    def test_from_cli_args(self) -> None:
        f = CoordinateFilter.from_args(
            distribution="debian", release=None, architecture="amd64"
        )
        assert f.distribution == "debian"
        assert f.release is None
        assert f.architecture == "amd64"

    def test_component_list_filter(self, row: dict) -> None:
        f = CoordinateFilter(
            distribution="debian",
            release="trixie",
            architecture="amd64",
            components="main+non-free-firmware",
        )
        assert f.selects(row) is True
        assert f.selects({**row, "component": "contrib"}) is False
        # no components selector: every component passes
        g = CoordinateFilter(distribution="debian")
        assert g.selects({**row, "component": "contrib"}) is True
        # a row without component passes a components filter only if
        # the list is not given (else it cannot be inside)
        assert f.selects({"distribution": "debian"}) is False