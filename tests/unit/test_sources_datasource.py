"""Tests for SourcesDebianOrgDataSource."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

from debian_observer.ingestion.acquisition import SourcesDebianOrgDataSource


def _mock_urlopen_response(
    mock_urlopen: MagicMock,
    data: dict[str, Any],
) -> None:
    """Configure a ``urlopen`` mock to return a context-managed JSON response."""
    fake_response = MagicMock()
    fake_response.read.return_value = json.dumps(data).encode()
    fake_response.__enter__ = MagicMock(return_value=fake_response)
    fake_response.__exit__ = MagicMock(return_value=False)
    mock_urlopen.return_value = fake_response


class TestSourcesDebianOrgDataSource:
    """Unit tests for the real Debian HTTP data source."""

    @patch("urllib.request.urlopen")
    def test_retrieve_parses_versions(self, mock_urlopen: MagicMock) -> None:
        _mock_urlopen_response(
            mock_urlopen,
            {
                "package": "hello",
                "versions": [
                    {"version": "2.0-1", "suites": ["sid"], "area": "main"},
                    {"version": "1.0-1", "suites": ["bookworm"], "area": "main"},
                ],
            },
        )

        source = SourcesDebianOrgDataSource(package_name="hello")
        records = source.retrieve()

        assert len(records) == 2
        assert records[0]["package"] == "hello"
        assert records[0]["version"] == "2.0-1"
        assert records[0]["distribution"] == "debian"
        assert records[0]["release"] == "sid"
        assert records[0]["suite"] is None
        assert records[1]["release"] == "bookworm"

    @patch("urllib.request.urlopen")
    def test_multi_suite_expands_records(self, mock_urlopen: MagicMock) -> None:
        _mock_urlopen_response(
            mock_urlopen,
            {
                "package": "testpkg",
                "versions": [
                    {"version": "1.0", "suites": ["sid", "trixie"], "area": "main"},
                ],
            },
        )

        source = SourcesDebianOrgDataSource(package_name="testpkg")
        records = source.retrieve()

        assert len(records) == 2
        assert all(r["distribution"] == "debian" for r in records)
        assert records[0]["release"] == "sid"
        assert records[1]["release"] == "trixie"

    @patch("urllib.request.urlopen")
    def test_compute_checksum_deterministic(self, mock_urlopen: MagicMock) -> None:
        _mock_urlopen_response(
            mock_urlopen,
            {
                "package": "hello",
                "versions": [{"version": "1.0", "suites": ["sid"], "area": "main"}],
            },
        )

        source = SourcesDebianOrgDataSource(package_name="hello")
        checksum1 = source.compute_checksum()
        checksum2 = source.compute_checksum()
        assert checksum1 == checksum2
        assert len(checksum1) == 64  # SHA-256 hex length


class TestSourcesDistributionSuiteSemantics:
    """sources.debian.org rows follow the archive-family field semantics."""

    @patch("urllib.request.urlopen")
    def test_suite_role_lands_in_suite_field(self, mock_urlopen: MagicMock) -> None:
        _mock_urlopen_response(
            mock_urlopen,
            {
                "package": "hello",
                "versions": [
                    {
                        "version": "2.0-1",
                        "suites": ["stable", "bookworm"],
                        "area": "main",
                    },
                ],
            },
        )

        source = SourcesDebianOrgDataSource(package_name="hello")
        records = source.retrieve()

        assert len(records) == 2
        stable_rec = next(r for r in records if r["suite"] == "stable")
        assert stable_rec["release"] is None
        assert stable_rec["distribution"] == "debian"
        bookworm_rec = next(r for r in records if r["release"] == "bookworm")
        assert bookworm_rec["suite"] is None


class TestSourcesComponentKey:
    """The archive component arrives under the component key."""

    @patch("urllib.request.urlopen")
    def test_area_lands_in_component_field(self, mock_urlopen: MagicMock) -> None:
        _mock_urlopen_response(
            mock_urlopen,
            {
                "package": "hello",
                "versions": [
                    {"version": "1.0", "suites": ["sid"], "area": "main"},
                ],
            },
        )

        source = SourcesDebianOrgDataSource(package_name="hello")
        records = source.retrieve()

        assert records[0]["component"] == "main"
        assert "area" not in records[0]
