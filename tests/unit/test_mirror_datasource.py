"""Tests for DebianMirrorDataSource."""

from __future__ import annotations

import gzip
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from debian_observer.ingestion.acquisition import DebianMirrorDataSource


def _build_packages_gz(packages: list[dict[str, str]]) -> bytes:
    """Build a fake Packages.gz file."""
    lines = []
    for pkg in packages:
        for key, value in pkg.items():
            lines.append(f"{key}: {value}")
        lines.append("")
    lines.append("")  # trailing empty line to trigger last record flush
    content = "\n".join(lines).encode()
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        f.write(content)
    return buf.getvalue()


def _mock_urlopen_response(
    mock_urlopen: MagicMock, packages: list[dict[str, str]]
) -> None:
    """Configure mock_urlopen to return a fake Packages.gz response."""
    fake_data = _build_packages_gz(packages)
    mock_resp = MagicMock()
    mock_resp.read.return_value = fake_data
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)
    mock_urlopen.return_value = mock_resp


class TestDebianMirrorDataSource:
    """Unit tests for the Debian mirror Packages.gz data source."""

    @patch("urllib.request.urlopen")
    def test_retrieve_parses_packages(self, mock_urlopen: MagicMock) -> None:
        _mock_urlopen_response(
            mock_urlopen,
            [
                {"Package": "hello", "Version": "2.10-3", "Architecture": "amd64"},
                {"Package": "world", "Version": "1.0", "Architecture": "amd64"},
            ],
        )

        source = DebianMirrorDataSource(
            suite="stable",
            component="main",
            architecture="amd64",
            limit=10,
        )
        records = source.retrieve()

        assert len(records) == 2
        assert records[0]["package"] == "hello"
        assert records[0]["version"] == "2.10-3"
        assert records[0]["architecture"] == "amd64"
        assert records[0]["distribution"] == "debian"
        assert records[0]["suite"] == "stable"
        assert records[0]["release"] is None

    @patch("urllib.request.urlopen")
    def test_limit_truncates(self, mock_urlopen: MagicMock) -> None:
        _mock_urlopen_response(
            mock_urlopen,
            [
                {"Package": f"pkg{i}", "Version": f"1.{i}", "Architecture": "amd64"}
                for i in range(20)
            ],
        )

        source = DebianMirrorDataSource(limit=5)
        records = source.retrieve()

        assert len(records) == 5

    @patch("urllib.request.urlopen")
    def test_compute_checksum_deterministic(self, mock_urlopen: MagicMock) -> None:
        _mock_urlopen_response(
            mock_urlopen,
            [
                {"Package": "hello", "Version": "2.10-3", "Architecture": "amd64"},
            ],
        )

        source = DebianMirrorDataSource()
        checksum1 = source.compute_checksum()
        checksum2 = source.compute_checksum()
        assert checksum1 == checksum2
        assert len(checksum1) == 64


class TestDebianMirrorSectionPriority:
    """Mirror Packages.gz parser must keep Section and Priority."""

    @patch("urllib.request.urlopen")
    def test_retrieve_includes_section_priority(
        self, mock_urlopen: MagicMock
    ) -> None:
        _mock_urlopen_response(
            mock_urlopen,
            [
                {
                    "Package": "hello",
                    "Version": "2.10-3",
                    "Architecture": "amd64",
                    "Section": "devel",
                    "Priority": "optional",
                },
            ],
        )

        source = DebianMirrorDataSource(
            suite="stable", component="main", architecture="amd64", limit=5
        )
        records = source.retrieve()

        assert records[0]["section"] == "devel"
        assert records[0]["priority"] == "optional"


class TestMirrorDistributionSuiteSemantics:
    """distribution is the archive family; suite holds only suite roles."""

    @patch("urllib.request.urlopen")
    def test_distribution_is_archive_family(self, mock_urlopen: MagicMock) -> None:
        _mock_urlopen_response(
            mock_urlopen,
            [
                {"Package": "hello", "Version": "2.10-3", "Architecture": "amd64"},
            ],
        )

        source = DebianMirrorDataSource(suite="stable", limit=5)
        records = source.retrieve()

        assert records[0]["distribution"] == "debian"

    @patch("urllib.request.urlopen")
    def test_suite_role_lands_in_suite_field(self, mock_urlopen: MagicMock) -> None:
        _mock_urlopen_response(
            mock_urlopen,
            [
                {"Package": "hello", "Version": "2.10-3", "Architecture": "amd64"},
            ],
        )

        source = DebianMirrorDataSource(suite="testing", limit=5)
        records = source.retrieve()

        assert records[0]["suite"] == "testing"
        assert records[0]["release"] is None

    @patch("urllib.request.urlopen")
    def test_codename_lands_in_release_field(self, mock_urlopen: MagicMock) -> None:
        _mock_urlopen_response(
            mock_urlopen,
            [
                {"Package": "hello", "Version": "2.10-3", "Architecture": "amd64"},
            ],
        )

        source = DebianMirrorDataSource(suite="bookworm", limit=5)
        records = source.retrieve()

        assert records[0]["release"] == "bookworm"
        assert records[0]["suite"] is None

    @patch("urllib.request.urlopen")
    def test_ubuntu_mirror_keeps_archive_family(
        self, mock_urlopen: MagicMock
    ) -> None:
        _mock_urlopen_response(
            mock_urlopen,
            [
                {"Package": "hello", "Version": "2.10-3", "Architecture": "amd64"},
            ],
        )

        source = DebianMirrorDataSource(
            suite="noble",
            component="main",
            architecture="amd64",
            limit=5,
            mirror="http://archive.ubuntu.com/ubuntu",
            distribution="ubuntu",
        )
        records = source.retrieve()

        assert records[0]["distribution"] == "ubuntu"
        assert records[0]["release"] == "noble"

    def test_invalid_distribution_rejected(self) -> None:
        with pytest.raises(ValueError, match="distribution"):
            DebianMirrorDataSource(suite="stable", distribution="kali")
