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
        assert records[0]["distribution"] == "stable"

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
