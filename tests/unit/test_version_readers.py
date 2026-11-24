"""Tests for the readers of the package version.

``debian_observer.version.__version__`` is the single source of the
version (docs/design/module-contracts.md, ``version``): the wheel, the
sdist and the runtime must all see one value. Every place that shows
the version at run time is a reader of that source; a literal copy
drifts at the first bump and tells users and servers a version that
is not the one installed.
"""

from __future__ import annotations

import gzip
import io
from unittest.mock import patch

from click.testing import CliRunner

from debian_observer.version import __version__


def test_cli_version_reports_the_package_version():
    """``debian-observer --version`` prints the installed version."""
    from debian_observer.cli.main import cli

    result = CliRunner().invoke(cli, ["--version"])

    assert result.exit_code == 0
    assert __version__ in result.output


def test_api_reports_the_package_version():
    """The API (OpenAPI ``info.version``) reports the installed version."""
    from debian_observer.api.app import create_app

    assert create_app().version == __version__


def _captured_request(call, response: bytes):
    """Run *call* with ``urlopen`` faked; return the request it sent."""
    sent = []

    def fake_urlopen(request, timeout=None):
        sent.append(request)
        return io.BytesIO(response)

    with patch("urllib.request.urlopen", fake_urlopen):
        call()
    assert len(sent) == 1
    return sent[0]


def test_dump_download_identifies_the_package_version(tmp_path):
    """The UDD dump download names the installed version as User-Agent."""
    from debian_observer.ingestion.acquisition import download_dump

    request = _captured_request(
        lambda: download_dump("https://udd.example.org/udd.dump",
                              tmp_path / "udd.dump"),
        b"dump",
    )

    assert request.get_header("User-agent") == f"debian-observer/{__version__}"


def test_mirror_fetch_identifies_the_package_version():
    """The mirror Packages.gz fetch names the installed version."""
    from debian_observer.ingestion.acquisition import DebianMirrorDataSource

    source = DebianMirrorDataSource(suite="stable")
    request = _captured_request(source.retrieve, gzip.compress(b""))

    assert request.get_header("User-agent") == f"debian-observer/{__version__}"
