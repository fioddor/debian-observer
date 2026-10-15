"""Compliance tests: licensing (REUSE) and SBOM (SPDX).

Covers the repository compliance feature:
- ``docs/SBOM.spdx.json`` is a SPDX 2.3 document that covers every declared
  runtime dependency and embeds no license texts.
- ``reuse lint`` is clean over the whole tree.
"""

import json
import re
import subprocess
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _normalize(name: str) -> str:
    """Normalize a package name following PEP 503."""
    return re.sub(r"[-_.]+", "-", name).lower()


def _dependency_name(dependency: str) -> str:
    """Strip version and extras markers from a PEP 508 dependency string."""
    return _normalize(re.split(r"[\[<>=!~; ]", dependency)[0])


def _declared_dependencies() -> list[str]:
    pyproject_text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    dependencies = tomllib.loads(pyproject_text)["project"]["dependencies"]
    return [_dependency_name(dependency) for dependency in dependencies]


def test_sbom_spdx_covers_declared_deps():
    sbom_path = REPO_ROOT / "docs" / "SBOM.spdx.json"
    sbom = json.loads(sbom_path.read_text(encoding="utf-8"))

    assert sbom["spdxVersion"] == "SPDX-2.3"
    assert sbom["dataLicense"] == "CC0-1.0"

    extracted = sbom.get("hasExtractedLicensingInfos") or []
    assert not extracted, "SBOM must not embed license texts"

    packages = {_normalize(package["name"]) for package in sbom["packages"]}
    missing = [dep for dep in _declared_dependencies() if dep not in packages]
    assert not missing, f"declared dependencies missing from SBOM: {missing}"


def test_reuse_lint_clean():
    result = subprocess.run(
        ["reuse", "lint"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"reuse lint reported violations (rc={result.returncode}):"
        f"\n{result.stdout}{result.stderr}"
    )
