"""Compliance tests: licensing (REUSE) and SBOM (SPDX).

Covers the repository compliance feature:
- ``docs/SBOM.spdx.json`` is a SPDX 2.3 document that covers every declared
  runtime dependency and embeds no license texts.
- ``reuse lint`` is clean over the whole tree.
"""

import json
import re
from fnmatch import fnmatch
import subprocess
import sys
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
    # Run reuse via the running interpreter (python -m): the PATH may not
    # carry the venv Scripts/bin dir (container without activation), while
    # sys.executable is the interpreter whose site-packages installed reuse.
    result = subprocess.run(
        [sys.executable, "-m", "reuse", "lint"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"reuse lint reported violations (rc={result.returncode}):"
        f"\n{result.stdout}{result.stderr}"
    )

#: The license the project is distributed under (REUSE.toml's
#: SPDX-License-Concluded and the default annotation).
PROJECT_LICENSE = "AGPL-3.0-or-later"
#: The directory the license texts live in (REUSE 3.3: each License
#: File lives in LICENSES/, at the root of the project).
LICENSES_DIR = "LICENSES"
#: Root files a build backend picks up as license texts by default:
#: none of them may exist, or the texts travel twice.
ROOT_LICENSE_FILES = ("LICENSE", "COPYING", "NOTICE", "AUTHORS")


def test_manifest_declares_the_license_files() -> None:
    """The wheel ships the license texts that LICENSES/ holds.

    The manifest declares the license as an SPDX expression and
    ``license-files`` patterns that cover ``LICENSES/<expression>.txt``
    (the file exists), so a build that follows the manifest carries
    the texts with the package.
    """
    manifest = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = manifest["project"]
    assert project["license"] == PROJECT_LICENSE
    patterns = project["license-files"]
    license_file = REPO_ROOT / LICENSES_DIR / f"{PROJECT_LICENSE}.txt"
    assert license_file.exists(), f"{license_file} does not exist"
    relative = license_file.relative_to(REPO_ROOT).as_posix()
    covering = [p for p in patterns if fnmatch(relative, p)]
    assert covering, f"license-files {patterns} do not cover {relative}"


def test_no_license_text_at_the_root() -> None:
    """No license text lives at the repository root.

    The texts live exactly once, in LICENSES/; a root file the build
    backend picks up by default would be a second copy.
    """
    stray = [name for name in ROOT_LICENSE_FILES if (REPO_ROOT / name).exists()]
    assert not stray, f"license texts at the repository root: {stray}"
