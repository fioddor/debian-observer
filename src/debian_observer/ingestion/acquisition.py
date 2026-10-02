"""Ingestion: Acquisition.

Retrieves or receives source data from public Debian datasets.

This module defines:

- ``DataSource`` — abstract base for all data sources
- ``InMemoryDataSource`` — simple in-memory source for tests
- ``MockDebianDataSource`` — generates realistic synthetic Debian data
- ``AcquisitionService`` — orchestrates retrieval and validation

Design notes
------------

* ``DataSource`` is intentionally minimal. R1 focuses on the pipeline
  structure; real UDD database connections can be added later as a
  ``UDDDataSource`` implementation.
* ``MockDebianDataSource`` produces deterministic output when seeded,
  making tests reproducible.
* Checksums are SHA-256 of the JSON-serialized record set, providing
  a stable fingerprint for idempotency checks.
"""

from __future__ import annotations

import hashlib
import json
import random
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class DataSource(ABC):
    """Abstract base class for Debian Observatory data sources.

    All data sources must provide:
    - A name identifying the source
    - A method to retrieve records as a list of dicts
    - A method to compute a checksum for idempotency
    - A method to report record count
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the canonical name of this data source."""
        ...

    @abstractmethod
    def retrieve(self) -> list[dict[str, Any]]:
        """Retrieve all records from this source.

        Returns:
            A list of dict records. Each dict represents one raw row.
        """
        ...

    @abstractmethod
    def compute_checksum(self) -> str:
        """Compute a SHA-256 checksum of the source data.

        This fingerprint is used for idempotency: if the checksum
        matches a previous acquisition, the data has not changed.
        """
        ...

    def record_count(self) -> int:
        """Return the number of records available.

        Default implementation retrieves and counts. Subclasses may
        override for efficiency when count is known without retrieval.
        """
        return len(self.retrieve())


# ---------------------------------------------------------------------------
# In-memory source (for tests)
# ---------------------------------------------------------------------------

class InMemoryDataSource(DataSource):
    """A simple in-memory data source for testing.

    This source wraps a pre-built list of record dicts. It is useful
    for unit tests and for pipeline validation where real network
    or database access is not required.

    Example::

        source = InMemoryDataSource(
            name="test",
            records=[
                {"package": "libc6", "version": "2.37-12"},
            ],
        )
    """

    def __init__(
        self,
        name: str,
        records: list[dict[str, Any]],
    ) -> None:
        self._name = name
        self._records = records

    @property
    def name(self) -> str:
        return self._name

    def retrieve(self) -> list[dict[str, Any]]:
        # Return a deep copy to prevent accidental mutation
        return [dict(r) for r in self._records]

    def compute_checksum(self) -> str:
        """Compute SHA-256 of canonical JSON representation."""
        return _compute_sha256(self._records)

    def record_count(self) -> int:
        return len(self._records)


# ---------------------------------------------------------------------------
# Mock Debian data source (synthetic data for development)
# ---------------------------------------------------------------------------

# Small set of realistic Debian package names for synthetic data generation
_DEBIAN_PACKAGES = [
    "libc6", "libc6-dev", "python3", "python3-dev",
    "bash", "coreutils", "dpkg", "apt",
    "gcc", "g++", "make", "build-essential",
    "vim", "nano", "emacs",
    "curl", "wget", "git", "openssh-client",
    "nginx", "apache2", "postgresql", "mysql-server",
    "redis-server", "memcached",
    "systemd", "sysvinit-core",
    "linux-image-amd64", "firmware-linux",
    "gnome-shell", "kde-plasma-desktop", "xfce4",
    "firefox", "chromium",
    "openssl", "libssl-dev",
    "zlib1g", "zlib1g-dev",
    "libpng16-16", "libjpeg62-turbo",
]

_DEBIAN_VERSIONS = [
    "2.37-12", "2.37-13", "2.38-1",
    "3.11.2-1", "3.11.2-2", "3.12.0-1",
    "5.2-21", "5.2-22",
    "9.1-1", "9.2-1",
    "1.22.0-1", "1.22.0-2",
    "2.9.1-1", "2.10.0-1",
    "1:3.11.2-1", "1:3.12.0-1",
    "2.31-13+deb12u5", "2.31-13+deb12u6",
    "1.21.6-1", "1.21.7-1",
    "8.0.33-1", "8.0.34-1",
    "7.0.12-1", "7.2.0-1",
    "252.17-1", "252.22-1",
    "6.4-4", "6.4-5",
    "1.2.11-1", "1.2.11-2",
    "1.6.40-1", "1.6.40-2",
]

_DEBIAN_DISTRIBUTIONS = ["sid", "trixie", "bookworm", "bullseye"]
_DEBIAN_COMPONENTS = ["main", "contrib", "non-free"]
_DEBIAN_ARCHITECTURES = ["amd64", "i386", "arm64", "armhf", "all", "source"]
_DEBIAN_SECTIONS = ["libs", "python", "net", "devel", "editors", "web", "kernel", "x11", "utils"]
_DEBIAN_PRIORITIES = ["required", "important", "standard", "optional", "extra"]


class MockDebianDataSource(DataSource):
    """Generates synthetic but realistic Debian package data.

    This source produces deterministic output when a seed is provided,
    making it ideal for reproducible tests and demonstrations.

    Each record represents a package version observation with fields
    that mirror the UDD ``packages`` table structure:

    - package: source package name
    - version: Debian version string
    - distribution: suite (sid, trixie, etc.)
    - architecture: target architecture
    - component: archive component
    - priority: package priority
    - section: archive section

    Example::

        source = MockDebianDataSource(package_count=100, seed=42)
        records = source.retrieve()
        # records contain realistic Debian package/version data
    """

    def __init__(
        self,
        package_count: int = 10,
        versions_per_package: int = 1,
        seed: int | None = None,
    ) -> None:
        if package_count < 1:
            raise ValueError("package_count must be >= 1")
        if versions_per_package < 1:
            raise ValueError("versions_per_package must be >= 1")

        self.package_count = package_count
        self.versions_per_package = versions_per_package
        self.seed = seed
        # Pre-generate records so that retrieve() and compute_checksum()
        # are consistent across multiple calls on the same instance.
        self._records = self._generate_records(seed)

    @property
    def name(self) -> str:
        return "mock-debian"

    def _generate_records(self, seed: int | None) -> list[dict[str, Any]]:
        """Generate synthetic package version records deterministically."""
        rng = random.Random(seed)
        records: list[dict[str, Any]] = []

        # Deterministically sample packages
        packages = rng.sample(
            _DEBIAN_PACKAGES,
            min(self.package_count, len(_DEBIAN_PACKAGES)),
        )

        for pkg in packages:
            versions = rng.sample(
                _DEBIAN_VERSIONS,
                min(self.versions_per_package, len(_DEBIAN_VERSIONS)),
            )
            for ver in versions:
                records.append({
                    "package": pkg,
                    "source": pkg,  # simplified: source = binary name
                    "version": ver,
                    "distribution": rng.choice(_DEBIAN_DISTRIBUTIONS),
                    "architecture": rng.choice(_DEBIAN_ARCHITECTURES),
                    "component": rng.choice(_DEBIAN_COMPONENTS),
                    "priority": rng.choice(_DEBIAN_PRIORITIES),
                    "section": rng.choice(_DEBIAN_SECTIONS),
                })

        return records

    def retrieve(self) -> list[dict[str, Any]]:
        """Return the pre-generated synthetic records."""
        return [dict(r) for r in self._records]

    def compute_checksum(self) -> str:
        return _compute_sha256(self._records)

    def record_count(self) -> int:
        # Deterministic count without generating all records
        actual_pkgs = min(self.package_count, len(_DEBIAN_PACKAGES))
        actual_versions = min(
            self.versions_per_package,
            len(_DEBIAN_VERSIONS),
        )
        return actual_pkgs * actual_versions


# ---------------------------------------------------------------------------
# Acquisition service
# ---------------------------------------------------------------------------

class AcquisitionService:
    """Orchestrates retrieval and initial validation of source data.

    The acquisition service is responsible for:
    - Delegating retrieval to a ``DataSource``
    - Computing checksums for idempotency
    - Recording acquisition timestamps
    - Validating that retrieved data matches expected checksums

    Usage:

        service = AcquisitionService()
        result = service.acquire(source)
        if not service.validate(result["checksum"], source):
            raise RuntimeError("Checksum mismatch")
    """

    def acquire(self, source: DataSource) -> dict[str, Any]:
        """Acquire data from a source and return acquisition metadata.

        Args:
            source: The data source to acquire from.

        Returns:
            A dict containing:
            - source_name: name of the source
            - record_count: number of records retrieved
            - checksum: SHA-256 of the data
            - acquired_at: ISO-format UTC timestamp
        """
        records = source.retrieve()
        checksum = source.compute_checksum()

        return {
            "source_name": source.name,
            "record_count": len(records),
            "checksum": checksum,
            "acquired_at": datetime.now(timezone.utc).isoformat(),
        }

    def validate(self, expected_checksum: str, source: DataSource) -> bool:
        """Validate that a source still matches a known checksum.

        This is used for incremental ingestion: if the checksum
        matches, the data has not changed since last acquisition.

        Args:
            expected_checksum: The previously recorded checksum.
            source: The source to re-check.

        Returns:
            True if the current data matches the expected checksum.
        """
        current = source.compute_checksum()
        return current == expected_checksum


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _compute_sha256(records: list[dict[str, Any]]) -> str:
    """Compute a SHA-256 checksum of a list of record dicts.

    Uses a canonical JSON representation (sorted keys, no whitespace)
    to ensure the checksum is stable across runs.
    """
    serialized = json.dumps(
        records,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

# ---------------------------------------------------------------------------
# Real Debian data source (smoke test)
# ---------------------------------------------------------------------------

class SourcesDebianOrgDataSource(DataSource):
    """Fetches real Debian package version data from sources.debian.org API.

    This is a minimal real-data source for smoke testing the pipeline.
    It queries the sources.debian.org REST API for a given package name
    and produces records that mirror the UDD structure.

    Usage:

        source = SourcesDebianOrgDataSource(package_name="hello")
        records = source.retrieve()
    """

    def __init__(self, package_name: str = "hello") -> None:
        self._package_name = package_name
        self._url = f"https://sources.debian.org/api/src/{package_name}/"
        self._records: list[dict[str, Any]] | None = None

    @property
    def name(self) -> str:
        return "sources.debian.org"

    def retrieve(self) -> list[dict[str, Any]]:
        """Download and parse package version history."""
        import urllib.request
        if self._records is not None:
            return self._records

        req = urllib.request.Request(
            self._url,
            headers={"User-Agent": "debian-observer-smoke-test"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())

        records = []
        for v in data.get("versions", []):
            for suite in v.get("suites", []):
                records.append({
                    "package": self._package_name,
                    "version": v["version"],
                    "distribution": suite,
                    "area": v.get("area", "main"),
                    "source": self.name,
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                })
        self._records = records
        return records

    def compute_checksum(self) -> str:
        """Compute SHA-256 of canonical JSON representation."""
        canonical = json.dumps(self.retrieve(), sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Debian mirror data source (Packages.gz)
# ---------------------------------------------------------------------------

class DebianMirrorDataSource(DataSource):
    """Fetches real Debian package metadata from a Debian mirror.

    Downloads and parses a ``Packages.gz`` file from any Debian mirror,
    producing records that match the UDD structure. The parser is
    deliberately minimal: it extracts core fields (package, version,
    architecture, distribution) and ignores optional fields.

    The ``limit`` parameter truncates the result to a fixed number of
    packages, making the first ingestion fast enough for smoke tests
    on slow connections.

    Usage:

        source = DebianMirrorDataSource(
            suite="stable", component="main", architecture="amd64", limit=5
        )
        records = source.retrieve()
    """

    MIRROR_DEFAULT = "https://deb.debian.org/debian"

    def __init__(
        self,
        *,
        suite: str = "stable",
        component: str = "main",
        architecture: str = "amd64",
        limit: int = 5,
        mirror: str = MIRROR_DEFAULT,
    ) -> None:
        self._suite = suite
        self._component = component
        self._architecture = architecture
        self._limit = limit
        self._mirror = mirror.rstrip("/")
        self._url = (
            f"{self._mirror}/dists/{suite}/{component}/binary-{architecture}/Packages.gz"
        )
        self._records: list[dict[str, Any]] | None = None

    @property
    def name(self) -> str:
        return "debian.mirror"

    def retrieve(self) -> list[dict[str, Any]]:
        """Download and parse Packages.gz from the mirror."""
        import gzip
        import urllib.request

        if self._records is not None:
            return self._records

        req = urllib.request.Request(
            self._url,
            headers={"User-Agent": "debian-observer/0.1.0"},
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = gzip.decompress(resp.read())

        records = []
        current: dict[str, str] = {}
        count = 0

        for line in raw.decode("utf-8", errors="replace").splitlines():
            if line.startswith("Package: "):
                current = {"package": line.split(": ", 1)[1]}
            elif line.startswith("Version: "):
                current["version"] = line.split(": ", 1)[1]
            elif line.startswith("Architecture: "):
                current["architecture"] = line.split(": ", 1)[1]
            elif line == "" and current:
                records.append({
                    "package": current.get("package", ""),
                    "version": current.get("version", ""),
                    "distribution": self._suite,
                    "architecture": current.get("architecture", self._architecture),
                    "component": self._component,
                    "source": self.name,
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                })
                current = {}
                count += 1
                if count >= self._limit:
                    break

        self._records = records
        return records

    def compute_checksum(self) -> str:
        """Compute SHA-256 of canonical JSON representation."""
        canonical = json.dumps(self.retrieve(), sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
