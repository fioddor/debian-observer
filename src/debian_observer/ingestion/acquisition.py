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
import os
import random
import re
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
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
# Checksum: hash the OBSERVED DATA, never the observation timestamps
# ---------------------------------------------------------------------------


def _stable_records_checksum(records: list[dict[str, Any]]) -> str:
    """SHA-256 of the canonical JSON of the data records.

    The checksum pins what was OBSERVED, not WHEN it was observed:
    ``observed_at`` is excluded (stripped per record) because it is
    generated at acquisition time — including it makes every acquisition
    of identical data hash differently, which kills the idempotency
    contract of :meth:`AcquisitionService.validate`.
    """
    stable = [
        {k: v for k, v in record.items() if k != "observed_at"}
        for record in records
    ]
    canonical = json.dumps(stable, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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
    "libc6",
    "libc6-dev",
    "python3",
    "python3-dev",
    "bash",
    "coreutils",
    "dpkg",
    "apt",
    "gcc",
    "g++",
    "make",
    "build-essential",
    "vim",
    "nano",
    "emacs",
    "curl",
    "wget",
    "git",
    "openssh-client",
    "nginx",
    "apache2",
    "postgresql",
    "mysql-server",
    "redis-server",
    "memcached",
    "systemd",
    "sysvinit-core",
    "linux-image-amd64",
    "firmware-linux",
    "gnome-shell",
    "kde-plasma-desktop",
    "xfce4",
    "firefox",
    "chromium",
    "openssl",
    "libssl-dev",
    "zlib1g",
    "zlib1g-dev",
    "libpng16-16",
    "libjpeg62-turbo",
]

_DEBIAN_VERSIONS = [
    "2.37-12",
    "2.37-13",
    "2.38-1",
    "3.11.2-1",
    "3.11.2-2",
    "3.12.0-1",
    "5.2-21",
    "5.2-22",
    "9.1-1",
    "9.2-1",
    "1.22.0-1",
    "1.22.0-2",
    "2.9.1-1",
    "2.10.0-1",
    "1:3.11.2-1",
    "1:3.12.0-1",
    "2.31-13+deb12u5",
    "2.31-13+deb12u6",
    "1.21.6-1",
    "1.21.7-1",
    "8.0.33-1",
    "8.0.34-1",
    "7.0.12-1",
    "7.2.0-1",
    "252.17-1",
    "252.22-1",
    "6.4-4",
    "6.4-5",
    "1.2.11-1",
    "1.2.11-2",
    "1.6.40-1",
    "1.6.40-2",
]

# Codenames/pockets, mirroring the UDD release column
_DEBIAN_RELEASES = ["sid", "trixie", "bookworm", "bullseye"]
_DEBIAN_COMPONENTS = ["main", "contrib", "non-free"]
_DEBIAN_ARCHITECTURES = ["amd64", "i386", "arm64", "armhf", "all", "source"]
_DEBIAN_SECTIONS = [
    "libs",
    "python",
    "net",
    "devel",
    "editors",
    "web",
    "kernel",
    "x11",
    "utils",
]
_DEBIAN_PRIORITIES = ["required", "important", "standard", "optional", "extra"]


class MockDebianDataSource(DataSource):
    """Generates synthetic but realistic Debian package data.

    This source produces deterministic output when a seed is provided,
    making it ideal for reproducible tests and demonstrations.

    Each record represents a package version observation with fields
    that mirror the UDD ``packages`` table structure:

    - package: source package name
    - version: Debian version string
    - distribution: archive family (debian)
    - release: pocket (sid, trixie, etc.)
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
                records.append(
                    {
                        "package": pkg,
                        "source": pkg,  # simplified: source = binary name
                        "version": ver,
                        "distribution": "debian",
                        "release": rng.choice(_DEBIAN_RELEASES),
                        "suite": None,
                        "architecture": rng.choice(_DEBIAN_ARCHITECTURES),
                        "component": rng.choice(_DEBIAN_COMPONENTS),
                        "priority": rng.choice(_DEBIAN_PRIORITIES),
                        "section": rng.choice(_DEBIAN_SECTIONS),
                    }
                )

        return records

    def retrieve(self) -> list[dict[str, Any]]:
        """Return the pre-generated synthetic records."""
        return [dict(r) for r in self._records]

    def compute_checksum(self) -> str:
        return _stable_records_checksum(self._records)

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
            for suite_value in v.get("suites", []):
                # sources.debian.org mixes codenames and suite roles
                # in its suites list: classify each value with the
                # same rule as the mirror dists/ path.
                suite_role, release = _classify_dists_path(suite_value)
                records.append(
                    {
                        "package": self._package_name,
                        "version": v["version"],
                        "distribution": "debian",
                        "suite": suite_role,
                        "release": release,
                        "component": v.get("area", "main"),
                        "source": self.name,
                        "observed_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
        self._records = records
        return records

    def compute_checksum(self) -> str:
        """Compute SHA-256 of canonical JSON representation."""
        return _stable_records_checksum(self.retrieve())


# ---------------------------------------------------------------------------
# Debian mirror data source (Packages.gz)
# ---------------------------------------------------------------------------

# Archive families the mirror layout (dists/<suite>/...) can describe.
# ``distribution`` in records means exactly this family, never a suite.
_ARCHIVE_FAMILIES = frozenset({"debian", "ubuntu"})

# Debian suite roles: the lifecycle aliases usable in the dists/ path
# (dists/stable, dists/testing). Every other path value is a codename
# or pocket (bookworm, sid, bookworm-security) and belongs in the
# ``release`` field, not in ``suite``.
_SUITE_ROLES = frozenset(
    {
        "oldoldstable",
        "oldstable",
        "stable",
        "testing",
        "unstable",
        "experimental",
    }
)


def _classify_dists_path(value: str) -> tuple[str | None, str | None]:
    """Split a dists/ path segment or API suite value into (suite, release).

    A suite role (oldstable, stable, testing) is a lifecycle alias and
    belongs in ``suite``; any other value (codename or pocket: bookworm,
    sid, bookworm-security) belongs in ``release``.
    """
    if value in _SUITE_ROLES:
        return value, None
    return None, value

class DebianMirrorDataSource(DataSource):
    """Fetches real Debian package metadata from a Debian mirror.

    Downloads and parses a ``Packages.gz`` file from any Debian mirror,
    producing records that match the UDD structure. The parser extracts
    package, version, architecture, Section and Priority.

    ``distribution`` is the archive family: ``debian``, or ``ubuntu``
    for Ubuntu mirrors (same dists/ layout). The ``dists/<suite>/``
    path value lands in ``suite`` when it is a suite role (oldstable,
    stable, testing) and in ``release`` when it is a codename or pocket
    (bookworm, sid, bookworm-security).

    The ``limit`` parameter truncates the result to a fixed number
    of packages. The default ``None`` ingests the whole file.

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
        limit: int | None = None,
        mirror: str = MIRROR_DEFAULT,
        distribution: str = "debian",
    ) -> None:
        if distribution not in _ARCHIVE_FAMILIES:
            raise ValueError(
                f"distribution must be one of {sorted(_ARCHIVE_FAMILIES)}, "
                f"got {distribution!r}"
            )
        self._suite = suite
        self._component = component
        self._architecture = architecture
        self._limit = limit
        self._mirror = mirror.rstrip("/")
        self._distribution = distribution
        # The dists/<suite>/ path segment is either a suite role (stable,
        # testing) or a codename/pocket (bookworm, sid): classify once
        # so every record lands the value in the correct field.
        self._suite_role, self._release_path = _classify_dists_path(suite)
        self._url = f"{self._mirror}/dists/{suite}/{component}/binary-{architecture}/Packages.gz"
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
            elif line.startswith("Section: "):
                current["section"] = line.split(": ", 1)[1]
            elif line.startswith("Priority: "):
                current["priority"] = line.split(": ", 1)[1]
            elif line == "" and current:
                records.append(
                    {
                        "package": current.get("package", ""),
                        "version": current.get("version", ""),
                        "distribution": self._distribution,
                        "suite": self._suite_role,
                        "release": self._release_path,
                        "architecture": current.get("architecture", self._architecture),
                        "component": self._component,
                        "section": current.get("section"),
                        "priority": current.get("priority"),
                        "source": self.name,
                        "observed_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                current = {}
                count += 1
                if self._limit is not None and count >= self._limit:
                    break

        self._records = records
        return records

    def compute_checksum(self) -> str:
        """Compute SHA-256 of canonical JSON representation."""
        canonical = json.dumps(self.retrieve(), sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# UDD pg_dump file source (ingest udddump)
# ---------------------------------------------------------------------------


class UDDDumpError(RuntimeError):
    """A ``pg_restore`` run over a UDD dump file failed."""


def _run_pg_restore(args: list[str]) -> str:
    """Run ``pg_restore`` on the host and return its stdout.

    ``pg_restore`` is a host dependency (PATH) — the dump file is
    read directly; no container indirection and no database is
    created or restored (ADR-006).
    """
    command = ["pg_restore", *args]
    try:
        result = subprocess.run(
            command, capture_output=True, text=True, check=True
        )
    except FileNotFoundError as exc:
        raise UDDDumpError(
            "pg_restore not found on PATH; install postgresql-client"
        ) from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or "").strip().splitlines()
        tail = detail[-1] if detail else f"exit code {exc.returncode}"
        raise UDDDumpError(f"pg_restore failed: {tail}") from exc
    return result.stdout


# Only TABLE DATA entries name restorable tables; schema, sequence and
# index entries are ignored. "<oid>; <db-oid> <table-oid> TABLE DATA
# <schema> <table> [<owner>]".
_TOC_TABLE_DATA_RE = re.compile(
    r"^\s*\d+;\s*\d+\s+\d+\s+TABLE DATA\s+\S+\s+(\S+)",
    re.MULTILINE,
)


def parse_dump_toc_tables(toc_text: str) -> list[str]:
    """Return the restorable table names of a ``pg_restore --list`` TOC.

    Order is TOC order; repeated entries are dropped.
    """
    tables: list[str] = []
    for match in _TOC_TABLE_DATA_RE.finditer(toc_text):
        name = match.group(1)
        if name not in tables:
            tables.append(name)
    return tables


def list_dump_tables(file: str) -> list[str]:
    """List the restorable tables of a UDD pg_dump file (read-only TOC)."""
    return parse_dump_toc_tables(_run_pg_restore(["--list", file]))


# ---------------------------------------------------------------------------
# Table shape perimeter (ingest udddump startup gate)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TableShape:
    """One ingestible table shape: required COPY columns.

    ``required_columns`` must all be present; ``required_any`` needs
    at least one of them. The `packages` shape mirrors the validator
    (`UDDValidator.REQUIRED_FIELDS` / `PACKAGE_FIELDS`): `version`
    plus any of `package`/`source` — the real `ci` table carries
    `source`+`version` and ingests fine (2026-10-22 real-dump smoke).
    """

    name: str
    required_columns: frozenset[str]
    required_any: frozenset[str] = frozenset()


# The registry drives the startup gate: a new shape is data, not gate
# code (future-ready — today only `packages` is ingestible).
SUPPORTED_TABLE_SHAPES: tuple[TableShape, ...] = (
    TableShape(
        name="packages",
        required_columns=frozenset({"version"}),
        required_any=frozenset({"package", "source"}),
    ),
)


def _render_shape(shape: TableShape) -> str:
    """Render one supported shape for gate error messages."""
    text = f"{shape.name} ({', '.join(sorted(shape.required_columns))}"
    if shape.required_any:
        text += f" + any of: {', '.join(sorted(shape.required_any))}"
    return text + ")"


def match_table_shape(columns: list[str]) -> TableShape | None:
    """Return the registered shape whose requirements ``columns`` carry."""
    for shape in SUPPORTED_TABLE_SHAPES:
        if not shape.required_columns.issubset(columns):
            continue
        if shape.required_any and not shape.required_any.intersection(columns):
            continue
        return shape
    return None


def assert_supported_table_shape(columns: list[str], *, table: str) -> TableShape:
    """Fail fast when a table's COPY header matches no registered shape.

    The error names the cause (the table and its COPY header) and the
    perimeter (the supported shapes and their required columns) — a
    usage message, never a traceback.
    """
    shape = match_table_shape(columns)
    if shape is None:
        supported = ", ".join(_render_shape(s) for s in SUPPORTED_TABLE_SHAPES)
        raise UDDDumpError(
            f"table {table!r} is not shaped like any supported table "
            f"(COPY header columns: {', '.join(columns) or '<none>'}); "
            f"supported shapes: {supported}"
        )
    return shape


def peek_dump_copy_columns(file: str, *, table: str) -> list[str]:
    """Peek the COPY header of a data-only restore: the table's columns.

    Streams ``pg_restore --data-only --table`` and scans past the
    preamble — ``--`` comments, ``\`` directives and ``SET`` statements
    (pg_restore 17) — to the COPY header, then closes the stream (no
    data restored, ADR-006). This is the shape gate's probe: it costs
    a preamble, not one table.
    """
    command = [
        "pg_restore",
        "--data-only",
        "--table",
        table,
        "--file",
        "-",
        file,
    ]
    try:
        proc = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError as exc:
        raise UDDDumpError(
            "pg_restore not found on PATH; install postgresql-client"
        ) from exc
    try:
        assert proc.stdout is not None
        header = ""
        first_content = ""
        # Real dumps open with a preamble before the COPY block (2026-10-22
        # real-dump smoke): `--` comments, `\restrict` directives and `SET`
        # statements. Scan for the COPY header itself — cap the scan,
        # never restore data — and remember the first content line for
        # the error message when no header shows up.
        for _ in range(256):
            line = proc.stdout.readline()
            if not line:
                break
            stripped = line.strip()
            if not stripped or stripped.startswith(("--", "\\")):
                continue
            if _COPY_HEADER_RE.match(line):
                header = line
                break
            if not first_content:
                first_content = line
    finally:
        proc.terminate()
        proc.wait()
    if not header:
        if first_content:
            raise UDDDumpError(
                f"pg_restore emitted no COPY header for table {table!r} "
                f"(first non-comment line: {first_content.strip()!r})"
            )
        raise UDDDumpError(
            f"pg_restore emitted no COPY header for table {table!r} "
            f"(empty stream — the table may have no data, or the dump "
            f"is unreadable)"
        )
    # header is only set on a COPY match — parse its column list.
    match = _COPY_HEADER_RE.match(header)
    assert match is not None
    return [column.strip() for column in match.group("columns").split(",")]


# A COPY block header: "COPY <schema.table> (<columns>) FROM stdin;".
# MULTILINE is a compile flag: .search()'s second argument is pos,
# not flags (bit me 2026-10-21).
_COPY_HEADER_RE = re.compile(
    r"^COPY \S+ \((?P<columns>[^)]*)\) FROM stdin;$", re.MULTILINE
)


def _unescape_copy_field(field: str) -> str | None:
    """Unescape one COPY data field: ``\\N`` is NULL; backslash escapes."""
    if field == r"\N":
        return None
    out: list[str] = []
    i = 0
    while i < len(field):
        char = field[i]
        if char == "\\" and i + 1 < len(field):
            nxt = field[i + 1]
            if nxt == "t":
                out.append("\t")
            elif nxt == "r":
                out.append("\r")
            elif nxt == "n":
                out.append("\n")
            else:
                out.append(nxt)
            i += 2
        else:
            out.append(char)
            i += 1
    return "".join(out)


def _parse_copy_block(
    stdout: str, *, source_name: str, limit: int | None
) -> list[dict[str, Any]]:
    """Map a ``pg_restore --data-only`` COPY block to UDD records.

    Columns are mapped by name from the COPY header — tables that are
    not packages-shaped still map (missing columns read as None and
    fall back to "unknown" like the live UDD source).
    """
    match = _COPY_HEADER_RE.search(stdout)
    if match is None:
        raise UDDDumpError(
            "pg_restore emitted no COPY block for the requested table"
        )
    columns = [c.strip() for c in match.group("columns").split(",")]
    observed_at = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []
    for line in stdout[match.end() :].splitlines():
        if line == r"\.":
            break
        if not line.strip():
            continue
        values = [_unescape_copy_field(field) for field in line.split("\t")]
        row: dict[str, Any] = dict(zip(columns, values))
        records.append(
            {
                "package": row.get("package") or "unknown",
                "source_package": row.get("source"),
                "version": row.get("version") or "unknown",
                "distribution": row.get("distribution") or "unknown",
                "release": row.get("release") or "unknown",
                "suite": None,  # UDD exposes pockets, not suite roles
                "architecture": row.get("architecture") or "unknown",
                "component": row.get("component") or "unknown",
                "section": row.get("section"),
                "priority": row.get("priority"),
                "source": source_name,
                "observed_at": observed_at,
            }
        )
        if limit is not None and len(records) >= limit:
            break
    return records


class UDDDumpDataSource(DataSource):
    """Reads one table of a UDD ``pg_dump`` custom-format file.

    Rows stream from ``pg_restore --data-only --table`` on the host
    (``pg_restore`` on PATH): no database is created or restored
    (ADR-006). ``file`` is the host path to the dump; ``table`` must
    name a restorable TABLE DATA entry — validate with
    :func:`list_dump_tables` (the CLI gate does).

    Usage::

        source = UDDDumpDataSource(
            file="/srv/udd/udd.dump", table="packages", limit=10
        )
        records = source.retrieve()
    """

    def __init__(
        self,
        *,
        file: str,
        table: str = "packages",
        limit: int | None = None,
    ) -> None:
        self._file = file
        self._table = table
        self._limit = limit
        self._records: list[dict[str, Any]] | None = None

    @property
    def name(self) -> str:
        return "udd.dump"

    def retrieve(self) -> list[dict[str, Any]]:
        """Stream the table's COPY block out of the dump file."""
        if self._records is not None:
            return self._records
        # --file - streams the COPY block to stdout: pg_restore requires
        # an explicit output target (-d or -f) for data restores — caught
        # by the 2026-10-21 real-dump smoke, invisible to the unit mocks.
        stdout = _run_pg_restore(
            [
                "--data-only",
                "--table",
                self._table,
                "--file",
                "-",
                self._file,
            ]
        )
        self._records = _parse_copy_block(
            stdout, source_name=self.name, limit=self._limit
        )
        return self._records

    def compute_checksum(self) -> str:
        """Compute SHA-256 of canonical JSON representation."""
        canonical = json.dumps(self.retrieve(), sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# UDD data source
# ---------------------------------------------------------------------------


class UDDDataSource(DataSource):
    """Connects to the public UDD mirror via PostgreSQL.

    Queries the ultimate Debian database (UDD) public mirror at
    udd-mirror.debian.net for real package metadata.

    Connection parameters can be overridden via constructor arguments
    or environment variables. Constructor arguments take highest priority,
    followed by env vars, then built-in defaults.

    Usage:

        source = UDDDataSource(table="sources", limit=10)
        records = source.retrieve()
    """

    @property
    def _host(self) -> str:
        return self._host_override or os.getenv("UDD_HOST", "udd-mirror.debian.net")

    @property
    def _port(self) -> int:
        port = self._port_override or os.getenv("UDD_PORT", "5432")
        return int(port)

    @property
    def _database(self) -> str:
        return self._database_override or os.getenv("UDD_DATABASE", "udd")

    @property
    def _user(self) -> str:
        return self._user_override or os.getenv("UDD_USER", "udd-mirror")

    @property
    def _password(self) -> str:
        return self._password_override or os.getenv("UDD_PASSWORD", "udd-mirror")

    def __init__(
        self,
        *,
        table: str = "packages",
        limit: int | None = None,
        host: str | None = None,
        port: str | None = None,
        database: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        self._table = table
        self._limit = limit
        self._records: list[dict[str, Any]] | None = None
        self._host_override = host
        self._port_override = port
        self._database_override = database
        self._user_override = user
        self._password_override = password

    @property
    def name(self) -> str:
        return "udd"

    def retrieve(self) -> list[dict[str, Any]]:
        """Query the UDD public mirror."""
        import psycopg2

        if self._records is not None:
            return self._records

        conn = psycopg2.connect(
            host=self._host,
            port=self._port,
            database=self._database,
            user=self._user,
            password=self._password,
        )
        with conn.cursor() as cursor:
            query = (
                f"SELECT package, source, version, distribution, release, "
                f"architecture, section, priority, component FROM {self._table}"
            )
            if self._limit is not None:
                query += " LIMIT %s"
                cursor.execute(query, (self._limit,))
            else:
                cursor.execute(query)
            rows = cursor.fetchall()
        conn.close()

        records = []
        for row in rows:
            records.append(
                {
                    "package": row[0] or "unknown",
                    "source_package": row[1],
                    "version": row[2] or "unknown",
                    "distribution": row[3] or "unknown",
                    "release": row[4] or "unknown",
                    "suite": None,  # UDD exposes pockets, not suite roles
                    "architecture": row[5] or "unknown",
                    "component": row[8] or "unknown",
                    "section": row[6],
                    "priority": row[7],
                    "source": self.name,
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                }
            )

        self._records = records
        return records

    def compute_checksum(self) -> str:
        """Compute SHA-256 of canonical JSON representation."""
        canonical = json.dumps(self.retrieve(), sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
