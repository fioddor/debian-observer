"""Shared type definitions and enumerations for the Debian Observatory.

This module provides canonical identifiers, enumerations, and type aliases
used across domain and event models to ensure consistency.
"""

from __future__ import annotations

from enum import Enum
from typing import Literal

# ---------------------------------------------------------------------------
# Entity type identifiers (used in entity_id composition)
# ---------------------------------------------------------------------------

EntityType = Literal["package", "package_version", "release", "migration"]

# ---------------------------------------------------------------------------
# Event type identifiers (canonical event type strings)
# ---------------------------------------------------------------------------

EventType = Literal[
    "PackageObserved",
    "PackageVersionObserved",
    "ReleaseObserved",
    "MigrationObserved",
    "DependencyObserved",
    "PopconObserved",
]

# ---------------------------------------------------------------------------
# Release status values (from Debian release semantics)
# ---------------------------------------------------------------------------

ReleaseStatus = Literal[
    "stable",
    "testing",
    "unstable",
    "experimental",
    "oldstable",
    "frozen",
]

# ---------------------------------------------------------------------------
# Architecture values (common Debian architectures)
# ---------------------------------------------------------------------------

Architecture = Literal[
    "all",
    "amd64",
    "arm64",
    "armel",
    "armhf",
    "i386",
    "mips64el",
    "mipsel",
    "ppc64el",
    "riscv64",
    "s390x",
    "source",
]


class MigrationStatus(str, Enum):
    """Canonical migration status values.

    The migration model is deliberately simplified - the full
    Britney state machine (testing, proposed, unblocked, etc.)
    is a downstream question.
    """

    UNKNOWN = "unknown"
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"


class SchemaVersion(int, Enum):
    """Event schema versions.

    All events start at schema_version=1.
    Incompatible changes must bump this version.
    """

    V1 = 1


# Type alias for canonical version strings
DebianVersionString = str
