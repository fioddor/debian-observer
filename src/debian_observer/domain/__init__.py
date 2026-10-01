"""Domain models for the Debian Observatory canonical data model.

This package defines the core entities used to represent Debian
packages, versions, releases, migrations, and observation metadata
in a normalized, source-agnostic form.
"""

from debian_observer.domain.models import (
    Migration,
    Observation,
    Package,
    PackageVersion,
    Release,
)

__all__ = [
    "Package",
    "PackageVersion",
    "Release",
    "Migration",
    "Observation",
]
