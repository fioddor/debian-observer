"""API: In-memory data store.

Provides an in-memory repository for domain models, events, and datasets.

This store is intentionally simple for R1. It supports:
- Package, PackageVersion, Release, Migration lookups
- Event lookups by event_id
- Dataset manifest lookups by name

Future releases will replace this with analytical storage (Parquet/DuckDB).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from debian_observer.domain.models import Package, PackageVersion, Release, Migration
from debian_observer.events import EventEnvelope


@dataclass
class InMemoryStore:
    """In-memory repository for Debian Observatory data.

    Attributes:
        packages: Dict mapping package_id → Package.
        package_versions: Dict mapping package_id → list of PackageVersion.
        releases: Dict mapping release_id → Release.
        migrations: Dict mapping migration_id → Migration.
        events: Dict mapping event_id (str) → EventEnvelope.
        datasets: Dict mapping dataset_name → dataset manifest dict.
    """

    packages: dict[str, Package] = field(default_factory=dict)
    package_versions: dict[str, list[PackageVersion]] = field(default_factory=dict)
    releases: dict[str, Release] = field(default_factory=dict)
    migrations: dict[str, Migration] = field(default_factory=dict)
    events: dict[str, EventEnvelope] = field(default_factory=dict)
    datasets: dict[str, dict[str, Any]] = field(default_factory=dict)

    # -----------------------------------------------------------------------
    # Package operations
    # -----------------------------------------------------------------------

    def add_package(self, package: Package) -> None:
        self.packages[package.package_id] = package

    def get_package(self, name: str) -> Package | None:
        """Return a package by its human-readable name (not package_id).

        Args:
            name: The package name (e.g. "libc6").

        Returns:
            The Package if found, None otherwise.
        """
        for pkg in self.packages.values():
            if pkg.name == name:
                return pkg
        return None

    def get_package_by_id(self, package_id: str) -> Package | None:
        return self.packages.get(package_id)

    def list_packages(self) -> list[Package]:
        return list(self.packages.values())

    # -----------------------------------------------------------------------
    # Package version operations
    # -----------------------------------------------------------------------

    def add_package_version(self, version: PackageVersion) -> None:
        self.package_versions.setdefault(version.package_id, []).append(version)

    def get_package_versions(self, package_name: str) -> list[PackageVersion] | None:
        """Return versions for a package by its human-readable name."""
        pkg = self.get_package(package_name)
        if pkg is None:
            return None
        return self.package_versions.get(pkg.package_id, [])

    def list_package_versions(self) -> list[PackageVersion]:
        result: list[PackageVersion] = []
        for versions in self.package_versions.values():
            result.extend(versions)
        return result

    # -----------------------------------------------------------------------
    # Release operations
    # -----------------------------------------------------------------------

    def add_release(self, release: Release) -> None:
        self.releases[release.release_id] = release

    def get_release(self, codename: str) -> Release | None:
        for rel in self.releases.values():
            if rel.codename == codename:
                return rel
        return None

    def list_releases(self) -> list[Release]:
        return list(self.releases.values())

    # -----------------------------------------------------------------------
    # Migration operations
    # -----------------------------------------------------------------------

    def add_migration(self, migration: Migration) -> None:
        self.migrations[migration.migration_id] = migration

    def list_migrations(self) -> list[Migration]:
        return list(self.migrations.values())

    # -----------------------------------------------------------------------
    # Event operations
    # -----------------------------------------------------------------------

    def add_event(self, event: EventEnvelope) -> None:
        self.events[str(event.event_id)] = event

    def get_event(self, event_id: str) -> EventEnvelope | None:
        return self.events.get(event_id)

    def list_events(self) -> list[EventEnvelope]:
        return list(self.events.values())

    # -----------------------------------------------------------------------
    # Dataset operations
    # -----------------------------------------------------------------------

    def add_dataset(self, name: str, manifest: dict[str, Any]) -> None:
        self.datasets[name] = manifest

    def get_dataset(self, name: str) -> dict[str, Any] | None:
        return self.datasets.get(name)

    def list_datasets(self) -> list[dict[str, Any]]:
        return list(self.datasets.values())
