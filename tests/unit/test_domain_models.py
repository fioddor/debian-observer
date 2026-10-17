"""Unit tests for Debian Observatory domain models."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from debian_observer.common.types import MigrationStatus
from debian_observer.domain.models import (
    Migration,
    Observation,
    Package,
    PackageVersion,
    Release,
)

# ---------------------------------------------------------------------------
# Package tests
# ---------------------------------------------------------------------------


class TestPackage:
    """Tests for the Package domain model."""

    def test_minimal_construction(self) -> None:
        pkg = Package(package_id="python3", name="python3")
        assert pkg.package_id == "python3"
        assert pkg.name == "python3"
        assert pkg.source_package is None
        assert pkg.section is None
        assert pkg.architecture is None

    def test_full_construction(self) -> None:
        pkg = Package(
            package_id="libc6:amd64",
            name="libc6",
            source_package="glibc",
            section="libs",
            architecture="amd64",
            maintainer="GNU Libc Maintainers <debian-glibc@lists.debian.org>",
            description="GNU C Library: Shared libraries",
        )
        assert pkg.source_package == "glibc"
        assert pkg.section == "libs"
        assert pkg.architecture == "amd64"

    def test_immutability(self) -> None:
        pkg = Package(package_id="test", name="test")
        with pytest.raises(ValidationError):
            pkg.name = "changed"  # type: ignore[misc]

    def test_empty_name_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Package(package_id="test", name="")

    def test_invalid_package_name_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Package(package_id="bad", name="UPPERCASE")

    def test_single_char_name_allowed(self) -> None:
        """Single-character package names are valid in Debian."""
        pkg = Package(package_id="r", name="r")
        assert pkg.name == "r"

    def test_hashable(self) -> None:
        pkg1 = Package(package_id="aa", name="aa")
        pkg2 = Package(package_id="aa", name="aa")
        assert hash(pkg1) == hash(pkg2)
        assert pkg1 == pkg2

    def test_equality_different_values(self) -> None:
        pkg1 = Package(package_id="a", name="a")
        pkg2 = Package(package_id="b", name="b")
        assert pkg1 != pkg2


# ---------------------------------------------------------------------------
# PackageVersion tests
# ---------------------------------------------------------------------------


class TestPackageVersion:
    """Tests for the PackageVersion domain model."""

    def test_minimal_construction(self) -> None:
        pv = PackageVersion(
            package_id="python3",
            version="3.11.2-1",
            source="udd",
        )
        assert pv.package_id == "python3"
        assert pv.version == "3.11.2-1"
        assert pv.source == "udd"
        assert pv.observed_at.tzinfo is not None

    def test_full_construction(self) -> None:
        now = datetime.now(timezone.utc)
        pv = PackageVersion(
            package_id="libc6:amd64",
            version="2.31-13+deb12u5",
            distribution="bookworm",
            architecture="amd64",
            component="main",
            priority="required",
            observed_at=now,
            source="udd",
        )
        assert pv.distribution == "bookworm"
        assert pv.component == "main"
        assert pv.priority == "required"

    def test_version_validation(self) -> None:
        # Valid versions
        PackageVersion(package_id="p", version="1.0-1", source="udd")
        PackageVersion(package_id="p", version="1:2.31-13", source="udd")
        PackageVersion(package_id="p", version="0.0~alpha1-1", source="udd")
        PackageVersion(package_id="p", version="1.0", source="udd")

    def test_invalid_version_rejected(self) -> None:
        with pytest.raises(ValidationError):
            PackageVersion(package_id="p", version="", source="udd")
        with pytest.raises(ValidationError):
            PackageVersion(package_id="p", version="not-a-version", source="udd")
        with pytest.raises(ValidationError):
            PackageVersion(package_id="p", version="-1.0", source="udd")

    def test_utc_normalization(self) -> None:
        naive = datetime(2024, 1, 1, 12, 0, 0)
        pv = PackageVersion(
            package_id="p",
            version="1.0-1",
            source="udd",
            observed_at=naive,
        )
        assert pv.observed_at.tzinfo == timezone.utc

    def test_hashable(self) -> None:
        now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        pv1 = PackageVersion(
            package_id="p", version="1.0-1", source="s", observed_at=now
        )
        pv2 = PackageVersion(
            package_id="p", version="1.0-1", source="s", observed_at=now
        )
        assert hash(pv1) == hash(pv2)
        assert pv1 == pv2


# ---------------------------------------------------------------------------
# Release tests
# ---------------------------------------------------------------------------


class TestRelease:
    """Tests for the Release domain model."""

    def test_minimal_construction(self) -> None:
        rel = Release(release_id="stable", name="stable")
        assert rel.release_id == "stable"
        assert rel.name == "stable"
        assert rel.codename is None
        assert rel.status is None

    def test_full_construction(self) -> None:
        rel = Release(
            release_id="bookworm",
            name="stable",
            codename="bookworm",
            status="stable",
            version="12.0",
            description="Debian 12.0 (bookworm)",
        )
        assert rel.codename == "bookworm"
        assert rel.status == "stable"
        assert rel.version == "12.0"

    def test_immutability(self) -> None:
        rel = Release(release_id="stable", name="stable")
        with pytest.raises(ValidationError):
            rel.name = "unstable"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Migration tests
# ---------------------------------------------------------------------------


class TestMigration:
    """Tests for the Migration domain model (minimal R1)."""

    def test_minimal_construction(self) -> None:
        mig = Migration(
            migration_id="mig-001",
            source="unstable",
            target="testing",
            package="python3",
            version="3.11.2-1",
        )
        assert mig.migration_id == "mig-001"
        assert mig.source == "unstable"
        assert mig.target == "testing"
        assert mig.status == MigrationStatus.UNKNOWN

    def test_full_construction(self) -> None:
        now = datetime.now(timezone.utc)
        mig = Migration(
            migration_id="mig-002",
            source="sid",
            target="trixie",
            package="nginx",
            version="1.24.0-1",
            architecture="amd64",
            status=MigrationStatus.PENDING,
            observed_at=now,
        )
        assert mig.architecture == "amd64"
        assert mig.status == MigrationStatus.PENDING

    def test_invalid_version_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Migration(
                migration_id="mig-003",
                source="sid",
                target="trixie",
                package="bad",
                version="invalid",
            )

    def test_immutability(self) -> None:
        mig = Migration(
            migration_id="mig-001",
            source="sid",
            target="trixie",
            package="test",
            version="1.0-1",
        )
        with pytest.raises(ValidationError):
            mig.source = "unstable"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Observation tests
# ---------------------------------------------------------------------------


class TestObservation:
    """Tests for the Observation metadata model."""

    def test_minimal_construction(self) -> None:
        now = datetime.now(timezone.utc)
        obs = Observation(
            observation_id="obs-001",
            entity_id="pkg:python3",
            entity_type="package",
            source="udd",
        )
        assert obs.observation_id == "obs-001"
        assert obs.entity_id == "pkg:python3"
        assert obs.schema_version == 1

    def test_effective_before_observed(self) -> None:
        now = datetime.now(timezone.utc)
        yesterday = now - timedelta(days=1)
        obs = Observation(
            observation_id="obs-002",
            entity_id="pkg:test",
            entity_type="package",
            source="udd",
            observed_at=now,
            effective_at=yesterday,
        )
        assert obs.effective_at < obs.observed_at

    def test_effective_after_observed_rejected(self) -> None:
        now = datetime.now(timezone.utc)
        tomorrow = now + timedelta(days=1)
        with pytest.raises(ValidationError):
            Observation(
                observation_id="obs-003",
                entity_id="pkg:test",
                entity_type="package",
                source="udd",
                observed_at=now,
                effective_at=tomorrow,
            )

    def test_utc_normalization(self) -> None:
        naive = datetime(2024, 1, 1, 12, 0, 0)
        obs = Observation(
            observation_id="obs-004",
            entity_id="pkg:test",
            entity_type="package",
            source="udd",
            observed_at=naive,
        )
        assert obs.observed_at.tzinfo == timezone.utc

    def test_hashable(self) -> None:
        now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        obs1 = Observation(
            observation_id="obs-001",
            entity_id="pkg:a",
            entity_type="package",
            source="udd",
            observed_at=now,
            effective_at=now,
        )
        obs2 = Observation(
            observation_id="obs-001",
            entity_id="pkg:a",
            entity_type="package",
            source="udd",
            observed_at=now,
            effective_at=now,
        )
        assert hash(obs1) == hash(obs2)
        assert obs1 == obs2


class TestPackageVersionExtendedFields:
    """PackageVersion carries the full package-version fact fields."""

    def test_full_construction_with_extended_fields(self) -> None:
        now = datetime.now(timezone.utc)
        pv = PackageVersion(
            package_id="librte-dpdk:amd64",
            source_package="dpdk",
            version="25.11.3-1",
            distribution="debian",
            release="bullseye-security",
            architecture="amd64",
            component="main",
            section="libs",
            priority="optional",
            observed_at=now,
            source="udd",
        )
        assert pv.source_package == "dpdk"
        assert pv.release == "bullseye-security"
        assert pv.section == "libs"

    def test_extended_fields_default_to_none(self) -> None:
        pv = PackageVersion(
            package_id="python3",
            version="3.11.2-1",
            source="udd",
        )
        assert pv.source_package is None
        assert pv.release is None
        assert pv.section is None


class TestPackageVersionSuiteField:
    """PackageVersion carries the suite role field."""

    def test_full_construction_with_suite(self) -> None:
        pv = PackageVersion(
            package_id="vim:amd64",
            version="2:9.0-1",
            distribution="debian",
            release="bookworm",
            suite="stable",
            architecture="amd64",
            source="debian.mirror",
        )
        assert pv.suite == "stable"

    def test_suite_defaults_to_none(self) -> None:
        pv = PackageVersion(
            package_id="vim:amd64",
            version="2:9.0-1",
            source="udd",
        )
        assert pv.suite is None
