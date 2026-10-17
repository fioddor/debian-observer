"""Tests for normalization/transformer module."""

from __future__ import annotations

from debian_observer.domain.models import PackageVersion
from debian_observer.normalization.transformer import PackageVersionTransformer


class TestPackageVersionTransformer:
    def test_transform_basic(self):
        transformer = PackageVersionTransformer(source_name="udd")
        record = {
            "package": "vim",
            "source_package": "vim",
            "version": "2:9.0-1",
            "distribution": "debian",
            "release": "sid",
            "suite": "unstable",
            "architecture": "amd64",
            "component": "main",
            "section": "editors",
            "priority": "optional",
        }
        version = transformer.transform(record)
        assert isinstance(version, PackageVersion)
        assert version.package_id == "vim:amd64"
        assert version.source_package == "vim"
        assert version.version == "2:9.0-1"
        assert version.distribution == "debian"
        assert version.release == "sid"
        assert version.suite == "unstable"
        assert version.architecture == "amd64"
        assert version.component == "main"
        assert version.section == "editors"
        assert version.priority == "optional"
        assert version.source == "udd"
        assert version.observed_at.tzinfo is not None

    def test_transform_uses_source_field(self):
        transformer = PackageVersionTransformer(source_name="udd")
        record = {
            "source": "apache2",
            "version": "2.4.57-1",
        }
        version = transformer.transform(record)
        assert version.package_id == "apache2:all"

    def test_transform_defaults(self):
        transformer = PackageVersionTransformer(source_name="udd")
        record = {
            "package": "minimal",
            "version": "1.0",
        }
        version = transformer.transform(record)
        assert version.package_id == "minimal:all"
        assert version.architecture is None
        assert version.distribution is None
        assert version.release is None
        assert version.suite is None
        assert version.component is None
        assert version.section is None
        assert version.source_package is None
        assert version.priority is None

    def test_transform_all(self):
        transformer = PackageVersionTransformer(source_name="udd")
        records = [
            {"package": "a", "version": "1"},
            {"package": "b", "version": "2"},
        ]
        versions = transformer.transform_all(records)
        assert len(versions) == 2
        assert versions[0].package_id == "a:all"
        assert versions[1].package_id == "b:all"
