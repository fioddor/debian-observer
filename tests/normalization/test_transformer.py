"""Tests for normalization/transformer module."""

from __future__ import annotations

import warnings

import pytest

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


class TestTransformAllResilience:
    """The transformer never crashes on unexpected data (2026-10-22).

    A record that fails canonical transformation is skipped with a
    warning naming the cause — an error message, never a traceback.
    """

    @staticmethod
    def _good(package: str) -> dict:
        return {"package": package, "version": "1.0-1"}

    def test_transform_all_skips_canonical_failures_with_a_warning(self):
        transformer = PackageVersionTransformer(source_name="udd.dump")
        records = [
            self._good("one"),
            # non-empty, so it passes presence validation, but not
            # the canonical Debian-version format check
            {"package": "bad", "version": "unknown"},
            self._good("two"),
        ]
        with pytest.warns(UserWarning, match="skipped 1 record") as caught:
            versions = transformer.transform_all(records)
        assert [v.package_id for v in versions] == ["one:all", "two:all"]
        message = str(caught[0].message)
        assert "version" in message   # the cause field
        assert "unknown" in message  # the offending value
        assert "bad" in message      # the offending record

    def test_transform_all_warns_once_per_batch_not_per_record(self):
        transformer = PackageVersionTransformer(source_name="udd.dump")
        records = [
            {"package": f"p{i}", "version": "unknown"} for i in range(50)
        ]
        with pytest.warns(UserWarning, match="skipped 50 record") as caught:
            versions = transformer.transform_all(records)
        assert versions == []
        assert len(caught) == 1

    def test_transform_all_all_valid_emits_no_warning(self):
        transformer = PackageVersionTransformer(source_name="udd.dump")
        records = [self._good("a"), self._good("b")]
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            versions = transformer.transform_all(records)
        assert len(versions) == 2
        assert caught == []
