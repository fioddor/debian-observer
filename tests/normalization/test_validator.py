"""Tests for normalization/validator module."""

from __future__ import annotations

from debian_observer.normalization.validator import UDDValidator


class TestUDDValidator:
    def test_valid_record(self):
        validator = UDDValidator()
        record = {
            "package": "vim",
            "version": "1.0",
            "architecture": "amd64",
        }
        assert validator.is_valid(record) is True

    def test_missing_version(self):
        validator = UDDValidator()
        record = {
            "package": "vim",
            "architecture": "amd64",
        }
        assert validator.is_valid(record) is False

    def test_missing_package_and_source(self):
        validator = UDDValidator()
        record = {
            "version": "1.0",
            "architecture": "amd64",
        }
        assert validator.is_valid(record) is False

    def test_source_instead_of_package(self):
        validator = UDDValidator()
        record = {
            "source": "vim",
            "version": "1.0",
        }
        assert validator.is_valid(record) is True

    def test_non_dict_record(self):
        validator = UDDValidator()
        assert validator.is_valid("not a dict") is False
        assert validator.is_valid(None) is False

    def test_why_invalid_missing_version(self):
        validator = UDDValidator()
        reason = validator.why_invalid({"package": "vim"})
        assert "Missing required fields" in reason

    def test_why_invalid_missing_package(self):
        validator = UDDValidator()
        reason = validator.why_invalid({"version": "1.0"})
        assert "package identifier" in reason

    def test_why_invalid_none(self):
        validator = UDDValidator()
        assert validator.why_invalid({"package": "vim", "version": "1.0"}) is None

    def test_validate_all_split(self):
        validator = UDDValidator()
        records = [
            {"package": "vim", "version": "1.0"},
            {"package": "emacs", "version": "2.0"},
            {"package": "nano"},
            {"version": "3.0"},
        ]
        valid, invalid = validator.validate_all(records)
        assert len(valid) == 2
        assert len(invalid) == 2
