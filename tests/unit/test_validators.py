"""Unit tests for common/validators module."""

from __future__ import annotations

import pytest

from debian_observer.common.validators import (
    make_version_validator,
    validate_debian_version,
    validate_package_name,
)


class TestValidateDebianVersion:
    """Tests for validate_debian_version."""

    def test_valid_version(self) -> None:
        assert validate_debian_version("2.37-12") == "2.37-12"

    def test_valid_version_with_epoch(self) -> None:
        assert validate_debian_version("1:2.37-12") == "1:2.37-12"

    def test_valid_version_no_revision(self) -> None:
        assert validate_debian_version("2.37") == "2.37"

    def test_rejects_empty_string(self) -> None:
        with pytest.raises(ValueError, match="non-empty string"):
            validate_debian_version("")

    def test_rejects_none(self) -> None:
        with pytest.raises(ValueError, match="non-empty string"):
            validate_debian_version(None)  # type: ignore[arg-type]

    def test_rejects_too_long(self) -> None:
        long_version = "1." + "0" * 600
        with pytest.raises(ValueError, match="exceeds maximum length"):
            validate_debian_version(long_version)

    def test_rejects_invalid_format(self) -> None:
        with pytest.raises(ValueError, match="Invalid Debian version format"):
            validate_debian_version("abc")


class TestMakeVersionValidator:
    """Tests for make_version_validator factory."""

    def test_validator_passes_valid_version(self) -> None:
        validator = make_version_validator("version")
        assert validator(None, "2.37-12") == "2.37-12"

    def test_validator_allows_none(self) -> None:
        validator = make_version_validator("version")
        assert validator(None, None) is None

    def test_validator_rejects_non_string(self) -> None:
        validator = make_version_validator("version")
        with pytest.raises(ValueError, match="must be a string"):
            validator(None, 123)  # type: ignore[arg-type]

    def test_validator_rejects_invalid_version(self) -> None:
        validator = make_version_validator("version")
        with pytest.raises(ValueError, match="Invalid Debian version format"):
            validator(None, "!!!")


class TestValidatePackageName:
    """Tests for validate_package_name."""

    def test_valid_name(self) -> None:
        assert validate_package_name("libc6") == "libc6"

    def test_valid_name_with_plus(self) -> None:
        assert validate_package_name("libstdc++6") == "libstdc++6"

    def test_rejects_empty_string(self) -> None:
        with pytest.raises(ValueError, match="non-empty string"):
            validate_package_name("")

    def test_rejects_none(self) -> None:
        with pytest.raises(ValueError, match="non-empty string"):
            validate_package_name(None)  # type: ignore[arg-type]

    def test_rejects_too_long(self) -> None:
        long_name = "x" * 200
        with pytest.raises(ValueError, match="exceeds maximum length"):
            validate_package_name(long_name)

    def test_rejects_invalid_chars(self) -> None:
        with pytest.raises(ValueError, match="Invalid package name"):
            validate_package_name("BAD_NAME")

    def test_rejects_starting_with_hyphen(self) -> None:
        with pytest.raises(ValueError, match="Invalid package name"):
            validate_package_name("-bad")
