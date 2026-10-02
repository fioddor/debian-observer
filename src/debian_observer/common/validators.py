"""Pydantic v2-compatible validators for Debian-specific formats.

All validators in this module are designed as Pydantic v2 field validators
(or reusable functions that can be called from them) and raise
ValueError with descriptive messages on invalid input.
"""

from __future__ import annotations

import re
from typing import Any

# ---------------------------------------------------------------------------
# Debian version format (per Debian Policy Manual section 5.6.12)
#
#   [epoch:]upstream_version[-debian_revision]
#
# Components:
#   epoch            — non-negative integer, optional, followed by ':'
#   upstream_version — must start with a digit; may contain alphanumerics,
#                      '.', '+', '-', '~', ':'
#   debian_revision  — optional, follows '-'; same character rules
#
# This is an approximate regex suitable for R1 validation.
# It does not reject every theoretically invalid version, but catches
# common malformations and empty strings.
# ---------------------------------------------------------------------------

_DEBIAN_VERSION_RE = re.compile(
    r"^"
    r"(?:(?P<epoch>\d+):)?"  # optional epoch
    r"(?P<upstream>[0-9][A-Za-z0-9.\-+~:]*)"  # upstream must start with digit
    r"(?:-(?P<revision>[A-Za-z0-9.\-+~:]+))?"  # optional debian revision
    r"$"
)

# Maximum sensible length to prevent abuse
_MAX_VERSION_LENGTH = 512


def validate_debian_version(value: str) -> str:
    """Validate that *value* conforms to Debian package version syntax.

    Args:
        value: The version string to validate.

    Returns:
        The original version string (unchanged) if valid.

    Raises:
        ValueError: If the version string is empty, too long, or does not
            match Debian version syntax.
    """
    if not value or not isinstance(value, str):
        raise ValueError("Debian version must be a non-empty string")

    if len(value) > _MAX_VERSION_LENGTH:
        raise ValueError(
            f"Debian version exceeds maximum length ({_MAX_VERSION_LENGTH}): "
            f"{len(value)} characters"
        )

    match = _DEBIAN_VERSION_RE.match(value)
    if not match:
        raise ValueError(
            f"Invalid Debian version format: {value!r}. "
            "Expected [epoch:]upstream_version[-debian_revision] "
            "where upstream_version starts with a digit."
        )

    return value


# ---------------------------------------------------------------------------
# Reusable Pydantic v2 validator factory
# ---------------------------------------------------------------------------


def make_version_validator(field_name: str = "version") -> Any:
    """Return a Pydantic v2 field validator for Debian version strings.

    Usage inside a Pydantic model::

        from pydantic import field_validator
        from debian_observer.common.validators import make_version_validator

        _validate_version = field_validator("version", mode="before")(
            make_version_validator("version")
        )

    Args:
        field_name: The name of the field being validated (for error messages).

    Returns:
        A callable suitable for Pydantic ``field_validator(..., mode="before")``.
    """

    def _validator(cls: Any, value: Any) -> Any:  # noqa: ARG001
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError(
                f"{field_name} must be a string, got {type(value).__name__}"
            )
        return validate_debian_version(value)

    return _validator


# ---------------------------------------------------------------------------
# Additional format validators
# ---------------------------------------------------------------------------

_DEBIAN_PACKAGE_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9+\-.]*$")
_MAX_PACKAGE_NAME_LENGTH = 128


def validate_package_name(value: str) -> str:
    """Validate a Debian package name.

    Args:
        value: The package name to validate.

    Returns:
        The original name if valid.

    Raises:
        ValueError: If the name is empty or contains invalid characters.
    """
    if not value or not isinstance(value, str):
        raise ValueError("Package name must be a non-empty string")

    if len(value) > _MAX_PACKAGE_NAME_LENGTH:
        raise ValueError(
            f"Package name exceeds maximum length ({_MAX_PACKAGE_NAME_LENGTH})"
        )

    if not _DEBIAN_PACKAGE_NAME_RE.match(value):
        raise ValueError(
            f"Invalid package name: {value!r}. "
            "Must start with alphanumeric and contain only lowercase letters, "
            "digits, plus signs, hyphens, or periods."
        )

    return value
