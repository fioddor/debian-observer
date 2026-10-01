"""Normalization: Validator.

Validates source-specific structures before canonical transformation.

R1 supports UDD (Ultimate Debian Database) rows. Future releases
will add support for other sources (snapshot, buildd, etc.).
"""

from __future__ import annotations

from typing import Any


class UDDValidator:
    """Validates incoming raw UDD records against expected fields.

    A valid UDD row for package version observation must contain at
    minimum a ``package`` (or ``source``) name and a ``version``.

    Usage::

        validator = UDDValidator()
        if validator.is_valid(record):
            ...

        invalid = validator.validate_all(records)
        # returns list of invalid records with reason
    """

    REQUIRED_FIELDS = {"version"}
    PACKAGE_FIELDS = {"package", "source"}

    def is_valid(self, record: dict[str, Any]) -> bool:
        """Check if a single record is valid for transformation.

        Args:
            record: A raw dict from a UDD query result.

        Returns:
            True if the record can be normalized.
        """
        if not isinstance(record, dict):
            return False
        if not self.REQUIRED_FIELDS.issubset(record.keys()):
            return False
        if not any(field in record for field in self.PACKAGE_FIELDS):
            return False
        return True

    def validate_all(
        self,
        records: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Split records into valid and invalid.

        Returns:
            Tuple of (valid_records, invalid_records).
        """
        valid: list[dict[str, Any]] = []
        invalid: list[dict[str, Any]] = []
        for record in records:
            if self.is_valid(record):
                valid.append(record)
            else:
                invalid.append(record)
        return valid, invalid

    def why_invalid(self, record: dict[str, Any]) -> str | None:
        """Return a human-readable reason why a record is invalid.

        Returns:
            Reason string, or None if the record is valid.
        """
        if not isinstance(record, dict):
            return "Record is not a dict"
        missing_required = self.REQUIRED_FIELDS - record.keys()
        if missing_required:
            return f"Missing required fields: {missing_required}"
        if not any(field in record for field in self.PACKAGE_FIELDS):
            return f"Missing package identifier field ({self.PACKAGE_FIELDS})"
        return None
