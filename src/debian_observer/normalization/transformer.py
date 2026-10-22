"""Normalization: Transformer.

Converts validated source records into canonical domain objects.

R1 supports UDD rows → PackageVersion only. Future releases will
add ReleaseObserved, MigrationObserved, etc.
"""

from __future__ import annotations

import warnings

from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from debian_observer.domain.models import PackageVersion


class PackageVersionTransformer:
    """Transforms raw UDD records into canonical PackageVersion models.

    Usage::

        transformer = PackageVersionTransformer(source_name="udd")
        version = transformer.transform(record)
    """

    def __init__(self, source_name: str) -> None:
        """Initialize transformer.

        Args:
            source_name: Identifier of the data source (e.g. "udd").
        """
        self.source_name = source_name

    def transform(self, record: dict[str, Any]) -> PackageVersion:
        """Transform a single valid UDD record into PackageVersion.

        Args:
            record: A validated UDD row dict.

        Returns:
            A canonical PackageVersion instance.
        """
        pkg_name = record.get("package") or record.get("source", "unknown")
        arch = record.get("architecture", "all")
        package_id = f"{pkg_name}:{arch}"

        return PackageVersion(
            package_id=package_id,
            source_package=record.get("source_package"),
            version=record.get("version", "0.0-1"),
            distribution=record.get("distribution"),
            release=record.get("release"),
            suite=record.get("suite"),
            architecture=record.get("architecture"),
            component=record.get("component"),
            section=record.get("section"),
            priority=record.get("priority"),
            source=self.source_name,
            observed_at=datetime.now(timezone.utc),
        )

    def transform_all(
        self,
        records: list[dict[str, Any]],
    ) -> list[PackageVersion]:
        """Transform a batch of records.

        A record that fails canonical transformation is not a crash:
        it is skipped with ONE warning per batch naming the cause
        (2026-10-22) — unexpected data yields messages, never
        tracebacks.

        Returns:
            List of PackageVersion instances.
        """
        versions: list[PackageVersion] = []
        skipped = 0
        first_cause: str | None = None
        for index, record in enumerate(records):
            try:
                versions.append(self.transform(record))
            except ValidationError as exc:
                skipped += 1
                if first_cause is None:
                    first_cause = (
                        f"record {index} {record!r}: "
                        f"{_format_validation_error(exc)}"
                    )
        if skipped:
            warnings.warn(
                f"transformer skipped {skipped} record(s); "
                f"first cause: {first_cause}",
                stacklevel=2,
            )
        return versions


def _format_validation_error(exc: ValidationError) -> str:
    """Render a pydantic ValidationError as a readable cause."""
    parts: list[str] = []
    for error in exc.errors():
        loc = ".".join(str(piece) for piece in error["loc"])
        parts.append(f"{loc}: {error['msg']} (input: {error['input']!r})")
    return "; ".join(parts)
