"""Normalization: Transformer.

Converts validated source records into canonical domain objects.

R1 supports UDD rows → PackageVersion only. Future releases will
add ReleaseObserved, MigrationObserved, etc.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

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
            version=record.get("version", "0.0-1"),
            distribution=record.get("distribution"),
            architecture=record.get("architecture"),
            component=record.get("component"),
            priority=record.get("priority"),
            source=self.source_name,
            observed_at=datetime.now(timezone.utc),
        )

    def transform_all(
        self,
        records: list[dict[str, Any]],
    ) -> list[PackageVersion]:
        """Transform a batch of records.

        Returns:
            List of PackageVersion instances.
        """
        return [self.transform(record) for record in records]
