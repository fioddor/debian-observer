"""Normalization: Normalizer.

Orchestrates validation → transformation → event generation
in a single callable interface.

Usage::

    normalizer = Normalizer(source_name="udd", ingestion_run_id="run-001")
    events, invalid = normalizer.normalize_records(records)
"""

from __future__ import annotations

from typing import Any

from debian_observer.events import EventEnvelope
from debian_observer.normalization.events import EventFactory
from debian_observer.normalization.transformer import PackageVersionTransformer
from debian_observer.normalization.validator import UDDValidator


class Normalizer:
    """End-to-end normalization for a batch of raw records.

    The normalizer validates each record, transforms the valid ones
    into canonical domain models, and emits domain events.

    Invalid records are collected into a separate list so callers can
    inspect, log, or quarantine them.
    """

    def __init__(self, source_name: str, ingestion_run_id: str) -> None:
        """Initialize the normalizer.

        Args:
            source_name: Identifier of the data source.
            ingestion_run_id: Run identifier for provenance.
        """
        self.source_name = source_name
        self.ingestion_run_id = ingestion_run_id
        self._validator = UDDValidator()
        self._transformer = PackageVersionTransformer(source_name)
        self._events = EventFactory(source_name, ingestion_run_id)

    def normalize_records(
        self,
        records: list[dict[str, Any]],
    ) -> tuple[list[EventEnvelope], list[dict[str, Any]]]:
        """Normalize a batch of raw records.

        Args:
            records: Raw dicts from a data source.

        Returns:
            Tuple of (events, invalid_records).
        """
        valid, invalid = self._validator.validate_all(records)
        versions = self._transformer.transform_all(valid)
        events = self._events.from_package_versions(versions)
        return events, invalid

    def normalize(self, record: dict[str, Any]) -> EventEnvelope | None:
        """Normalize a single record.

        Args:
            record: A single raw dict.

        Returns:
            An EventEnvelope if valid, or None.
        """
        if not self._validator.is_valid(record):
            return None
        version = self._transformer.transform(record)
        return self._events.from_package_version(version)
