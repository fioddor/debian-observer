"""Normalization layer.

Converts source-specific structures into canonical domain
representations and emits domain events.

Usage::

    from debian_observer.normalization import Normalizer

    normalizer = Normalizer(source_name="udd", ingestion_run_id="run-001")
    events = normalizer.normalize_records(records)
"""

from __future__ import annotations

from debian_observer.normalization.validator import UDDValidator
from debian_observer.normalization.transformer import PackageVersionTransformer
from debian_observer.normalization.events import EventFactory
from debian_observer.normalization.normalizer import Normalizer

__all__ = [
    "UDDValidator",
    "PackageVersionTransformer",
    "EventFactory",
    "Normalizer",
]
