"""Storage layer.

Handles raw artifact preservation and analytical storage
(Parquet/DuckDB).

Usage::

    from debian_observer.storage import ParquetWriter, AnalyticalStorage

    writer = ParquetWriter(output_dir="data/parquet")
    writer.write_events(events)

    dataset = PackageVersionDatasetWriter(
        output_dir="data/normalized/package_versions"
    )
    dataset.write_events(events)

    analytics = AnalyticalStorage("data/parquet")
    result = analytics.query("SELECT * FROM events WHERE release = 'sid'")
"""

from __future__ import annotations

from debian_observer.storage.analytical import AnalyticalStorage
from debian_observer.storage.dataset import PackageVersionDatasetWriter
from debian_observer.storage.parquet import ParquetWriter

__all__ = [
    "ParquetWriter",
    "PackageVersionDatasetWriter",
    "AnalyticalStorage",
]
