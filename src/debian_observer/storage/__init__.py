"""Storage layer.

Handles raw artifact preservation and analytical storage
(Parquet/DuckDB).

Usage::

    from debian_observer.storage import ParquetWriter, AnalyticalStorage

    writer = ParquetWriter(output_dir="data/parquet")
    writer.write_events(events)

    analytics = AnalyticalStorage(writer.dataset_dir)
    df = analytics.query("SELECT * FROM package_versions WHERE distribution = 'sid'")
"""

from __future__ import annotations

from debian_observer.storage.parquet import ParquetWriter
from debian_observer.storage.analytical import AnalyticalStorage

__all__ = [
    "ParquetWriter",
    "AnalyticalStorage",
]
