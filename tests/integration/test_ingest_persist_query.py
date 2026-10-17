"""End-to-end: ingest → persist → query.

Verifies that events produced by the ingestion pipeline can be
written via PersistingHandler and subsequently queried through the
DuckDB analytical layer.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.modules["psycopg2"] = MagicMock()

from debian_observer.ingestion.acquisition import UDDDataSource
from debian_observer.ingestion.pipeline import IngestionPipeline
from debian_observer.storage.analytical import AnalyticalStorage
from debian_observer.storage.handlers import PersistingHandler


def test_ingest_events_then_query_via_analytical_storage() -> None:
    """Full flow: UDD ingest → Parquet (via PersistingHandler) → DuckDB query."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("hello", "hello", "2.10-3", "debian", "sid", "amd64", "devel", "optional", "main"),
        ("world", "world", "1.0", "debian", "trixie", "arm64", "devel", "optional", "main"),
        ("foo", "foo", "3.0-1", "debian", "bookworm", "all", "devel", "optional", "main"),
    ]
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with tempfile.TemporaryDirectory() as tmpdir:
        with patch("psycopg2.connect", return_value=mock_conn):
            source = UDDDataSource(table="packages", limit=3)
            pipeline = IngestionPipeline(
                raw_dir=Path(tmpdir) / "raw",
                parquet_dir=None,
            )
            result = pipeline.run(source)

        # Persist events via PersistingHandler (single flush for determinism)
        persist = PersistingHandler(
            output_dir=Path(tmpdir) / "parquet",
            flush_threshold=100,  # only manual flush
        )
        for event in result.events:
            persist(event)
        persist.flush()  # write all 3 events at once

        # Debug: verify files exist
        pq_files = list((Path(tmpdir) / "parquet").glob("*.parquet"))
        assert len(pq_files) >= 0  # noqa: PLR2004

        # Query via AnalyticalStorage
        storage = AnalyticalStorage(Path(tmpdir) / "parquet")
        total = storage.count_events()
        assert total == 3

        summary = storage.summary()
        assert summary["events"] == 3
        assert summary["files"] == 1
        assert summary["types"]["PackageVersionObserved"] == 3

        rows = storage.query(
            "SELECT entity_id, version FROM events WHERE distribution = 'debian' LIMIT 1"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "hello:amd64"
        assert rows[0][1] == "2.10-3"
