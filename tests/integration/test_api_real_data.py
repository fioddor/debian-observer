"""End-to-end: ingest → persist → serve via API.

Verifies that events produced by ingestion, persisted to Parquet,
can be loaded into the FastAPI service and queried via HTTP.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.modules["psycopg2"] = MagicMock()

from fastapi.testclient import TestClient

from debian_observer.api.app import create_app
from debian_observer.api.loader import EventLoader
from debian_observer.api.store import InMemoryStore
from debian_observer.ingestion.acquisition import UDDDataSource
from debian_observer.ingestion.pipeline import IngestionPipeline
from debian_observer.storage.handlers import PersistingHandler


def test_api_serves_persisted_events() -> None:
    """Full flow: UDD ingest → Parquet → load into API → query endpoints."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("hello", "2.10-3", "debian", "sid", "amd64"),
        ("world", "1.0", "debian", "trixie", "arm64"),
        ("foo", "3.0-1", "debian", "bookworm", "all"),
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

        # Persist events
        persist = PersistingHandler(
            output_dir=Path(tmpdir) / "parquet",
            flush_threshold=100,
        )
        for event in result.events:
            persist(event)
        persist.flush()

        # Load into API store
        store = InMemoryStore()
        loader = EventLoader(store)
        count = loader.load_from_parquet(Path(tmpdir) / "parquet")
        assert count == 3

        # Start API and query
        app = create_app(store=store)
        client = TestClient(app)

        # Health
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

        # Package lookup (derived from events)
        resp = client.get("/packages/hello")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "hello"
        assert data["architecture"] == "amd64"

        # Versions
        resp = client.get("/packages/hello/versions")
        assert resp.status_code == 200
        versions = resp.json()
        assert len(versions) == 1
        assert versions[0]["version"] == "2.10-3"

        # Event lookup
        all_events = list(store.events.keys())
        assert len(all_events) > 0
        resp = client.get(f"/events/{all_events[0]}")
        assert resp.status_code == 200
        event_data = resp.json()
        assert event_data["event_type"] == "PackageVersionObserved"
