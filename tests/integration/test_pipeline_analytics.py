"""Integration test: IngestionPipeline.run() records analytics row."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from debian_observer.analytics.store import ExecutionStore
from debian_observer.ingestion.pipeline import IngestionPipeline


def test_pipeline_creates_analytics_row():
    """After pipeline.run(), an execution_runs row is queryable."""
    with tempfile.TemporaryDirectory() as tmpdir:
        raw_dir = Path(tmpdir) / "raw"
        raw_dir.mkdir()
        db_path = raw_dir.parent / "execution_runs.duckdb"

        # Synthetic benchmark-like source
        source = MagicMock()
        source.name = "benchmark"
        source.url = "file:///dev/null"
        synthetic = [
            {"package": f"pkg_{i}", "version": f"1.0-{i}", "architecture": "amd64"}
            for i in range(10)
        ]
        source.retrieve = lambda: synthetic
        source.compute_checksum.return_value = "abc123"
        source.record_count.return_value = 10

        pipeline = IngestionPipeline(raw_dir=raw_dir)
        result = pipeline.run(source, ingestion_run_id="benchmark-run-001")

        # The DB should have been created and contain a row
        assert db_path.exists()
        store = ExecutionStore(str(db_path))
        query_result = store.query(
            f"SELECT * FROM execution_runs WHERE run_id = '{result.ingestion_run_id}'"
        )
        row = query_result.fetchone()
        assert row is not None
        assert row[0] == result.ingestion_run_id
        assert row[2] == "ingestion"
        assert row[3] == "benchmark"
        assert row[5] == 10  # records_processed


def test_pipeline_analytics_includes_metrics():
    """Pipeline analytics row includes duration and throughput."""
    with tempfile.TemporaryDirectory() as tmpdir:
        raw_dir = Path(tmpdir) / "raw"
        raw_dir.mkdir()
        db_path = raw_dir.parent / "execution_runs.duckdb"

        source = MagicMock()
        source.name = "benchmark"
        source.url = "file:///dev/null"
        synthetic = [{"package": f"pkg_{i}", "version": f"1.0-{i}", "architecture": "amd64"} for i in range(5)]
        source.retrieve = lambda: synthetic
        source.compute_checksum.return_value = "def456"
        source.record_count.return_value = 5

        pipeline = IngestionPipeline(raw_dir=raw_dir)
        pipeline.run(source, ingestion_run_id="benchmark-run-002")

        store = ExecutionStore(str(db_path))
        result = store.query(
            "SELECT duration_seconds, throughput_rps FROM execution_runs WHERE run_id = 'benchmark-run-002'"
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] is not None  # duration_seconds
        assert row[0] > 0
        assert row[1] is not None  # throughput_rps
        assert row[1] > 0
