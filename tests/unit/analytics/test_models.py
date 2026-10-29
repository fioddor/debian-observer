"""Unit tests for analytics models."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from debian_observer.analytics.models import ExecutionRecord


def test_execution_record_basic_instantiation():
    """ExecutionRecord requires run_id, run_at, execution_type, source_name."""
    record = ExecutionRecord(
        run_id="run-001",
        run_at=datetime(2026, 10, 29, 10, 0, 0, tzinfo=timezone.utc),
        execution_type="ingestion",
        source_name="udd",
    )
    assert record.run_id == "run-001"
    assert record.execution_type == "ingestion"
    assert record.source_name == "udd"
    assert record.error_count == 0


def test_execution_record_all_fields():
    """ExecutionRecord accepts all schema fields."""
    record = ExecutionRecord(
        run_id="run-002",
        run_at=datetime(2026, 10, 29, 10, 0, 0, tzinfo=timezone.utc),
        execution_type="ingestion",
        source_name="benchmark",
        records_requested=1000,
        records_processed=1000,
        events_produced=500,
        duration_seconds=1.5,
        throughput_rps=666.7,
        raw_bytes=10240,
        parquet_bytes=4096,
        raw_path="data/raw/benchmark.csv.xz",
        parquet_path="data/parquet/benchmark.parquet",
        checksum="abc123",
        error_count=0,
        error_message=None,
        host_info='{"hostname": "test", "cpu_count": 4}',
        metadata='{"records_requested": 1000}',
    )
    assert record.records_processed == 1000
    assert record.raw_bytes == 10240
    assert record.error_count == 0


def test_execution_record_defaults():
    """Optional fields default to None; error_count defaults to 0."""
    record = ExecutionRecord(
        run_id="run-003",
        run_at=datetime(2026, 10, 29, 10, 0, 0, tzinfo=timezone.utc),
        execution_type="query",
        source_name="mirror",
    )
    assert record.records_requested is None
    assert record.records_processed is None
    assert record.events_produced is None
    assert record.duration_seconds is None
    assert record.throughput_rps is None
    assert record.raw_bytes is None
    assert record.parquet_bytes is None
    assert record.raw_path is None
    assert record.parquet_path is None
    assert record.checksum is None
    assert record.error_count == 0
    assert record.error_message is None
    assert record.host_info is None
    assert record.metadata is None


def test_execution_record_invalid_type_rejected():
    """execution_type must be a known value (validated by Literal)."""
    with pytest.raises(ValueError):
        ExecutionRecord(
            run_id="run-004",
            run_at=datetime(2026, 10, 29, 10, 0, 0, tzinfo=timezone.utc),
            execution_type="unknown",
            source_name="udd",
        )


def test_execution_record_to_dict():
    """to_dict serializes timestamps as ISO 8601 strings."""
    run_at = datetime(2026, 10, 29, 10, 0, 0, tzinfo=timezone.utc)
    record = ExecutionRecord(
        run_id="run-005",
        run_at=run_at,
        execution_type="ingestion",
        source_name="udd",
    )
    d = record.to_dict()
    assert d["run_id"] == "run-005"
    assert d["run_at"] == "2026-10-29T10:00:00+00:00"
    assert d["error_count"] == 0
