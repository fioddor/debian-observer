"""Unit tests for MicroReporter output format."""

from __future__ import annotations

from datetime import datetime, timezone

from debian_observer.analytics.models import ExecutionRecord
from debian_observer.analytics.report import MicroReporter


def test_print_report_shows_run_id():
    """MicroReporter prints run_id in the output."""
    import io
    import sys

    buf = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = buf

    reporter = MicroReporter()
    record = ExecutionRecord(
        run_id="run-123",
        run_at=datetime(2026, 10, 29, 10, 0, 0, tzinfo=timezone.utc),
        execution_type="ingestion",
        source_name="benchmark",
        records_processed=1000,
        events_produced=500,
        duration_seconds=2.5,
        throughput_rps=400.0,
    )
    reporter.print_report(record)

    sys.stdout = old_stdout
    output = buf.getvalue()

    assert "run-123" in output
    assert "benchmark" in output
    assert "1000" in output
    assert "500" in output


def test_print_report_includes_rate():
    """MicroReporter includes throughput rate when available."""
    import io
    import sys

    buf = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = buf

    reporter = MicroReporter()
    record = ExecutionRecord(
        run_id="run-fast",
        run_at=datetime(2026, 10, 29, 10, 0, 0, tzinfo=timezone.utc),
        execution_type="ingestion",
        source_name="udd",
        records_processed=10000,
        events_produced=2000,
        duration_seconds=1.0,
        throughput_rps=10000.0,
    )
    reporter.print_report(record)

    sys.stdout = old_stdout
    output = buf.getvalue()

    assert "10000" in output or "10000.0" in output


def test_print_report_no_records():
    """MicroReporter handles records_processed=None gracefully."""
    import io
    import sys

    buf = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = buf

    reporter = MicroReporter()
    record = ExecutionRecord(
        run_id="run-empty",
        run_at=datetime(2026, 10, 29, 10, 0, 0, tzinfo=timezone.utc),
        execution_type="query",
        source_name="mirror",
    )
    reporter.print_report(record)

    sys.stdout = old_stdout
    output = buf.getvalue()

    assert "run-empty" in output
    assert "mirror" in output
    assert "query" in output
