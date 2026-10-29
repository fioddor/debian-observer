"""Micro-reporter: minimal stdout table for a single execution run.

The micro-report is intentionally compact—it always prints so that
operators see feedback after every pipeline execution without
needing to run a separate command.
"""

from __future__ import annotations

from debian_observer.analytics.models import ExecutionRecord


class MicroReporter:
    """Print a one-line or compact table summary of an execution run."""

    def print_report(self, record: ExecutionRecord) -> None:
        """Write a compact human-readable report to stdout."""
        parts = [
            f"run_id={record.run_id}",
            f"type={record.execution_type}",
            f"source={record.source_name}",
        ]
        if record.records_processed is not None:
            parts.append(f"records={record.records_processed}")
        if record.events_produced is not None:
            parts.append(f"events={record.events_produced}")
        if record.duration_seconds is not None:
            parts.append(f"duration={record.duration_seconds:.3f}s")
        if record.throughput_rps is not None:
            parts.append(f"rate={record.throughput_rps:.1f} r/s")
        if record.error_count:
            parts.append(f"errors={record.error_count}")

        print(" | ".join(parts))
