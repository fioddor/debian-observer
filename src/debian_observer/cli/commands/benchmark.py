"""CLI command: benchmark.

Run performance benchmarks for the ingestion pipeline.
"""

from __future__ import annotations

import time

import click

from debian_observer.ingestion.acquisition import AcquisitionService
from pathlib import Path
from debian_observer.ingestion.pipeline import IngestionPipeline
from unittest.mock import MagicMock


@click.command(name="benchmark")
@click.option("--records", default=1000, help="Number of synthetic records to generate (default: 1000).")
@click.option("--raw-dir", default="data/raw", help="Directory for raw artifacts (default: data/raw).")
def benchmark(records: int, raw_dir: str) -> None:
    """Run a quick benchmark of the ingestion pipeline."""
    # Generate synthetic records
    synthetic_records = [
        {"package": f"pkg_{i}", "version": f"1.0-{i}", "architecture": "amd64"}
        for i in range(records)
    ]

    # Create a mock data source with proper return values
    source = MagicMock()
    source.name = "benchmark"
    source.url = "file:///dev/null"
    source.retrieve = lambda: synthetic_records
    source.compute_checksum.return_value = "abc123"
    source.record_count.return_value = records

    pipeline = IngestionPipeline(raw_dir=Path(raw_dir))

    start = time.perf_counter()
    result = pipeline.run(source, ingestion_run_id="benchmark-run")
    duration = time.perf_counter() - start

    click.echo(f"Records: {result.record_count}")
    click.echo(f"Events: {len(result.events)}")
    click.echo(f"Duration: {duration:.3f}s")
    click.echo(f"Throughput: {result.record_count / duration:.1f} records/s")
