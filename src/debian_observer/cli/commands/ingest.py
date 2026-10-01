"""CLI command: ingest.

Acquire and ingest source data, producing raw artifacts and domain events.
"""

from __future__ import annotations

from pathlib import Path

import click

from debian_observer.ingestion.acquisition import AcquisitionService, MockDebianDataSource
from debian_observer.ingestion.pipeline import IngestionPipeline


@click.command(name="ingest")
@click.option(
    "--source",
    required=True,
    type=click.Choice(["mock"]),
    help="Data source to ingest from.",
)
@click.option("--raw-dir", default="data/raw", help="Directory for raw artifacts (default: data/raw).")
@click.pass_context
def ingest(
    ctx: click.Context,
    source: str,
    raw_dir: str,
) -> None:
    """Acquire source data and run the ingestion pipeline: raw
    artifact, dataset manifest, provenance record and domain events.

    Implemented data source: `mock` (deterministic synthetic Debian
    rows for tests, not real Debian data).
    """
    acquisition = AcquisitionService()

    if source == "mock":
        data_source = MockDebianDataSource()
    else:  # pragma: no cover — click.Choice rejects anything else first
        raise click.BadParameter(
            f"unknown source {source!r}; valid: mock",
            param_hint="--source",
        )

    pipeline = IngestionPipeline(raw_dir=Path(raw_dir))
    result = pipeline.run(data_source)

    click.echo(f"Ingested {result.record_count} records")
    click.echo(f"Run ID: {result.ingestion_run_id}")
    click.echo(f"Checksum: {result.checksum}")
    click.echo(f"Events produced: {len(result.events)}")
