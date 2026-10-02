"""CLI command: ingest.

Acquire and ingest source data, producing raw artifacts and domain events.
"""

from __future__ import annotations

from pathlib import Path

import click

from debian_observer.ingestion.acquisition import AcquisitionService, SourcesDebianOrgDataSource
from debian_observer.ingestion.pipeline import IngestionPipeline


@click.command(name="ingest")
@click.option(
    "--source",
    required=True,
    type=click.Choice(["sources"]),
    help="Data source to ingest from.",
)
@click.option("--url", required=True, help="URL to fetch data from.")
@click.option("--raw-dir", default="data/raw", help="Directory for raw artifacts (default: data/raw).")
@click.option("--parquet-dir", default=None, help="Optional directory for Parquet output.")
@click.pass_context
def ingest(
    ctx: click.Context,
    source: str,
    url: str,
    raw_dir: str,
    parquet_dir: str | None,
) -> None:
    """Acquire source data and run the ingestion pipeline: raw
    artifact, dataset manifest, provenance record and domain events.

    Implemented data sources: `sources` (sources.debian.org API;
    `--url` is the package name) plus the synthetic sources of
    debian_observer.ingestion.acquisition (in-memory, mock) — synthetic
    rows for tests, not real Debian data).
    """
    acquisition = AcquisitionService()

    if source == "sources":
        data_source = SourcesDebianOrgDataSource(package_name=url)
    else:  # pragma: no cover — click.Choice rejects anything else first
        raise click.BadParameter(
            f"unknown source {source!r}; valid: sources",
            param_hint="--source",
        )

    pipeline = IngestionPipeline(
        raw_dir=Path(raw_dir),
        parquet_dir=Path(parquet_dir) if parquet_dir else None,
    )
    result = pipeline.run(data_source)

    click.echo(f"Ingested {result.record_count} records")
    click.echo(f"Run ID: {result.ingestion_run_id}")
    click.echo(f"Checksum: {result.checksum}")
    click.echo(f"Events produced: {len(result.events)}")
    if result.parquet_path:
        click.echo(f"Parquet written: {result.parquet_path}")
