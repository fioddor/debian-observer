"""CLI command: ingest.

Acquire and ingest source data, producing raw artifacts and domain events.
"""

from __future__ import annotations

from pathlib import Path

import click

from debian_observer.ingestion.acquisition import (
    AcquisitionService,
    DebianMirrorDataSource,
    SourcesDebianOrgDataSource,
    UDDDataSource,
)
from debian_observer.ingestion.pipeline import IngestionPipeline


@click.command(name="ingest")
@click.option(
    "--source",
    required=True,
    type=click.Choice(["sources", "mirror", "udd"]),
    help="Data source to ingest from.",
)
@click.option("--url", required=True, help="URL or package/suite/table name.")
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Max records to ingest (mirror/udd); unset ingests the whole source.",
)
@click.option("--raw-dir", default="data/raw", help="Directory for raw artifacts (default: data/raw).")
@click.option(
    "--parquet-dir", default=None, help="Optional directory for Parquet output."
)
@click.option(
    "--dataset-dir",
    default=None,
    help="Optional directory for the normalized package_versions dataset.",
)
@click.pass_context
def ingest(
    ctx: click.Context,
    source: str,
    url: str,
    limit: int | None,
    raw_dir: str,
    parquet_dir: str | None,
    dataset_dir: str | None,
) -> None:
    """Acquire source data and run the ingestion pipeline: raw
    artifact, dataset manifest, provenance record and domain events.

    Implemented data sources: `sources` (sources.debian.org API;
    `--url` is the package name), `mirror` (Packages.gz from a Debian
    mirror; `--url` is the suite) and `udd` (UDD PostgreSQL mirror,
    udd-mirror.debian.net; `--url` is the table name); `--limit`
    caps records for mirror/udd; unset ingests the whole source
    with a warning on stderr. Synthetic sources (in-memory,
    mock) emit synthetic rows for tests, not real Debian data.
    """
    acquisition = AcquisitionService()

    if source == "sources":
        data_source = SourcesDebianOrgDataSource(package_name=url)
    elif source == "mirror":
        data_source = DebianMirrorDataSource(
            suite=url,
            component="main",
            architecture="amd64",
            limit=limit,
        )
    elif source == "udd":
        data_source = UDDDataSource(
            table=url,
            limit=limit,
        )
    else:  # pragma: no cover — click.Choice rejects anything else first
        raise click.BadParameter(
            f"unknown source {source!r}; valid: sources, mirror, udd",
            param_hint="--source",
        )

    if limit is None and source in ("mirror", "udd"):
        click.echo(
            "Warning: --limit unset; ingesting the whole source.",
            err=True,
        )

    pipeline = IngestionPipeline(
        raw_dir=Path(raw_dir),
        parquet_dir=Path(parquet_dir) if parquet_dir else None,
        dataset_dir=Path(dataset_dir) if dataset_dir else None,
    )
    result = pipeline.run(data_source)

    click.echo(f"Ingested {result.record_count} records")
    click.echo(f"Run ID: {result.ingestion_run_id}")
    click.echo(f"Checksum: {result.checksum}")
    click.echo(f"Events produced: {len(result.events)}")
    if result.parquet_path:
        click.echo(f"Parquet written: {result.parquet_path}")
    if result.dataset_path:
        click.echo(f"Dataset written: {result.dataset_path}")
