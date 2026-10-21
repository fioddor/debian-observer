"""CLI command group: ingest.

Acquire source data and run the ingestion pipeline: raw artifact,
dataset manifest, provenance record and domain events.

One subcommand per implemented data source (ADR-012): `sources`
(sources.debian.org API), `mirror` (Packages.gz from a Debian
mirror), `udd` (UDD PostgreSQL mirror). Each subcommand's selector
is a mandatory filter flag — elements are not auto-detected yet.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import click

from debian_observer.ingestion.acquisition import (
    DebianMirrorDataSource,
    SourcesDebianOrgDataSource,
    UDDDataSource,
)
from debian_observer.ingestion.pipeline import IngestionPipeline


@click.group(name="ingest")
def ingest() -> None:
    """Acquire source data: raw artifact and dataset manifest with
    provenance record.

    One sub command per implemented data source:

    
    - `sources` (sources.debian.org API; `--package` is the source package name),
    -  `mirror` (Packages.gz from a Debian mirror; `--suite` is the suite role, codename or pocket),
    - `udd` (UDD PostgreSQL mirror; `--table` is the table name).

    Behaviour:

    
    - Stores the acquired files byte-preserving with a manifest sidecar.
    - Acquisition only: the gates and eventization live in `normalize`.
    - Selectors are mandatory filter flags.

    Run `debian-observer ingest <source> --help` for a subcommand's options.
    """


def _run_ingest(
    data_source: Any,
    *,
    raw_dir: str,
    parquet_dir: str | None,
    dataset_dir: str | None,
) -> None:
    """Run the ingestion pipeline for a data source and echo the result."""
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


def _warn_limit_unset() -> None:
    """Mirror/udd ingest the whole source when --limit is unset."""
    click.echo("Warning: --limit unset; ingesting the whole source.", err=True)


@ingest.command(name="sources")
@click.option(
    "--package",
    "package_name",
    required=True,
    help="Source package name to fetch from sources.debian.org.",
)
@click.option(
    "--raw-dir", default="data/raw", help="Directory for raw artifacts (default: data/raw)."
)
@click.option(
    "--parquet-dir", default=None, help="Optional directory for Parquet output."
)
@click.option(
    "--dataset-dir",
    default=None,
    help="Optional directory for the normalized package_versions dataset.",
)
def ingest_sources(
    package_name: str,
    raw_dir: str,
    parquet_dir: str | None,
    dataset_dir: str | None,
) -> None:
    """Fetch a package's version history from sources.debian.org and ingest it."""
    _run_ingest(
        SourcesDebianOrgDataSource(package_name=package_name),
        raw_dir=raw_dir,
        parquet_dir=parquet_dir,
        dataset_dir=dataset_dir,
    )


@ingest.command(name="mirror")
@click.option(
    "--suite",
    required=True,
    help="Suite to fetch (suite role, codename or pocket).",
)
@click.option(
    "--component",
    default="main",
    help="Archive component (default: main).",
)
@click.option(
    "--architecture",
    default="amd64",
    help="Package architecture (default: amd64).",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Max records to ingest; unset ingests the whole source.",
)
@click.option(
    "--raw-dir", default="data/raw", help="Directory for raw artifacts (default: data/raw)."
)
@click.option(
    "--parquet-dir", default=None, help="Optional directory for Parquet output."
)
@click.option(
    "--dataset-dir",
    default=None,
    help="Optional directory for the normalized package_versions dataset.",
)
def ingest_mirror(
    suite: str,
    component: str,
    architecture: str,
    limit: int | None,
    raw_dir: str,
    parquet_dir: str | None,
    dataset_dir: str | None,
) -> None:
    """Fetch a suite's Packages.gz from a Debian mirror and ingest it."""
    if limit is None:
        _warn_limit_unset()
    _run_ingest(
        DebianMirrorDataSource(
            suite=suite,
            component=component,
            architecture=architecture,
            limit=limit,
        ),
        raw_dir=raw_dir,
        parquet_dir=parquet_dir,
        dataset_dir=dataset_dir,
    )


@ingest.command(name="udd")
@click.option(
    "--table",
    required=True,
    help="UDD table to query (e.g. packages, sources).",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Max records to ingest; unset ingests the whole source.",
)
@click.option(
    "--raw-dir", default="data/raw", help="Directory for raw artifacts (default: data/raw)."
)
@click.option(
    "--parquet-dir", default=None, help="Optional directory for Parquet output."
)
@click.option(
    "--dataset-dir",
    default=None,
    help="Optional directory for the normalized package_versions dataset.",
)
def ingest_udd(
    table: str,
    limit: int | None,
    raw_dir: str,
    parquet_dir: str | None,
    dataset_dir: str | None,
) -> None:
    """Query a table from a UDD PostgreSQL mirror and ingest it.

    Works with any UDD mirror: connection settings come from the
    UDD_HOST, UDD_PORT, UDD_DATABASE, UDD_USER and UDD_PASSWORD
    environment variables (defaults: the public mirror
    udd-mirror.debian.net, database `udd`, user `udd-mirror`).
    """
    if limit is None:
        _warn_limit_unset()
    _run_ingest(
        UDDDataSource(table=table, limit=limit),
        raw_dir=raw_dir,
        parquet_dir=parquet_dir,
        dataset_dir=dataset_dir,
    )