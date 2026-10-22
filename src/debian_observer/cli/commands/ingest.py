"""CLI command group: ingest.

Acquire source data and run the ingestion pipeline: raw artifact,
dataset manifest, provenance record and domain events.

One subcommand per implemented data source (ADR-012): `sources`
(sources.debian.org API), `mirror` (Packages.gz from a Debian
mirror), `udd` (UDD PostgreSQL mirror), `udddump` (UDD `pg_dump`
file). Each subcommand's selector is a mandatory filter flag —
entities within a source are not auto-detected yet.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import click

from debian_observer.ingestion.acquisition import (
    DebianMirrorDataSource,
    SourcesDebianOrgDataSource,
    UDDDataSource,
    UDDDumpDataSource,
    UDDDumpError,
    assert_supported_table_shape,
    list_dump_tables,
    peek_dump_copy_columns,
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
    - `udd` (UDD PostgreSQL mirror; `--table` is the table name),
    - `udddump` (UDD `pg_dump` file) downloads the dump (`--url`).

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


@ingest.command(name="udddump")
@click.option(
    "--file",
    "dump_file",
    required=True,
    help="Path to the UDD pg_dump custom-format file (host path; read by pg_restore on PATH).",
)
@click.option(
    "--table",
    required=True,
    help=(
        "UDD table to restore: a TABLE DATA entry of the dump "
        "(validated at startup; the error lists the restorable tables)."
    ),
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Max records to ingest; unset ingests the whole table.",
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
def ingest_udddump(
    dump_file: str,
    table: str,
    limit: int | None,
    raw_dir: str,
    parquet_dir: str | None,
    dataset_dir: str | None,
) -> None:
    """Restore one table of a UDD pg_dump file through pg_restore.

    Implementation-pinned values, surfaced so operators know what runs
    (and what is not implemented): pg_restore executes on the host
    (PATH dependency; install postgresql-client if absent); --file is
    the host path to the dump; rows stream from a data-only COPY block,
    so no database is created or restored (ADR-006) — no container and
    no live UDD instance are involved.

    Perimeter: today only packages-shaped tables are ingestible
    (required COPY columns: version + any of: package, source); more
    table shapes will land as new event types do. The startup gates
    fail fast — the TOC gate lists the dump's restorable tables, and
    the shape gate names the cause and the supported shapes.
    """
    if limit is None:
        _warn_limit_unset()
    # Startup gate (ADR-012 amendment, alpha): validate the table
    # against the dump's TOC before any restore work — fail fast
    # listing the restorable tables instead of crashing downstream.
    try:
        available = list_dump_tables(dump_file)
    except UDDDumpError as exc:
        raise click.ClickException(str(exc)) from exc
    if table not in available:
        raise click.ClickException(
            f"table {table!r} not in dump {dump_file!r}; "
            f"available tables: {', '.join(sorted(available))}"
        )
    # Perimeter gate (ADR-012, evening amendment): peek the table's
    # COPY header — one stdout line, no data restored — and match it
    # against the registered shapes before any ingest work.
    try:
        columns = peek_dump_copy_columns(dump_file, table=table)
        assert_supported_table_shape(columns, table=table)
    except UDDDumpError as exc:
        raise click.ClickException(str(exc)) from exc
    _run_ingest(
        UDDDumpDataSource(file=dump_file, table=table, limit=limit),
        raw_dir=raw_dir,
        parquet_dir=parquet_dir,
        dataset_dir=dataset_dir,
    )
