"""CLI command group: ingest.

Acquire source data and run the ingestion pipeline: raw artifact,
dataset manifest, provenance record and domain events.

One subcommand per implemented data source (ADR-012): `sources`
(sources.debian.org API), `mirror` (Packages.gz from a Debian
mirror), `udd` (UDD PostgreSQL mirror), `udddump` (UDD `pg_dump`
file download). Each subcommand's selector is a mandatory filter
flag — entities within a source are not auto-detected yet.

`udddump` is acquisition-only: the dump is downloaded and stored
byte-preserving with a manifest sidecar; the reading (gates +
eventization) lives in `normalize udddump` (ADR-012 amendment,
2026-10-23).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import click

from debian_observer.ingestion.acquisition import (
    DebianMirrorDataSource,
    SourcesDebianOrgDataSource,
    UDDDataSource,
    download_dump,
)
from debian_observer.ingestion.manifest import DatasetManifest
from debian_observer.ingestion.pipeline import IngestionPipeline
from debian_observer.ingestion.provenance import ProvenanceRecorder, RawArtifact
from debian_observer.ingestion.raw import sha256_raw


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
    events_dir: str | None,
) -> None:
    """Run the ingestion pipeline for a data source and echo the result."""
    pipeline = IngestionPipeline(
        raw_dir=Path(raw_dir),
        parquet_dir=Path(events_dir) if events_dir else None,
    )
    result = pipeline.run(data_source)

    click.echo(f"Ingested {result.record_count} records")
    click.echo(f"Run ID: {result.ingestion_run_id}")
    click.echo(f"Checksum: {result.checksum}")
    click.echo(f"Events produced: {len(result.events)}")
    if result.parquet_path:
        click.echo(f"Parquet written: {result.parquet_path}")


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
    "--events-dir", default=None, help="Optional directory for the event log (one Parquet file per run)."
)
def ingest_sources(
    package_name: str,
    raw_dir: str,
    events_dir: str | None,
) -> None:
    """Fetch a package's version history from sources.debian.org and ingest it."""
    _run_ingest(
        SourcesDebianOrgDataSource(package_name=package_name),
        raw_dir=raw_dir,
        events_dir=events_dir,
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
    "--events-dir", default=None, help="Optional directory for the event log (one Parquet file per run)."
)
def ingest_mirror(
    suite: str,
    component: str,
    architecture: str,
    limit: int | None,
    raw_dir: str,
    events_dir: str | None,
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
        events_dir=events_dir,
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
    "--events-dir", default=None, help="Optional directory for the event log (one Parquet file per run)."
)
def ingest_udd(
    table: str,
    limit: int | None,
    raw_dir: str,
    events_dir: str | None,
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
        events_dir=events_dir,
    )


@ingest.command(name="udddump")
@click.option(
    "--url",
    required=True,
    help="URL of the UDD pg_dump custom-format file to download.",
)
@click.option(
    "--raw-dir", default="data/raw", help="Directory for raw artifacts (default: data/raw)."
)
def ingest_udddump(
    url: str,
    raw_dir: str,
) -> None:
    """Download a UDD pg_dump file over HTTPS (`--url`) and store it
    byte-preserving as the raw artifact
    `udddump-<timestamp>-<run-id>.dump`.

    `record_count` is null until normalization: the gates and the
    eventization are `debian-observer normalize udddump <artifact>`,
    which re-eventizes the same raw with other filters without
    re-downloading.
    """
    raw_dir_path = Path(raw_dir)
    raw_dir_path.mkdir(parents=True, exist_ok=True)
    run_id = f"run-{uuid.uuid4().hex[:12]}"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    artifact_path = raw_dir_path / f"udddump-{timestamp}-{run_id}.dump"

    download_dump(url, artifact_path)

    checksum = sha256_raw(artifact_path)
    size_bytes = artifact_path.stat().st_size
    manifest = DatasetManifest(
        dataset_name=f"udddump-{run_id}",
        source="udddump",
        retrieved_at=datetime.now(timezone.utc),
        checksum=checksum,
        size_bytes=size_bytes,
        record_count=None,
        license=None,
    )
    manifest_path = artifact_path.with_suffix(".manifest.json")
    manifest_path.write_text(manifest.to_json(), encoding="utf-8")

    ProvenanceRecorder().record_artifact(
        RawArtifact(
            artifact_id=f"artifact-{run_id}",
            source="udddump",
            path=artifact_path,
            checksum=checksum,
            size_bytes=size_bytes,
        ),
        ingestion_run_id=run_id,
    )

    click.echo(f"Downloaded {url}")
    click.echo(f"Run ID: {run_id}")
    click.echo(f"Raw artifact: {artifact_path}")
    click.echo(f"Manifest: {manifest_path}")
    click.echo(f"Checksum: {checksum}")
    click.echo(f"Size: {size_bytes} bytes")
