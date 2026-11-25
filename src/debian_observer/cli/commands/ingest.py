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

from debian_observer.storage.parquet import (
    EVENTS_DIR_DEFAULT,
)

from debian_observer.ingestion.acquisition import (
    DebianMirrorDataSource,
    SourcesDebianOrgDataSource,
    UDDDataSource,
    download_dump,
)
from debian_observer.ingestion.manifest import DatasetManifest
from debian_observer.ingestion import britney
from debian_observer.ingestion.britney import (
    BACKOFF_BASE,
    FETCH_ATTEMPTS,
    HTTP_TIMEOUT_SECONDS,
    MIN_LIVE_POLL_INTERVAL,
    fetch_index,
    select_window,
)
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
    - `udddump` (UDD `pg_dump` file) downloads the dump (`--url`),
    - `britney` (the migration excuses; `--from`/`--to`/`--last` select an
      archive window from the listed index, `--live` follows the endpoint).

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
    "--events-dir", default=None, help="Optional directory for the event log (one Parquet file per run; the default used across commands is "+ EVENTS_DIR_DEFAULT + ")."
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
    "--events-dir", default=None, help="Optional directory for the event log (one Parquet file per run; the default used across commands is "+ EVENTS_DIR_DEFAULT + ")."
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
    "--events-dir", default=None, help="Optional directory for the event log (one Parquet file per run; the default used across commands is "+ EVENTS_DIR_DEFAULT + ")."
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


@ingest.command(name="britney")
@click.option(
    "--year",
    type=int,
    default=None,
    help="Archive year to ingest (default: the current UTC year).",
)
@click.option(
    "--from",
    "from_ts",
    default=None,
    help="Inclusive UTC lower bound of the window (ISO 8601; naive means UTC).",
)
@click.option(
    "--to",
    "to_ts",
    default=None,
    help="Inclusive UTC upper bound of the window (ISO 8601; naive means UTC).",
)
@click.option(
    "--last",
    type=int,
    default=None,
    help="The N most recent snapshots of the index (positive integer).",
)
@click.option(
    "--raw-dir", default="data/raw", help="Directory for raw artifacts (default: data/raw)."
)
@click.option(
    "--codename",
    default=None,
    help="The testing codename at generated-date (coordinates; optional, recorded in the manifest).",
)
@click.option(
    "--min-interval",
    type=float,
    default=None,
    help="Seconds between two source requests (default: at least 1 s; never 0).",
)
@click.option(
    "--timeout",
    type=float,
    default=HTTP_TIMEOUT_SECONDS,
    help="Seconds before one source request is given up as lost "
    "(default: 60 s, generous for one archive file).",
)
@click.option(
    "--attempts",
    type=int,
    default=FETCH_ATTEMPTS,
    help="Download attempts per listed snapshot before it is recorded as "
    "failed (default: 3).",
)
@click.option(
    "--backoff-base",
    type=float,
    default=BACKOFF_BASE,
    help="Base of the exponential backoff between download attempts, "
    "in seconds (default: 2.0).",
)
@click.option(
    "--live",
    is_flag=True,
    default=False,
    help="Follow the live endpoint instead of a fixed window: conditional requests, "
    "a poll no more often than every 15 min (with --from: backfill first, then follow).",
)
@click.option(
    "--poll-interval",
    type=float,
    default=MIN_LIVE_POLL_INTERVAL,
    help="Seconds between two live polls (default: 900 s, the poll floor; "
    "never more often than every 15 min; --live only).",
)
def ingest_britney(
    year: int | None,
    from_ts: str | None,
    to_ts: str | None,
    last: int | None,
    raw_dir: str,
    codename: str | None,
    min_interval: float | None,
    timeout: float,
    attempts: int,
    backoff_base: float,
    live: bool,
    poll_interval: float,
) -> None:
    """Ingest britney's migration excuses over an archive window.

    The truth of the archive is the file listing of
    release.debian.org/britney/update_excuses/<year>/ — file names are
    never computed. The window:

    \b
    - --from/--to: inclusive UTC bounds on the snapshot timestamp,
    - --last N: the N most recent snapshots of the index,
    - --timeout/--attempts/--backoff-base: the fetch perimeter — a
      request given up after --timeout s, retried --attempts times
      with a --backoff-base exponential backoff,
    - --live: follow the live endpoint (with --from: backfill, then
      follow); polls no more often than every 15 minutes
      (--poll-interval) with conditional requests.

    Each listed snapshot is fetched once, politely (one request at a
    time, at least 1 s apart, --min-interval), stored byte-preserving
    in the raw store (re-runs skip by SHA-256), and described in a
    window manifest with its gaps (periods with no published
    snapshot) and failures (listed files that could not be retrieved)
    as data. One execution record per run.

    Acquisition only: the parse and the eventization are
    `debian-observer normalize britney`.
    """
    from debian_observer.ingestion.britney import (
        MIN_REQUEST_INTERVAL,
        acquire_window,
        follow_live,
    )

    chosen_year = year or datetime.now(timezone.utc).year
    raw_dir_path = Path(raw_dir)
    raw_dir_path.mkdir(parents=True, exist_ok=True)
    index = fetch_index(chosen_year, raw_dir=raw_dir_path, timeout=timeout)
    window = select_window(
        index.refs,
        from_ts=from_ts,
        to_ts=to_ts,
        last=last,
    )
    click.echo(f"Index: {britney.index_url(chosen_year)}")
    if index.from_cache:
        click.echo(f"Index cache: {index.cache_path}")
    click.echo(f"Window: {len(window)} snapshots")
    interval = (
        min_interval if min_interval is not None else MIN_REQUEST_INTERVAL
    )
    if live and from_ts is None:
        # --live without --from follows the endpoint; no archive window.
        follow_live(
            raw_dir=raw_dir_path,
            min_interval=interval,
            codename=codename,
            poll_interval=poll_interval,
        )
        click.echo("Live following stopped")
        return
    result = acquire_window(
        window,
        year=chosen_year,
        raw_dir=raw_dir_path,
        min_interval=interval,
        codename=codename,
        timeout=timeout,
        attempts=attempts,
        backoff_base=backoff_base,
    )
    click.echo(f"Fetched: {len(result.fetched)}")
    click.echo(f"Skipped (SHA-256 in store): {len(result.skipped)}")
    click.echo(f"Failed: {len(result.failures)}")
    click.echo(f"Gaps: {len(result.manifest['gaps'])}")
    click.echo(f"Manifest: {result.manifest_path}")
    if live:
        # --live --from X: the backfill ran above; now follow.
        follow_live(
            raw_dir=raw_dir_path,
            min_interval=interval,
            codename=codename,
            poll_interval=poll_interval,
        )
        click.echo("Live following stopped")
