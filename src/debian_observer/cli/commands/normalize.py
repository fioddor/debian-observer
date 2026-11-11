"""CLI command group: normalize.

Validate and transform raw artifacts into canonical domain events.

One subcommand per raw surface (ADR-012 amendment, 2026-10-23):
`artifacts` — pipeline `.csv.xz` artifacts; `udddump` — UDD
`pg_dump` download artifacts (the gates and the eventization).
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import click

from debian_observer.analytics.models import ExecutionRecord
from debian_observer.analytics.store import ExecutionStore
from debian_observer.ingestion.manifest import load_manifest
from debian_observer.ingestion.raw import read_raw_artifact
from debian_observer.normalization.normalizer import Normalizer
from debian_observer.normalization.udddump import (
    UDDDumpError,
    assert_supported_table_shape,
    list_dump_tables,
    peek_dump_copy_columns,
    read_dump_records,
)


def _load_manifest(artifact: Path):
    """Load the manifest sidecar, turning a missing manifest into a
    clear usage error instead of a traceback."""
    try:
        return load_manifest(artifact)
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc


def _run_id_from(manifest) -> str:
    """Recover the ingestion run id from the manifest.

    The pipeline names each dataset `{source.name}-{run_id}`
    (see `IngestionPipeline`); strip the source prefix to get the
    run id back.
    """
    prefix = f"{manifest.source}-"
    if manifest.dataset_name.startswith(prefix):
        return manifest.dataset_name[len(prefix):]
    return manifest.dataset_name


def _write_events(events, output_file) -> int:
    """Write events as JSON lines; return how many were written."""
    for event in events:
        output_file.write(event.to_json_bytes().decode("utf-8") + "\n")
    return len(events)


def _write_invalid(invalid, invalid_file) -> int:
    """Write invalid records as JSON lines; return how many."""
    for record in invalid:
        invalid_file.write(json.dumps(record) + "\n")
    return len(invalid)


def _record_execution(
    *,
    artifact: Path,
    source_name: str,
    records_processed: int,
    events_produced: int,
    invalid_count: int,
    duration: float,
    checksum: str | None = None,
    records_requested: int | None = None,
    metadata: dict | None = None,
) -> None:
    """Persist a normalization ExecutionRecord.

    The database follows the artifact: runs land in
    ``execution_runs.duckdb`` beside the directory of the artifact
    being normalized — the same convention as the ingestion pipeline
    (``raw_dir.parent``), so artifacts in one directory share one
    analytics database wherever they live.
    """
    import platform

    throughput = None
    if duration > 0 and records_processed:
        throughput = records_processed / duration

    host_info = {
        "hostname": platform.node(),
        "cpu_count": platform.processor() or "unknown",
        "python_version": platform.python_version(),
    }

    record = ExecutionRecord(
        run_id=f"norm-{uuid.uuid4().hex[:12]}",
        run_at=datetime.now(timezone.utc),
        execution_type="normalization",
        source_name=source_name,
        records_requested=records_requested,
        records_processed=records_processed,
        events_produced=events_produced,
        duration_seconds=duration,
        throughput_rps=throughput,
        raw_bytes=artifact.stat().st_size,
        raw_path=str(artifact),
        checksum=checksum,
        error_count=invalid_count,
        host_info=json.dumps(host_info),
        metadata=json.dumps(metadata) if metadata else None,
    )

    store = ExecutionStore(str(artifact.parent / "execution_runs.duckdb"))
    store.insert(record)
    store.close()


@click.group(name="normalize")
def normalize() -> None:
    """Validate and transform raw artifacts to domain events.

    One subcommand per raw surface (ADR-012): `artifacts` for
    pipeline `.csv.xz` artifacts, `udddump` for UDD `pg_dump`
    download artifacts. The source name and run id stamped on the
    events are read from each artifact's manifest sidecar
    (`.manifest.json`). Run
    `debian-observer normalize <surface> --help` for a surface's
    options.
    """


@normalize.command(name="artifacts")
@click.argument(
    "artifacts",
    nargs=-1,
    required=True,
    type=click.Path(path_type=Path, exists=True),
)
@click.option(
    "--output",
    "output_file",
    type=click.File("w"),
    default="-",
    help="Output JSON lines file (default: stdout).",
)
@click.option(
    "--invalid",
    "invalid_file",
    type=click.File("w"),
    default=None,
    help="File for invalid records; without it they are dropped.",
)
def normalize_artifacts(
    artifacts: tuple[Path, ...],
    output_file,
    invalid_file,
) -> None:
    """Normalize pipeline raw artifacts (`.csv.xz`) to domain events.

    The source name and run id are read from each artifact's manifest
    sidecar (`.manifest.json`). Records failing validation are
    written to --invalid (or dropped); events go to --output as
    JSON lines. Artifacts are processed in order.
    """
    total_events = 0
    total_invalid = 0

    for artifact in artifacts:
        start = time.perf_counter()
        manifest = _load_manifest(artifact)
        records = read_raw_artifact(artifact)

        normalizer = Normalizer(
            source_name=manifest.source,
            ingestion_run_id=_run_id_from(manifest),
        )
        events, invalid = normalizer.normalize_records(records)
        duration = time.perf_counter() - start

        total_events += _write_events(events, output_file)
        total_invalid += len(invalid)
        if invalid_file:
            _write_invalid(invalid, invalid_file)

        _record_execution(
            artifact=artifact,
            source_name=manifest.source,
            records_processed=len(records),
            events_produced=len(events),
            invalid_count=len(invalid),
            duration=duration,
            checksum=manifest.checksum,
        )

    click.echo(f"Normalized: {total_events} events, {total_invalid} invalid", err=True)


@normalize.command(name="udddump")
@click.argument(
    "dump_artifact",
    type=click.Path(path_type=Path, exists=True),
)
@click.option(
    "--table",
    required=True,
    help=(
        "UDD table to eventize: a TABLE DATA entry of the dump "
        "(validated at startup; the error lists the restorable tables)."
    ),
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Max records to normalize; unset normalizes the whole table.",
)
@click.option(
    "--output",
    "output_file",
    type=click.File("w"),
    default="-",
    help="Output JSON lines file (default: stdout).",
)
@click.option(
    "--invalid",
    "invalid_file",
    type=click.File("w"),
    default=None,
    help="File for invalid records; without it they are dropped.",
)
def normalize_udddump(
    dump_artifact: Path,
    table: str,
    limit: int | None,
    output_file,
    invalid_file,
) -> None:
    """Eventize one table of a downloaded UDD pg_dump artifact.

    The dump artifact is the raw file written by
    `ingest udddump --url <https://…>` (byte-preserving download,
    manifest sidecar beside it): this command reads the manifest for
    the source name and run id, validates `--table` against the
    dump's table of contents (the error lists the restorable
    tables) and the table's COPY header against the registered
    shapes, then streams the COPY block through `pg_restore
    --data-only --table` **on the host** (PATH; install
    `postgresql-client` if absent) — no database is created or
    restored (ADR-006).

    Re-eventizing the same artifact with other filters
    (`--table`/`--limit`) is supported: the raw download is not
    consumed, and each run re-reads the manifest sidecar.
    """
    manifest = _load_manifest(dump_artifact)
    if manifest.source != "udddump":
        raise click.ClickException(
            f"artifact {dump_artifact.name} was ingested from source "
            f"{manifest.source!r}, not 'udddump'; `normalize udddump` "
            "reads udddump dump downloads only — use `normalize "
            "artifacts` for pipeline artifacts"
        )
    if limit is None:
        click.echo(
            "Warning: --limit unset; normalizing the whole table.", err=True
        )
    # Startup gate (ADR-012 amendment, alpha): validate the table
    # against the dump's TOC before any restore work — fail fast
    # listing the restorable tables instead of crashing downstream.
    try:
        available = list_dump_tables(str(dump_artifact))
    except UDDDumpError as exc:
        raise click.ClickException(str(exc)) from exc
    if table not in available:
        raise click.ClickException(
            f"table {table!r} not in dump {dump_artifact.name!r}; "
            f"available tables: {', '.join(sorted(available))}"
        )
    # Perimeter gate (ADR-012, evening amendment): peek the table's
    # COPY header — one stdout line, no data restored — and match it
    # against the registered shapes before any eventization work.
    try:
        columns = peek_dump_copy_columns(str(dump_artifact), table=table)
        assert_supported_table_shape(columns, table=table)
    except UDDDumpError as exc:
        raise click.ClickException(str(exc)) from exc
    # The reading is normalization's job (ADR-012 amendment,
    # 2026-10-23): COPY → records through host pg_restore.
    start = time.perf_counter()
    records = read_dump_records(str(dump_artifact), table=table, limit=limit)
    normalizer = Normalizer(
        source_name=manifest.source,
        ingestion_run_id=_run_id_from(manifest),
    )
    events, invalid = normalizer.normalize_records(records)
    duration = time.perf_counter() - start

    total_events = _write_events(events, output_file)
    if invalid_file:
        _write_invalid(invalid, invalid_file)
    total_invalid = len(invalid)

    _record_execution(
        artifact=dump_artifact,
        source_name=manifest.source,
        records_processed=len(records),
        events_produced=len(events),
        invalid_count=len(invalid),
        duration=duration,
        checksum=manifest.checksum,
        records_requested=limit,
        metadata={"table": table},
    )

    click.echo(
        f"Normalized: {total_events} events, {total_invalid} invalid", err=True
    )