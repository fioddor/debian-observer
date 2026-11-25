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

from debian_observer.storage.parquet import (
    IncrementalParquetWriter,
    EVENTS_DIR_DEFAULT,
)

from debian_observer.analytics.models import ExecutionRecord
from debian_observer.analytics.store import ExecutionStore
from debian_observer.ingestion.manifest import load_manifest
from debian_observer.ingestion.raw import read_raw_artifact
from debian_observer.normalization.normalizer import Normalizer
from debian_observer.normalization.eventizers import (
    get_eventizer,
    list_eventizers,
)
from debian_observer.normalization.udddump import (
    UDDDumpError,
    assert_supported_table_shape,
    iter_dump_rows,
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


@click.option(
    "--eventizer",
    "eventizer_name",
    type=str,
    default=None,
    help=(
        "Eventizer for this source: one of "
        + ", ".join(list_eventizers("udddump"))
        + " (default: package-versions, the retro-compatible behaviour)."
    ),
)
@click.option(
    "--events-dir",
    "events_dir",
    type=click.Path(path_type=Path),
    default=Path(EVENTS_DIR_DEFAULT),
    help=(
        "Write the Parquet event log here: one directory per "
        "event type, one file per run (default: "
        + EVENTS_DIR_DEFAULT + "), the prosumers' input."
    ),
)

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
    default=None,
    help=(
        "Also write the events as JSON lines to this file (- for "
        "stdout); the Parquet event log is always written."
    ),
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
    eventizer_name,
    events_dir,
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
    # Perimeter gate (ADR-012): peek the table's COPY header — one
    # stdout line, no data restored — before any eventization work.
    # Which shape it must match depends on the selection: with a
    # selected eventizer, only ITS shape gates (its table may be
    # outside the log's perimeter); with none, the whole log.
    try:
        columns = peek_dump_copy_columns(str(dump_artifact), table=table)
        if eventizer_name is None:
            assert_supported_table_shape(columns, table=table)
    except UDDDumpError as exc:
        raise click.ClickException(str(exc)) from exc
    # The reading is normalization's job (ADR-012 amendment,
    # 2026-10-23): COPY rows stream one at a time through host
    # pg_restore (ADR-015) into the selected eventizer (ADR-014).
    eventizer = get_eventizer(manifest.source, eventizer_name)
    if hasattr(eventizer, "with_provenance"):
        eventizer = eventizer.with_provenance(
            source_name=manifest.source,
            ingestion_run_id=_run_id_from(manifest),
        )
    try:
        assert_supported_table_shape(
            columns, table=table, shape=eventizer.shape
        )
    except UDDDumpError as exc:
        raise click.ClickException(str(exc)) from exc
    run_id = _run_id_from(manifest)
    start = time.perf_counter()
    invalid: list = []
    # Incremental writer (ADR-015): events stream into the Parquet
    # file row group by row group. The --output JSON lines are a
    # copy written line by line beside it: nothing materializes.
    from debian_observer.storage.parquet import writer_for_event_type

    parquet_writer = writer_for_event_type(
        eventizer.event_type, events_dir=str(events_dir)
    )
    inc_writer = parquet_writer.open_parquet_file(run_id)
    total_events = 0
    for row in iter_dump_rows(
        str(dump_artifact),
        table=table,
        limit=limit,
    ):
        for out in eventizer.eventize(row):
            if isinstance(out, dict):
                invalid.append(out)
                continue
            total_events += 1
            inc_writer.append_event(out)
            if output_file is not None:
                _write_events([out], output_file)
    inc_writer.close()
    duration = time.perf_counter() - start

    if invalid_file:
        _write_invalid(invalid, invalid_file)
    total_invalid = len(invalid)

    _record_execution(
        artifact=dump_artifact,
        source_name=manifest.source,
        records_processed=total_events + total_invalid,
        events_produced=total_events,
        invalid_count=total_invalid,
        duration=duration,
        checksum=manifest.checksum,
        records_requested=limit,
        metadata={
            "table": table,
            "eventizer": eventizer.name,
            "events_dir": str(events_dir),
            "parquet_events": total_events,
        },
    )

    click.echo(
        f"Normalized: {total_events} events, {total_invalid} invalid", err=True
    )


@normalize.command(name="britney")
@click.argument(
    "manifest",
    type=click.Path(path_type=Path, exists=True),
)
@click.option(
    "--raw-dir",
    "raw_dir",
    default="data/raw",
    help="Root of the raw store the window was ingested into "
    "(default: data/raw).",
)
@click.option(
    "--eventizer",
    "eventizer_name",
    default="excuses",
    help=(
        "Eventizer for this source: one of "
        + ", ".join(list_eventizers("britney"))
        + " (default: excuses, the only eventization — the pair "
        "eventizer, ADR-024)."
    ),
)
@click.option(
    "--events-dir",
    "events_dir",
    type=click.Path(path_type=Path),
    default=Path(EVENTS_DIR_DEFAULT),
    help=(
        "Write the Parquet event lake here: one directory per "
        "event type, one file per run (default: "
        + EVENTS_DIR_DEFAULT + "), the prosumers' input."
    ),
)
@click.option(
    "--checkpoint",
    "checkpoint_path",
    type=click.Path(path_type=Path),
    default=None,
    help=(
        "Checkpoint file of the eventizer: the last eventized "
        "snapshot; a restart resumes after it "
        "(default: <events-dir>/checkpoint-britney-excuses.json)."
    ),
)
@click.option(
    "--output",
    "output_file",
    type=click.File("w"),
    default=None,
    help=(
        "Also write the events as JSON lines to this file (- for "
        "stdout); the Parquet event lake is always written."
    ),
)
def normalize_britney(
    manifest: Path,
    raw_dir: str,
    eventizer_name: str,
    events_dir: Path,
    checkpoint_path: Path | None,
    output_file,
) -> None:
    """Eventize an ingested britney window: seed, pairs, lake, resume.

    Reads a window MANIFEST written by `ingest britney`, parses each
    observed snapshot from the raw store (verifying SHA-256 against
    the manifest), orders them by generated-date, seeds the first
    snapshot complete (ADR-028) and eventizes each following one as
    deltas against its predecessor (ADR-024) — sequentially
    (ADR-027). The events go to the Parquet event lake (one
    directory per event type, one file per run), their event_ids are
    derived (ADR-025: a re-eventized pair keeps its ids), and a
    checkpoint advances per eventized snapshot: a restart resumes
    after it, and re-running a published window writes ZERO new
    events and rewrites none of the old ones.

    C2 produces to the lake only; publication to the migration
    stream is C3.
    """
    import json as _json
    import uuid as _uuid

    from debian_observer.ingestion.britney import RAW_SUBDIR, sha256_bytes
    from debian_observer.normalization.britney import parse_snapshot
    from debian_observer.normalization.eventizers.excuses import (
        EventizerCheckpoint,
    )
    from debian_observer.storage.parquet import writer_for_event_type

    window = _json.loads(manifest.read_text(encoding="utf-8"))
    if window.get("source") != "britney":
        raise click.ClickException(
            f"manifest {manifest.name} was ingested from source "
            f"{window.get('source')!r}, not 'britney'; `normalize "
            "britney` reads britney window manifests only"
        )
    eventizer = get_eventizer("britney", eventizer_name)
    # The raw subtree of this window: archive runs live under their
    # year, the live follower under `live` (the acquisition's own
    # layout, §4.2/§7.4).
    subdir = "live" if window.get("live") else str(window.get("year"))
    parsed = []
    for entry in window.get("snapshots", []):
        snapshot_id = entry["snapshot_id"]
        path = (
            Path(raw_dir) / RAW_SUBDIR / subdir / f"{snapshot_id}.yaml.gz"
        )
        if not path.exists():
            raise click.ClickException(
                f"snapshot {snapshot_id} is not in the raw store "
                f"({path}); run `ingest britney` first"
            )
        body = path.read_bytes()
        if sha256_bytes(body) != entry.get("sha256"):
            raise click.ClickException(
                f"snapshot {snapshot_id} fails its manifest sha256: "
                f"{path} is not the published file"
            )
        parsed.append(
            parse_snapshot(snapshot_id, body, url=entry.get("url"))
        )

    checkpoint = EventizerCheckpoint.load(
        checkpoint_path or Path(events_dir) / "checkpoint-britney-excuses.json"
    )
    lake_run = f"norm-{window.get('run_id', 'britney')}-{_uuid.uuid4().hex[:8]}"
    writers: dict[str, IncrementalParquetWriter] = {}
    snapshots_eventized = 0
    total_events = 0
    start = time.perf_counter()
    for snapshot_id, events in eventizer.iter_eventized(
        parsed, checkpoint=checkpoint
    ):
        for event in events:
            writer = writers.get(event.event_type)
            if writer is None:
                writer = writer_for_event_type(
                    event.event_type, events_dir=str(events_dir)
                ).open_parquet_file(lake_run)
                writers[event.event_type] = writer
            writer.append_event(event)
            if output_file is not None:
                _write_events([event], output_file)
        checkpoint.save(snapshot_id)
        snapshots_eventized += 1
        total_events += len(events)
    for writer in writers.values():
        writer.close()

    if snapshots_eventized == 0:
        click.echo(
            f"Eventized: 0 new events (checkpoint at "
            f"{checkpoint.last_snapshot})", err=True
        )
    else:
        click.echo(
            f"Eventized: {snapshots_eventized} snapshots, "
            f"{total_events} events", err=True
        )

    _record_execution(
        artifact=manifest,
        source_name="britney",
        records_processed=len(parsed),
        events_produced=total_events,
        invalid_count=0,
        duration=time.perf_counter() - start,
        metadata={
            "eventizer": eventizer.name,
            "snapshots": len(parsed),
            "snapshots_eventized": snapshots_eventized,
            "events_dir": str(events_dir),
            "checkpoint": str(
                checkpoint_path
                or Path(events_dir) / "checkpoint-britney-excuses.json"
            ),
        },
    )