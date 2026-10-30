#!/usr/bin/env python3
"""Consolidate event log into a single deduplicated package-versions table.

Usage::

    # File → file (batch rebuild of the consolidated table)
    debian-observer-consolidate --file-in events/run-*.parquet --file-out package_versions.parquet

    # Stream → stream (pipe-friendly)
    cat events.ndjson | debian-observer-consolidate --stream-in --stream-out > consolidated.ndjson

    # File → stream (inspect folded results as ND-JSON)
    debian-observer-consolidate --file-in events.parquet --stream-out
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import click
import pyarrow as pa
import pyarrow.parquet as pq

from debian_observer.prosumers.cli import build_sink, build_source, common_io_options
from debian_observer.prosumers.packages import ConsolidateError, fold_package_versions, write_consolidated_table
from debian_observer.prosumers.stream import Record, StreamSink


@click.command()
@common_io_options()
def main(file_in: Path | None, file_out: Path | None, stream_in: bool, stream_out: bool) -> None:
    """Fold package-version observation events into a consolidated table."""
    if not file_in and not stream_in:
        raise click.UsageError("Must provide --file-in or --stream-in")
    if not file_out and not stream_out:
        raise click.UsageError("Must provide --file-out or --stream-out")
    source = build_source(file_in, stream_in)
    sink = build_sink(file_out, stream_out)

    with source, sink:
        records = list(source)
        try:
            rows, stats = fold_package_versions(records)
        except ConsolidateError as exc:
            raise click.ClickException(str(exc)) from exc

        click.echo(
            f"Events read: {stats.events_read} | "
            f"PVO events: {stats.pvo_events} | "
            f"Consolidated rows: {stats.rows}",
            err=True,
        )

        for row in rows:
            record = Record(
                event_type="PackageVersionConsolidated",
                payload={k: v for k, v in row.items() if k not in ("observed_first_at", "observed_last_at")},
                meta={
                    "observed_first_at": row["observed_first_at"],
                    "observed_last_at": row["observed_last_at"],
                },
            )
            sink.emit(record)

        if not isinstance(sink, StreamSink):
            # FileSink path: report the written path
            if file_out:
                click.echo(f"Written: {file_out}")
            else:
                # Default path via write_consolidated_table (not used in
                # normal prosumer pipeline, but kept for backward compat)
                dataset_dir = Path(".")
                output_path = write_consolidated_table(rows, dataset_dir=dataset_dir)
                click.echo(f"Written: {output_path}")


if __name__ == "__main__":
    main()
