#!/usr/bin/env python3
"""Filter package-version events by distribution and/or release.

A simple stateless prosumer — every input record is either passed
through or dropped depending on the configured filters.  Because it
is stateless, it can be inserted anywhere in a pipeline.

Usage::

    # Filter a Parquet file on disk
    debian-observer-filter-packages --file-in events.parquet \
        --distribution debian --release sid --file-out filtered.parquet

    # Pipe through a streaming pipeline
    debian-observer-consolidate --stream-in --stream-out | \
        debian-observer-filter-packages --stream-in --stream-out \
        --distribution debian | \
        debian-observer-consolidate --stream-in --stream-out \
        > debian-only.parquet
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import click

from debian_observer.prosumers.cli import build_sink, build_source, common_io_options
from debian_observer.prosumers.stream import Record


def _match(record: Record, distributions: set[str], releases: set[str]) -> bool:
    """Return True if *record* passes both filter sets.

    An empty set means "no restriction" for that dimension.
    Records without a concrete ``distribution`` value are passed through
    unchanged (they are not package observations).
    """
    payload = record.payload
    dist = payload.get("distribution")
    # No concrete distribution → not a package observation → pass through.
    # pandas Parquet writes None as NaN (float), so we check type too.
    if dist is None or not isinstance(dist, str):
        return True

    if distributions and dist not in distributions:
        return False
    if releases and payload.get("release") not in releases:
        return False
    return True


@click.command()
@common_io_options()
@click.option(
    "--distribution",
    "distributions",
    multiple=True,
    help="Only pass records with this distribution (repeatable).",
)
@click.option(
    "--release",
    "releases",
    multiple=True,
    help="Only pass records with this release (repeatable).",
)
def main(
    file_in: Path | None,
    file_out: Path | None,
    stream_in: bool,
    stream_out: bool,
    distributions: tuple[str, ...],
    releases: tuple[str, ...],
) -> None:
    """Pass through package-version records matching distribution / release."""
    dist_set = set(distributions)
    rel_set = set(releases)

    if not file_in and not stream_in:
        raise click.UsageError("Must provide --file-in or --stream-in")
    if not file_out and not stream_out:
        raise click.UsageError("Must provide --file-out or --stream-out")
    if not dist_set and not rel_set:
        click.echo("Warning: no filters set — all records will pass through.", err=True)

    source = build_source(file_in, stream_in)
    sink = build_sink(file_out, stream_out)

    with source, sink:
        for record in source:
            if _match(record, dist_set, rel_set):
                sink.emit(record)


if __name__ == "__main__":
    main()
