"""CLI command: consolidate.

Fold the event log into the consolidated ``package_versions`` table.
"""

from __future__ import annotations

from pathlib import Path

import click

from debian_observer.consumer.packages import (
    ConsolidateError,
    fold_package_versions,
    write_consolidated_table,
)


@click.command(name="consolidate")
@click.option(
    "--events-dir",
    "events_dir",
    required=True,
    type=click.Path(path_type=Path, exists=True, file_okay=False),
    help="Directory of the Parquet event log (one file per ingestion run).",
)
@click.option(
    "--dataset-dir",
    "dataset_dir",
    default="data/normalized/package_versions",
    type=click.Path(path_type=Path),
    help=(
        "Output directory for the consolidated table "
        "(default: data/normalized/package_versions)."
    ),
)
def consolidate(events_dir: Path, dataset_dir: Path) -> None:
    """Consolidate the packages table from the event log.

    Reads every *.parquet event-log file in --events-dir (the append-only
    log; one file per ingestion run), keeps the
    PackageVersionObserved events and folds them into the consolidated
    package_versions table: one row per full observation PK
    (package_id, version, distribution, release, suite,
    architecture), last-observed-wins attributes, min/max observed_at
    per group. Writes ONE re-generable file — package_versions.parquet
    (fixed name) inside --dataset-dir — via an atomic tmp+rename; the
    fold is idempotent, so re-running over the same log rewrites the
    identical table. NATS/streaming is not involved: this is the R1
    local consumer over the on-disk event log.
    """
    try:
        rows, stats = fold_package_versions(events_dir)
        output_path = write_consolidated_table(rows, dataset_dir=dataset_dir)
    except ConsolidateError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Files read: {stats.files_read}")
    click.echo(f"Events read: {stats.events_read}")
    click.echo(f"{stats.pvo_events} PackageVersionObserved events")
    click.echo(f"Consolidated rows: {stats.rows}")
    click.echo(f"Dataset written: {output_path}")