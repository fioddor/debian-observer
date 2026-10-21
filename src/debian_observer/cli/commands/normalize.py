"""CLI command: normalize.

Validate and transform raw artifacts into canonical domain events.
"""

from __future__ import annotations

import json
from pathlib import Path

import click

from debian_observer.ingestion.manifest import load_manifest
from debian_observer.ingestion.raw import read_raw_artifact
from debian_observer.normalization.normalizer import Normalizer


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


@click.command(name="normalize")
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
def normalize(
    artifacts: tuple[Path, ...],
    output_file,
    invalid_file,
) -> None:
    """Normalize raw artifacts to domain events. Artifacts are
    `.csv.xz` raw files written by `ingest`; the source name and run
    id stamped on the events are read from each artifact's manifest
    sidecar (`.manifest.json`). Records failing validation are
    written to --invalid (or dropped); events go to --output as
    JSON lines.
    """
    total_events = 0
    total_invalid = 0

    for artifact in artifacts:
        manifest = _load_manifest(artifact)
        records = read_raw_artifact(artifact)

        normalizer = Normalizer(
            source_name=manifest.source,
            ingestion_run_id=_run_id_from(manifest),
        )
        events, invalid = normalizer.normalize_records(records)

        # Write events as JSON lines
        for event in events:
            output_file.write(event.to_json_bytes().decode("utf-8") + "\n")

        # Write invalid records if requested
        if invalid_file:
            for record in invalid:
                invalid_file.write(json.dumps(record) + "\n")

        total_events += len(events)
        total_invalid += len(invalid)

    click.echo(f"Normalized: {total_events} events, {total_invalid} invalid", err=True)