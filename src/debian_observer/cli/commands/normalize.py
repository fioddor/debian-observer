"""CLI command: normalize.

Validate and transform raw records into canonical domain events.
"""

from __future__ import annotations

import json

import click

from debian_observer.normalization.normalizer import Normalizer


@click.command(name="normalize")
@click.option("--source", default="udd", help="Data source name stamped on events (default: udd).")
@click.option("--run-id", default=None, help="Ingestion run identifier (default: generated `cli-normalize`).")
@click.option(
    "--input",
    "input_file",
    type=click.File("r"),
    default="-",
    help="Input JSON lines file (default: stdin).",
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
    source: str,
    run_id: str | None,
    input_file: click.File,
    output_file: click.File,
    invalid_file: click.File,
) -> None:
    """Normalize raw records to domain events; records failing
    validation are written to --invalid (or dropped)."""
    # Read records from JSON lines
    records = []
    for line in input_file:
        line = line.strip()
        if line:
            records.append(json.loads(line))

    normalizer = Normalizer(
        source_name=source, ingestion_run_id=run_id or "cli-normalize"
    )
    events, invalid = normalizer.normalize_records(records)

    # Write events as JSON lines
    for event in events:
        output_file.write(event.to_json_bytes().decode("utf-8") + "\n")

    # Write invalid records if requested
    if invalid_file:
        for record in invalid:
            invalid_file.write(json.dumps(record) + "\n")

    click.echo(f"Normalized: {len(events)} events, {len(invalid)} invalid", err=True)
