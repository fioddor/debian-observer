"""CLI command: validate.

Validate raw records against expected schemas.
"""

from __future__ import annotations

import json

import click

from debian_observer.ingestion.raw import read_raw_input
from debian_observer.normalization.validator import UDDValidator


@click.command(name="validate")
@click.option(
    "--input",
    "input_file",
    type=click.File("r"),
    default="-",
    help=(
        "Input raw records: JSON lines (stdin or .jsonl) or "
        "xz-compressed CSV artifact (.csv.xz); default stdin."
    ),
)
@click.option(
    "--source",
    type=click.Choice(["udd"]),
    default="udd",
    help="Data source type (for validator selection; default: udd).",
)
def validate(input_file: click.File, source: str) -> None:
    """Validate raw UDD-shaped records against expected fields before
    normalization: a record is valid with a `version` and a `package`
    or `source` name; invalid records are reported with a reason
    (stderr), and the run counts valid/invalid. Exit code is 0 even
    with invalid records — validation is a report, not a gate; run it
    before `normalize`.
    """
    records = read_raw_input(input_file)

    validator = UDDValidator()

    valid, invalid = validator.validate_all(records)

    for record in invalid:
        reason = validator.why_invalid(record)
        click.echo(f"INVALID: {reason} — {json.dumps(record)}", err=True)

    click.echo(f"Valid: {len(valid)}, Invalid: {len(invalid)}", err=True)
