"""CLI command: validate.

Validate raw records against expected schemas.
"""

from __future__ import annotations

import json

import click

from debian_observer.normalization.validator import UDDValidator


@click.command(name="validate")
@click.option(
    "--input",
    "input_file",
    type=click.File("r"),
    default="-",
    help="Input JSON lines file (default: stdin).",
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
    records = []
    for line in input_file:
        line = line.strip()
        if line:
            records.append(json.loads(line))

    validator = UDDValidator()

    valid, invalid = validator.validate_all(records)

    for record in invalid:
        reason = validator.why_invalid(record)
        click.echo(f"INVALID: {reason} — {json.dumps(record)}", err=True)

    click.echo(f"Valid: {len(valid)}, Invalid: {len(invalid)}", err=True)
