"""CLI command: validate.

Validate raw artifacts against expected schemas.
"""

from __future__ import annotations

import json
from pathlib import Path

import click

from debian_observer.ingestion.manifest import load_manifest
from debian_observer.ingestion.raw import read_raw_artifact
from debian_observer.normalization.validator import UDDValidator


def _load_manifest(artifact: Path):
    """Load the manifest sidecar, turning a missing manifest into a
    clear usage error instead of a traceback."""
    try:
        return load_manifest(artifact)
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc


@click.command(name="validate")
@click.argument(
    "artifacts",
    nargs=-1,
    required=True,
    type=click.Path(path_type=Path, exists=True),
)
def validate(artifacts: tuple[Path, ...]) -> None:
    """Validate raw artifacts against the expected UDD shape before
    normalization: a record is valid with a `version` and a
    `package` or `source` name. Artifacts are `.csv.xz` raw files
    written by `ingest`; each artifact's source is read from its
    manifest sidecar (`.manifest.json`, written beside every raw
    artifact). Invalid records are reported with a reason (stderr),
    and the run counts valid/invalid per artifact. Exit code is 0
    even with invalid records — validation is a report, not a gate;
    run it before `normalize`.
    """
    validator = UDDValidator()

    for artifact in artifacts:
        manifest = _load_manifest(artifact)
        records = read_raw_artifact(artifact)

        valid, invalid = validator.validate_all(records)

        for record in invalid:
            reason = validator.why_invalid(record)
            click.echo(f"INVALID: {reason} — {json.dumps(record)}", err=True)

        click.echo(
            f"{artifact.name} ({manifest.source}): "
            f"Valid: {len(valid)}, Invalid: {len(invalid)}",
            err=True,
        )