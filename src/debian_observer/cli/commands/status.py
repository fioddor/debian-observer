"""CLI command: status.

Show system health and summary statistics.
"""

from __future__ import annotations

import click

from debian_observer.storage.analytical import AnalyticalStorage


@click.command(name="status")
@click.option(
    "--parquet-dir", default="data/parquet", help="Directory with Parquet files (default: data/parquet)."
)
def status(parquet_dir: str) -> None:
    """Show system status and data summary."""
    try:
        storage = AnalyticalStorage(parquet_dir)
        summary = storage.summary()
        click.echo(f"Files: {summary['files']}")
        click.echo(f"Events: {summary['events']}")
        if summary["types"]:
            click.echo("Event types:")
            for event_type, count in summary["types"].items():
                click.echo(f"  {event_type}: {count}")
    except Exception as exc:
        click.echo(f"Status unavailable: {exc}", err=True)
