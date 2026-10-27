"""CLI command: query.

Execute analytical queries over stored Parquet data via DuckDB.
"""

from __future__ import annotations

import sys

import click

from debian_observer.storage.analytical import AnalyticalStorage


@click.command(name="query")
@click.argument("sql", required=False)
@click.option(
    "--events-dir",
    default="data/events",
    help="Directory with the event log (default: data/events).",
)
@click.option(
    "--dataset-dir",
    default="data/normalized/package_versions",
    help="Directory with consolidated dataset Parquet files (default: data/normalized/package_versions).",
)
@click.option(
    "--format",
    "output_format",
    default="table",
    type=click.Choice(["table", "json", "csv"]),
    help="Output format (default: table).",
)
@click.option(
    "--debug",
    is_flag=True,
    help="Show full traceback on error.",
)
@click.option(
    "--describe",
    "describe_flag",
    is_flag=True,
    help="Show available tables and columns, then exit.",
)
def query(
    sql: str,
    events_dir: str,
    dataset_dir: str,
    output_format: str,
    debug: bool,
    describe_flag: bool,
) -> None:
    """Run a SQL query over Parquet events and datasets.

    ``events`` is a view over all ``*.parquet`` files inside
    ``--events-dir``.  Each ``*.parquet`` file inside
    ``--dataset-dir`` is also available as a view named after its
    stem (e.g. ``package_versions``).
    """
    if sql is None and not describe_flag:
        click.echo("Error: Missing argument 'SQL'.", err=True)
        sys.exit(1)

    try:
        storage = AnalyticalStorage(events_dir, dataset_dir)

        if describe_flag:
            info = storage.describe()
            if not info:
                click.echo("No tables available.", err=True)
                sys.exit(1)
            for table, columns in info.items():
                click.echo(table)
                for col_name, col_type in columns:
                    click.echo(f"  {col_name} ({col_type})")
            return

        result = storage.query(sql)
    except Exception as exc:
        if debug:
            raise
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)

    if output_format == "table":
        rows = result.fetchall()
        if rows:
            header = [desc[0] for desc in result.description or []]
            click.echo(" | ".join(header))
            click.echo("-" * (len(" | ".join(header)) + 10))
            for row in rows:
                click.echo(" | ".join(str(c) for c in row))
        else:
            click.echo("No rows returned.")
    elif output_format == "json":
        import json

        rows = result.fetchall()
        if rows:
            header = [desc[0] for desc in result.description or []]
            out = [dict(zip(header, row)) for row in rows]
            click.echo(json.dumps(out, indent=2, default=str))
        else:
            click.echo("[]")
    elif output_format == "csv":
        import csv

        rows = result.fetchall()
        if rows:
            header = [desc[0] for desc in result.description or []]
            writer = csv.writer(sys.stdout)
            writer.writerow(header)
            writer.writerows(rows)
