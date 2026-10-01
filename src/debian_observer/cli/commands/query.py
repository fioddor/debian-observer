"""CLI command: query.

Execute analytical queries over stored Parquet data via DuckDB.
"""

from __future__ import annotations

import click

from debian_observer.storage.analytical import AnalyticalStorage


@click.command(name="query")
@click.argument("sql")
@click.option("--parquet-dir", default="data/parquet", help="Directory with Parquet files (default: data/parquet).")
@click.option("--format", "output_format", default="table", type=click.Choice(["table", "csv"]), help="Output format (default: table).")
def query(sql: str, parquet_dir: str, output_format: str) -> None:
    """Run a SQL query over Parquet data."""
    storage = AnalyticalStorage(parquet_dir)
    result = storage.query(sql)

    if output_format == "table":
        # Fetch all and print
        rows = result.fetchall()
        if rows:
            # Print header
            header = [desc[0] for desc in result.description or []]
            click.echo(" | ".join(header))
            click.echo("-" * (len(" | ".join(header)) + 10))
            for row in rows:
                click.echo(" | ".join(str(c) for c in row))
        else:
            click.echo("No rows returned.")
    elif output_format == "csv":
        import csv
        import sys
        rows = result.fetchall()
        if rows:
            header = [desc[0] for desc in result.description or []]
            writer = csv.writer(sys.stdout)
            writer.writerow(header)
            writer.writerows(rows)
