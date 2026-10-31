"""CLI command: execution-report.

Show historical execution summaries with percentiles.
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import click

from debian_observer.analytics.store import ExecutionStore


@click.command(name="execution-report")
@click.option(
    "--days",
    default=7,
    type=int,
    help="Number of days to look back (default: 7).",
)
@click.option(
    "--type",
    "execution_type",
    help="Filter by execution type (e.g., ingestion, query).",
)
@click.option(
    "--source",
    "source_name",
    help="Filter by source name (e.g., udd, benchmark, mirror).",
)
@click.option(
    "--format",
    "output_format",
    default="table",
    type=click.Choice(["table", "json", "csv"]),
    help="Output format (default: table).",
)
@click.option(
    "--db",
    "db_path",
    default="data/execution_runs.duckdb",
    help="Path to the DuckDB analytics database (default: data/execution_runs.duckdb).",
)
def execution_report(
    days: int,
    execution_type: str | None,
    source_name: str | None,
    output_format: str,
    db_path: str,
) -> None:
    """Show historical execution report with percentiles.

    Aggregates execution_runs by type and source, showing counts and
    median (50th percentile) duration and throughput.
    """
    path = Path(db_path)
    if not path.exists():
        click.echo("No execution records found (database does not exist).")
        return

    store = ExecutionStore(str(path))
    try:
        # Build WHERE clause
        where_parts = [f"run_at >= now() - INTERVAL '{days} days'"]
        params: list[object] = []
        if execution_type:
            where_parts.append("execution_type = ?")
            params.append(execution_type)
        if source_name:
            where_parts.append("source_name = ?")
            params.append(source_name)

        where_sql = " AND ".join(where_parts)

        sql = f"""
            SELECT
                execution_type,
                source_name,
                COUNT(*) AS run_count,
                quantile_cont(duration_seconds, 0.5) AS median_duration,
                quantile_cont(throughput_rps, 0.5) AS median_throughput
            FROM execution_runs
            WHERE {where_sql}
            GROUP BY execution_type, source_name
            ORDER BY execution_type, source_name
        """

        result = store.query(sql, params)
        rows = result.fetchall()
        if not rows:
            click.echo("No runs in the selected period.")
            return

        headers = ["execution_type", "source_name", "run_count", "median_duration", "median_throughput"]

        if output_format == "table":
            click.echo(" | ".join(headers))
            click.echo("-" * (len(" | ".join(headers)) + 20))
            for row in rows:
                click.echo(" | ".join(str(c) for c in row))
        elif output_format == "json":
            out = [dict(zip(headers, row)) for row in rows]
            click.echo(json.dumps(out, indent=2, default=str))
        elif output_format == "csv":
            writer = csv.writer(sys.stdout)
            writer.writerow(headers)
            writer.writerows(rows)

    finally:
        store.close()
