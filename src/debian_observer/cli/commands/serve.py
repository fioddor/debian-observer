"""CLI command: serve.

Start the FastAPI analytical service.
"""

from __future__ import annotations

import os

import click
import uvicorn

from debian_observer.api.app import create_app
from debian_observer.api.loader import EventLoader
from debian_observer.api.store import InMemoryStore


@click.command(name="serve")
@click.option("--host", default="127.0.0.1", help="API listen host (default: 127.0.0.1).")
@click.option("--port", default=8000, type=int, help="API listen port (default: 8000).")
@click.option(
    "--parquet-dir",
    default=None,
    help="Directory with Parquet data to load at startup; falls back to the PARQUET_DIR environment variable, otherwise starts with an empty store.",
)
def serve(host: str, port: int, parquet_dir: str | None) -> None:
    """Start the Debian Observer API service."""
    # Allow parquet directory to be set via environment variable when --parquet-dir
    # is not explicitly provided on the command line.
    resolved_parquet_dir = parquet_dir or os.environ.get("PARQUET_DIR")

    store = InMemoryStore()
    if resolved_parquet_dir:
        loader = EventLoader(store)
        count = loader.load_from_parquet(resolved_parquet_dir)
        click.echo(f"Loaded {count} events from {resolved_parquet_dir}", err=True)

    app = create_app(store=store)
    click.echo(f"Starting API on http://{host}:{port}", err=True)
    uvicorn.run(app, host=host, port=port)
