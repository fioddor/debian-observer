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
@click.option("--port", default=8088, type=int, help="API listen port (default: 8088).")
@click.option(
    "--events-dir",
    default=None,
    help="Directory with the event log to load at startup; falls back to the EVENTS_DIR environment variable, otherwise starts with an empty store.",
)
def serve(host: str, port: int, events_dir: str | None) -> None:
    """Start the Debian Observer API service."""
    # Allow the event-log directory to be set via environment variable when
    # --events-dir is not explicitly provided on the command line.
    resolved_events_dir = events_dir or os.environ.get("EVENTS_DIR")

    store = InMemoryStore()
    if resolved_events_dir:
        loader = EventLoader(store)
        count = loader.load_from_parquet(resolved_events_dir)
        click.echo(f"Loaded {count} events from {resolved_events_dir}", err=True)

    app = create_app(store=store)
    click.echo(f"Starting API on http://{host}:{port}", err=True)
    uvicorn.run(app, host=host, port=port)
