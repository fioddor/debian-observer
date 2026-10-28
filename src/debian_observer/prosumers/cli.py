"""CLI helpers for prosumers.

Provides ``click`` options with standard names so every prosumer speaks
the same CLI dialect:

    --file-in PATH      read from a Parquet file
    --file-out PATH     write to a Parquet file
    --stream-in         read ND-JSON from stdin
    --stream-out        write ND-JSON to stdout

Usage example (inside a prosumer module)::

    from debian_observer.prosumers.cli import common_io_options, build_source, build_sink

    @click.command()
    @common_io_options(required_in=("file",), required_out=("file",))
    def my_prosumer(file_in, file_out, stream_in, stream_out):
        source = build_source(file_in, stream_in)
        sink = build_sink(file_out, stream_out)
        ...
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import click

from debian_observer.prosumers.stream import open_sink, open_source


def common_io_options():
    """Return four click.option decorators in the standard order.

    All options are optional by default.  Each prosumer should validate
    that at least one input and at least one output are provided.
    """

    def decorator(f: Any) -> Any:
        f = click.option(
            "--file-in",
            "file_in",
            type=click.Path(path_type=Path, exists=True),
            help="Input Parquet file path.",
            default=None,
        )(f)
        f = click.option(
            "--file-out",
            "file_out",
            type=click.Path(path_type=Path),
            help="Output Parquet file path.",
            default=None,
        )(f)
        f = click.option(
            "--stream-in",
            "stream_in",
            is_flag=True,
            help="Read ND-JSON records from stdin.",
            default=False,
        )(f)
        f = click.option(
            "--stream-out",
            "stream_out",
            is_flag=True,
            help="Write ND-JSON records to stdout.",
            default=False,
        )(f)
        return f

    return decorator


def build_source(
    file_in: Path | None,
    stream_in: bool,
):
    """Instantiate a Source from parsed CLI flags."""
    return open_source(
        file_in=file_in,
        stream_in=stream_in,
        stream=sys.stdin,
    )


def build_sink(
    file_out: Path | None,
    stream_out: bool,
):
    """Instantiate a Sink from parsed CLI flags."""
    return open_sink(
        file_out=file_out,
        stream_out=stream_out,
        stream=sys.stdout,
    )
