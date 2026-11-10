"""Prosumers: the batched event-lake reader.

A lake (one event-type directory of Parquet files) is read in
batches with ``ParquetFile.iter_batches`` and handed to the caller
batch by batch: never ``read_table``, never ``to_pandas``, never
the whole lake in memory (ADR-015) — a full 8.4 M-event lake read
in one pass would not fit; folded batch by batch it does.

"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

#: Rows per batch handed to the fold (spec: a fixed size).
BATCH_SIZE = 100_000


def lake_files(lake: Path | str) -> list[Path]:
    """Resolve a lake to its Parquet files, in sorted order.

    Public substrate contract (the dopi-pdda plug-in consumes it):
    a lone Parquet file passes through; a glob pattern resolves to
    its matches; a directory resolves to its *.parquet files.
    The list drives every batched read of the lake.
    """
    path = Path(lake)
    if path.is_dir():
        return sorted(path.glob("*.parquet"))
    import glob as _glob

    matches = sorted(
        Path(p) for p in _glob.glob(str(path)) if Path(p).is_file()
    )
    return matches or [path]


def read_lake_batches(
    lake: Path | str,
    *,
    batch_size: int = BATCH_SIZE,
    columns: list[str] | None = None,
) -> Iterator[list[dict[str, Any]]]:
    """Yield the lake's events as batches of row dicts.

    Args:
        lake: a Parquet file, a glob pattern, or a directory of
            Parquet files (read as one lake, files in sorted order).
        batch_size: rows per batch.
        columns: projection — only these columns are read.

    Yields:
        Lists of row dicts (``batch_size`` rows, the tail batch
        smaller); each batch is materialized, the lake never is.
    """
    for file in lake_files(lake):
        parquet = pq.ParquetFile(file)
        for batch in parquet.iter_batches(
            batch_size=batch_size, columns=columns
        ):
            # to_pylist rows straight from the record batch: no
            # dataframe in between, no whole-file load.
            yield batch.to_pylist()


def fold_lake(
    lake: Path | str,
    *,
    initial: Any,
    step: Callable[[Any, dict[str, Any]], Any],
    batch_size: int = BATCH_SIZE,
    columns: list[str] | None = None,
) -> Any:
    """Fold a lake batch by batch into one accumulator.

    ``step(accumulator, event)`` returns the next accumulator;
    only the accumulator and the current batch are resident.
    """
    accumulator = initial
    for batch in read_lake_batches(
        lake, batch_size=batch_size, columns=columns
    ):
        for event in batch:
            accumulator = step(accumulator, event)
    return accumulator