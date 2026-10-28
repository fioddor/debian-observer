"""Prosumers: UNIX-pipe Debian observation processors.

Each prosumer is a standalone script (or importable module) that reads
from ``--file-in`` or ``--stream-in`` and writes to ``--file-out`` or
``--stream-out``.

Public API: ``Record``, ``Source``, ``Sink``, plus stream/file variants.
"""

from debian_observer.prosumers.packages import (
    CONSOLIDATED_COLUMNS,
    CONSOLIDATION_PK,
    ConsolidateError,
    FoldStats,
    fold_package_versions,
    write_consolidated_table,
)
from debian_observer.prosumers.stream import (
    FileSink,
    FileSource,
    Record,
    Sink,
    Source,
    StreamSink,
    StreamSource,
    open_sink,
    open_source,
)

__all__ = [
    # Stream primitives
    "Record",
    "Source",
    "Sink",
    "FileSource",
    "FileSink",
    "StreamSource",
    "StreamSink",
    "open_source",
    "open_sink",
    # Consolidation engine
    "fold_package_versions",
    "write_consolidated_table",
    "FoldStats",
    "ConsolidateError",
    "CONSOLIDATED_COLUMNS",
    "CONSOLIDATION_PK",
]
