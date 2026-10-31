"""Analytics store backed by DuckDB.

Every instrumented execution run is persisted to a local DuckDB file
and can be queried by the ``execution-report`` CLI command.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from debian_observer.analytics.models import ExecutionRecord

#: Column definitions for ``execution_runs`` with descriptions used as COMMENTs.
_COLUMN_DEFS: list[tuple[str, str, str]] = [
    ("run_id", "TEXT PRIMARY KEY", "Unique identifier for this execution run"),
    ("run_at", "TIMESTAMP", "When the run completed (UTC)"),
    ("execution_type", "TEXT", "Category: ingestion, normalization, replay-stream, query, etc."),
    ("source_name", "TEXT", "Data source: udd, mirror, benchmark, snapshot-YYYYMMDD, etc."),
    ("records_requested", "INTEGER", "Records the caller asked to process (NULL if not applicable)"),
    ("records_processed", "INTEGER", "Records actually processed"),
    ("events_produced", "INTEGER", "Domain events generated"),
    ("duration_seconds", "REAL", "Wall-clock time from start to completion"),
    ("throughput_rps", "REAL", "records_processed / duration_seconds (NULL if duration <= 0)"),
    ("raw_bytes", "INTEGER", "Size of the raw artifact in bytes"),
    ("parquet_bytes", "INTEGER", "Size of the Parquet output in bytes"),
    ("raw_path", "TEXT", "Filesystem path to the raw artifact"),
    ("parquet_path", "TEXT", "Filesystem path to the Parquet output"),
    ("checksum", "TEXT", "SHA-256 of the raw artifact"),
    ("error_count", "INTEGER DEFAULT 0", "Number of errors captured during execution"),
    ("error_message", "TEXT", "First error message if error_count > 0"),
    ("host_info", "TEXT", "JSON: hostname, cpu_count, mem_gb, python_version"),
    ("metadata", "TEXT", "JSON: type-specific fields, freely extensible"),
]


class ExecutionStore:
    """DuckDB-backed persistence for execution records.

    Args:
        db_path: Path to the DuckDB database file, or ``:memory:`` for tests.
    """

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)
        self._con = duckdb.connect(self._db_path)
        self._init_db()

    def _init_db(self) -> None:
        """Create or MIGRATE the ``execution_runs`` table.

        CREATE TABLE IF NOT EXISTS does not add columns: a database
        written by an older release (fewer columns) is migrated with
        ALTER TABLE ... ADD COLUMN before the COMMENTs run — opening an
        existing database must never crash (execution-report CLI).
        COMMENT ON COLUMN interpolates the _COLUMN_DEFS comment text:
        escape single quotes (a comment with an apostrophe is a
        ParserException otherwise).
        """
        columns_sql = ",\n    ".join(
            f"{name} {dtype}" for name, dtype, _ in _COLUMN_DEFS
        )
        self._con.execute(f"CREATE TABLE IF NOT EXISTS execution_runs (\n    {columns_sql}\n)")

        # Schema migration: add any column an older release did not have.
        existing = {
            row[0]
            for row in self._con.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'execution_runs'"
            ).fetchall()
        }
        for name, dtype, _ in _COLUMN_DEFS:
            if name not in existing:
                self._con.execute(
                    f"ALTER TABLE execution_runs ADD COLUMN {name} {dtype}"
                )

        for name, _, comment in _COLUMN_DEFS:
            # DuckDB does NOT accept bound parameters (?) in COMMENT ON
            # COLUMN — escape single quotes the SQL way ('' per ').
            safe = comment.replace("'", "''")
            self._con.execute(
                f"COMMENT ON COLUMN execution_runs.{name} IS '{safe}'"
            )

    def close(self) -> None:
        """Close the DuckDB connection (idempotent).

        The connection is a file handle on real databases (:memory: in
        tests hides it) — the CLI must not leak one per invocation.
        """
        closer = getattr(self._con, "close", None)
        if closer is not None:
            try:
                closer()
            except Exception:
                # DuckDB raises on double close of a closed connection in
                # some versions — idempotency is the contract here.
                pass

    def insert(self, record: ExecutionRecord) -> str:
        """Insert a record and return its ``run_id``."""
        values = (
            record.run_id,
            record.run_at,
            record.execution_type,
            record.source_name,
            record.records_requested,
            record.records_processed,
            record.events_produced,
            record.duration_seconds,
            record.throughput_rps,
            record.raw_bytes,
            record.parquet_bytes,
            record.raw_path,
            record.parquet_path,
            record.checksum,
            record.error_count,
            record.error_message,
            record.host_info,
            record.metadata,
        )
        self._con.execute(
            """
            INSERT OR REPLACE INTO execution_runs VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            values,
        )
        return record.run_id

    def query(self, sql: str, params: tuple | list | None = None) -> duckdb.DuckDBPyRelation:
        """Execute a raw SQL query and return the DuckDB result object."""
        return self._con.execute(sql, params)
