"""Storage: Analytical storage.

Lightweight analytical queries over Parquet via DuckDB.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb


class AnalyticalStorage:
    """Provides analytical query access over normalized Parquet datasets.

    Each query creates a fresh DuckDB in-memory connection. This is
    lightweight for R1; persistent DuckDB databases belong to R2+R3.
    """

    def __init__(
        self,
        parquet_dir: str | Path,
        dataset_dir: str | Path | None = None,
    ) -> None:
        """Initialize analytical storage.

        Args:
            parquet_dir: Directory containing event-log Parquet files.
            dataset_dir: Optional directory containing consolidated
                dataset Parquet files (e.g. package_versions.parquet).
                Each ``*.parquet`` file is registered as a view named
                after its stem, so future prosumers are queryable
                without code changes.
        """
        self.parquet_dir = Path(parquet_dir)
        self.dataset_dir = Path(dataset_dir) if dataset_dir else None

    def _get_event_files(self) -> list[str]:
        """Return list of event-log parquet file paths as strings."""
        return [str(p) for p in sorted(self.parquet_dir.glob("*.parquet"))]

    def _get_dataset_files(self) -> list[Path]:
        """Return list of dataset parquet file paths."""
        if not self.dataset_dir or not self.dataset_dir.exists():
            return []
        return list(sorted(self.dataset_dir.glob("*.parquet")))

    def _register_views(self, con: duckdb.DuckDBPyConnection) -> None:
        """Register ``events`` and per-file dataset views in DuckDB."""
        event_files = self._get_event_files()
        if not event_files:
            raise RuntimeError(
                f"No Parquet files found in {self.parquet_dir} — "
                "ingest first or point --events-dir at the event log"
            )
        if event_files:
            file_list = ", ".join(f"'{f}'" for f in event_files)
            con.execute(
                f"CREATE VIEW events AS SELECT * FROM "
                f"read_parquet([{file_list}], union_by_name=true)"
            )
        for p in self._get_dataset_files():
            view_name = p.stem
            con.execute(
                f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet('{p}')"
            )

    def query(self, sql: str) -> Any:
        """Execute a SQL query over registered views.

        Args:
            sql: DuckDB SQL query. The ``events`` view is available
                 when event-log files exist. Dataset views (one per
                 ``*.parquet`` in ``dataset_dir``) are also available
                 when the corresponding files exist.

        Returns:
            A DuckDB relation (lazy). Call ``.fetchdf()`` for a pandas
            DataFrame, ``.fetchall()`` for a list of tuples, etc.
        """
        con = duckdb.connect(":memory:")
        self._register_views(con)
        return con.execute(sql)

    def describe(self) -> dict[str, list[tuple[str, str]]]:
        """Return schema info for every registered table/view.

        Returns:
            Mapping ``table_name -> [(column_name, duckdb_type), ...]``.
        """
        con = duckdb.connect(":memory:")
        self._register_views(con)
        tables = ["events"] + [p.stem for p in self._get_dataset_files()]
        result: dict[str, list[tuple[str, str]]] = {}
        for table in tables:
            try:
                rel = con.execute(f"SELECT * FROM {table} LIMIT 0")
                result[table] = [
                    (desc[0], desc[1]) for desc in (rel.description or [])
                ]
            except Exception:
                pass
        return result

    def count_events(self) -> int:
        """Return total event count across all event-log files."""
        result = self.query("SELECT COUNT(*) as n FROM events").fetchone()
        return result[0] if result else 0

    def count_by_event_type(self) -> dict[str, int]:
        """Return event counts grouped by event_type."""
        rows = self.query(
            "SELECT event_type, COUNT(*) as n FROM events GROUP BY event_type"
        ).fetchall()
        return {row[0]: row[1] for row in rows}

    def summary(self) -> dict[str, Any]:
        """Return a lightweight summary of stored data."""
        files = self._get_event_files()
        if not files:
            return {"files": 0, "events": 0, "types": {}}

        counts = self.count_by_event_type()
        return {
            "files": len(files),
            "events": self.count_events(),
            "types": counts,
        }
