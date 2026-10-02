"""Storage: Analytical storage.

Lightweight analytical queries over Parquet via DuckDB.

Usage::

    analytics = AnalyticalStorage("data/parquet")
    df = analytics.query("SELECT * FROM events WHERE distribution = 'sid'")
    count = analytics.count_events()
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb


class AnalyticalStorage:
    """Provides analytical query access over normalized Parquet datasets.

    Each query creates a fresh DuckDB in-memory connection. This is
    lightweight for R1; persistent DuckDB databases belong to R2+R3.

    Usage::

        storage = AnalyticalStorage("data/parquet")
        df = storage.query("SELECT source, COUNT(*) as n FROM events GROUP BY source")
    """

    def __init__(self, parquet_dir: str | Path) -> None:
        """Initialize analytical storage.

        Args:
            parquet_dir: Directory containing Parquet files.
        """
        self.parquet_dir = Path(parquet_dir)

    def _get_files(self) -> list[str]:
        """Return list of parquet file paths as strings."""
        return [str(p) for p in sorted(self.parquet_dir.glob("*.parquet"))]

    def query(self, sql: str) -> Any:
        """Execute a SQL query over all Parquet files.

        Args:
            sql: DuckDB SQL query. The ``events`` table is available
                 as a view over all Parquet files.

        Returns:
            A DuckDB relation (lazy). Call ``.fetchdf()`` for a pandas
            DataFrame, ``.fetchall()`` for a list of tuples, etc.
        """
        files = self._get_files()
        if not files:
            raise RuntimeError(f"No Parquet files found in {self.parquet_dir}")

        con = duckdb.connect(":memory:")
        # Create a view over all parquet files
        file_list = ", ".join(f"'{f}'" for f in files)
        con.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet([{file_list}])")
        return con.execute(sql)

    def count_events(self) -> int:
        """Return total event count across all files."""
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
        files = self._get_files()
        if not files:
            return {"files": 0, "events": 0, "types": {}}

        counts = self.count_by_event_type()
        return {
            "files": len(files),
            "events": self.count_events(),
            "types": counts,
        }
