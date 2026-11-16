"""Storage: Analytical storage.

Lightweight analytical queries over Parquet via DuckDB.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import duckdb


class AnalyticalStorage:
    """Provides analytical query access over normalized Parquet datasets.

    Each query creates a fresh DuckDB in-memory connection; a
    persistent DuckDB database would only pay off once queries
    outlive a single process.
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
                A dataset directory is ONE table: its files are the
                writer's business and are registered as one view per
                schema found, named from the directory (never from a
                file name — see the ``cli`` contract in the design
                docs).
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

    @staticmethod
    def _quoted(name: str) -> str:
        """Return ``name`` as a quoted SQL identifier.

        Dataset stems (``run-001.parquet``) carry hyphens, which are
        not legal inside a bare SQL identifier; an unquoted CREATE
        VIEW dies in the parser. Doubling embedded quotes is the SQL
        escape.
        """
        return '"' + name.replace('"', '""') + '"'

    def _dataset_views(self) -> dict[str, list[Path]]:
        """Group the dataset files into views: one per schema.

        A dataset directory is ONE table (the ``cli`` contract in the
        design docs): its view is named from the directory, never from
        a file name. When every file shares one schema the view carries
        the directory's name; when N schemas mix, one view is
        registered per schema as ``<directory>__<suffix>``, where
        ``<suffix>`` is a short stable digest of the schema (column
        names and types, in file order) — deterministic across runs,
        never a file name. ``describe`` builds its names from this same
        helper, so what it lists is exactly what goes in FROM.
        """
        import hashlib

        import pyarrow.parquet as pq

        files = self._get_dataset_files()
        if not files:
            return {}
        groups: dict[tuple[str, ...], list[Path]] = {}
        for f in files:
            schema = pq.ParquetFile(f).schema_arrow
            key = tuple(
                (str(field.name), str(field.type)) for field in schema
            )
            groups.setdefault(key, []).append(f)
        directory = self.dataset_dir.name
        if len(groups) == 1:
            return {directory: files}
        views: dict[str, list[Path]] = {}
        for key, members in groups.items():
            digest = hashlib.sha256("\n".join(
                f"{n}\u0000{ty}" for n, ty in key
            ).encode()).hexdigest()[:8]
            views[f"{directory}__{digest}"] = members
        return views

    def _register_views(self, con: duckdb.DuckDBPyConnection) -> None:
        """Register ``events`` and per-directory dataset views in DuckDB.

        The ``events`` view is only created when event-log files
        exist; a missing event log is not an error here — queries
        and describes over datasets alone must reach the database.
        Raising on a missing event log belongs to ``query`` when
        the SQL actually references ``events``.
        """
        event_files = self._get_event_files()
        if event_files:
            file_list = ", ".join(f"'{f}'" for f in event_files)
            con.execute(
                f"CREATE VIEW events AS SELECT * FROM "
                f"read_parquet([{file_list}], union_by_name=true)"
            )
        for view_name, files in self._dataset_views().items():
            file_list = ", ".join(f"'{f}'" for f in files)
            con.execute(
                f"CREATE VIEW {self._quoted(view_name)} AS SELECT * FROM "
                f"read_parquet([{file_list}])"
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

        Raises:
            RuntimeError: the SQL references ``events`` but the
                events directory holds no Parquet files. Dataset-only
                queries never raise for the event log.
        """
        con = duckdb.connect(":memory:")
        self._register_views(con)
        event_files = self._get_event_files()
        if not event_files and self._references_events(sql):
            raise RuntimeError(
                f"No Parquet files found in {self.parquet_dir} — "
                "ingest first or point --events-dir at the event log"
            )
        return con.execute(sql)

    @staticmethod
    def _references_events(sql: str) -> bool:
        """True when the SQL mentions the ``events`` view.

        Word-boundary match so ``package_events`` or a string literal
        like 'events_dir' do not count. Conservative by design: when
        in doubt we treat the SQL as event-referencing so the
        interpreter's own error surfaces instead of a silent pass.
        """
        return re.search(r"\bevents\b", sql) is not None

    def describe(self) -> dict[str, list[tuple[str, str]]]:
        """Return schema info for every registered table/view.

        ``events`` is only listed when event-log files exist;
        a describe never requires the event log. Dataset views
        (one per ``*.parquet`` in ``dataset_dir``) are always
        listed when their files exist.

        Returns:
            Mapping ``table_name -> [(column_name, duckdb_type), ...]``.
        """
        con = duckdb.connect(":memory:")
        self._register_views(con)
        # same naming rule as _register_views — by construction,
        # not by coincidence
        tables = list(self._dataset_views())
        if self._get_event_files():
            tables = ["events"] + tables
        result: dict[str, list[tuple[str, str]]] = {}
        for table in tables:
            try:
                rel = con.execute(
                    f"SELECT * FROM {self._quoted(table)} LIMIT 0"
                )
                result[table] = [(desc[0], desc[1]) for desc in (rel.description or [])]
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
