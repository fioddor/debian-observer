"""Unit tests for ExecutionStore backed by DuckDB."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from debian_observer.analytics.models import ExecutionRecord
from debian_observer.analytics.store import ExecutionStore


@pytest.fixture
def store():
    """In-memory DuckDB store for testing."""
    return ExecutionStore(":memory:")


def test_init_creates_table(store):
    """Store initialization creates execution_runs table with comments."""
    result = store.query(
        """SELECT column_name, comment
           FROM duckdb_columns()
           WHERE table_name = 'execution_runs'
           ORDER BY column_name"""
    )
    rows = result.fetchall()
    assert len(rows) == 18
    columns_with_comments = {r[0] for r in rows}
    assert "run_id" in columns_with_comments
    assert "run_at" in columns_with_comments
    assert "execution_type" in columns_with_comments
    assert "records_processed" in columns_with_comments
    assert "error_count" in columns_with_comments
    # Every column should have a non-empty comment
    for row in rows:
        assert row[1] is not None and len(row[1]) > 0


def test_insert_returns_run_id(store):
    """insert returns the run_id of the inserted record."""
    record = ExecutionRecord(
        run_id="run-abc",
        run_at=datetime(2026, 10, 29, 10, 0, 0, tzinfo=timezone.utc),
        execution_type="ingestion",
        source_name="benchmark",
        records_processed=42,
        duration_seconds=1.0,
        throughput_rps=42.0,
    )
    run_id = store.insert(record)
    assert run_id == "run-abc"


def test_insert_and_query_roundtrip(store):
    """Inserted records are queryable."""
    record = ExecutionRecord(
        run_id="run-xyz",
        run_at=datetime(2026, 10, 29, 10, 0, 0, tzinfo=timezone.utc),
        execution_type="ingestion",
        source_name="udd",
        records_processed=100,
        events_produced=50,
        duration_seconds=2.5,
        throughput_rps=40.0,
        raw_bytes=1024,
        parquet_bytes=512,
        checksum="sha256-deadbeef",
        error_count=0,
        host_info='{"hostname": "testhost"}',
        metadata='{"batch": 1}',
    )
    store.insert(record)

    result = store.query("SELECT * FROM execution_runs WHERE run_id = 'run-xyz'")
    row = result.fetchone()
    assert row is not None
    # row columns: run_id, run_at, execution_type, source_name, ...
    assert row[0] == "run-xyz"
    assert row[2] == "ingestion"
    assert row[3] == "udd"
    assert row[5] == 100  # records_processed
    assert row[7] == 2.5  # duration_seconds
    assert row[8] == 40.0  # throughput_rps


def test_insert_multiple_records(store):
    """Multiple insertions produce multiple rows."""
    for i in range(3):
        store.insert(
            ExecutionRecord(
                run_id=f"run-{i}",
                run_at=datetime(2026, 10, 29, 10, 0, 0, tzinfo=timezone.utc),
                execution_type="query",
                source_name="mirror",
                records_processed=10 * i,
                duration_seconds=1.0,
                throughput_rps=10.0 * i if i > 0 else None,
            )
        )
    result = store.query("SELECT COUNT(*) FROM execution_runs")
    assert result.fetchone()[0] == 3


def test_query_aggregates(store):
    """Store supports aggregate queries for reporting."""
    for i in range(5):
        store.insert(
            ExecutionRecord(
                run_id=f"run-{i}",
                run_at=datetime(2026, 10, 29, 10, 0, 0, tzinfo=timezone.utc),
                execution_type="ingestion",
                source_name="udd",
                records_processed=100,
                duration_seconds=1.0 + i * 0.1,
                throughput_rps=100.0 / (1.0 + i * 0.1),
            )
        )
    result = store.query(
        """SELECT execution_type, source_name, COUNT(*),
                   quantile_cont(throughput_rps, 0.5)
            FROM execution_runs
            GROUP BY execution_type, source_name"""
    )
    rows = result.fetchall()
    assert len(rows) == 1
    assert rows[0][2] == 5


class TestExecutionStoreMigrations:
    """Behaviour: the store opens databases written by OLDER versions.

    CREATE TABLE IF NOT EXISTS does not add columns: a database with the
    previous schema (fewer columns) used to crash _init_db with a
    BinderException on the first COMMENT ON COLUMN — the
    execution-report CLI could not even open an existing database.
    """

    def test_opens_database_with_legacy_schema(self, tmp_path):
        import duckdb
        from debian_observer.analytics.store import ExecutionStore

        db = tmp_path / "runs.duckdb"
        con = duckdb.connect(str(db))
        # Schema of an OLDER release: 4 columns only
        con.execute(
            "CREATE TABLE execution_runs ("
            " run_id VARCHAR, run_at TIMESTAMP,"
            " execution_type VARCHAR, source_name VARCHAR)"
        )
        con.execute(
            "INSERT INTO execution_runs VALUES ('legacy-1', '2026-09-01 00:00:00',"
            " 'ingestion', 'mirror')"
        )
        con.close()

        # Opening the legacy DB must migrate (ALTER TABLE ADD COLUMN) instead of
        # crashing on the missing columns.
        store = ExecutionStore(db)
        rows = store.query("SELECT COUNT(*) AS n FROM execution_runs").fetchone()
        assert rows[0] == 1, "legacy rows must survive the migration"
        cols = {
            r[0]
            for r in store.query(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'execution_runs'"
            ).fetchall()
        }
        from debian_observer.analytics.store import _COLUMN_DEFS
        missing = {name for name, _, _ in _COLUMN_DEFS} - cols
        assert not missing, f"columns not migrated: {sorted(missing)}"
        store.close()

    def test_comment_with_apostrophe_does_not_crash(self, tmp_path):
        """_COLUMN_DEFS comments are interpolated into COMMENT ON COLUMN
        without escaping: any future edit adding an apostrophe used to
        blow up _init_db with a ParserException."""
        from debian_observer.analytics import store as store_mod

        db = tmp_path / "runs.duckdb"
        # temporarily patch a comment with an apostrophe
        original = store_mod._COLUMN_DEFS
        patched = list(original)
        patched[0] = (patched[0][0], patched[0][1], "it's a test comment")
        store_mod._COLUMN_DEFS = patched
        try:
            es = store_mod.ExecutionStore(db)
            es.close()
        finally:
            store_mod._COLUMN_DEFS = original

    def test_close_is_idempotent(self, tmp_path):
        from debian_observer.analytics.store import ExecutionStore

        store = ExecutionStore(tmp_path / "runs.duckdb")
        store.close()
        store.close()  # must not raise
