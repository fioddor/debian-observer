"""``raw_bytes``/``parquet_bytes`` are BIGINT columns (64-bit byte counts).

The real UDD dump artifact is 2,862,207,445 bytes — larger than
INT32 max (2,147,483,647). The first full-table ``normalize udddump``
run (2026-11-08, E1-preview) crashed recording its own execution
record: ``Conversion Error: Type INT64 ... out of range for the
destination type INT32``. Byte-count columns are BIGINT, and an
existing INTEGER database migrates in place.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from debian_observer.analytics.models import ExecutionRecord
from debian_observer.analytics.store import ExecutionStore

OVER_INT32 = 2_862_207_445  # the real dump size, > 2**31 - 1


def _record(**overrides) -> ExecutionRecord:
    base = dict(
        run_id="norm-bigint",
        run_at=datetime.now(timezone.utc),
        execution_type="normalization",
        source_name="udddump",
        records_processed=1_497_724,
        events_produced=1_497_724,
        duration_seconds=300.0,
        raw_bytes=OVER_INT32,
        raw_path="/dumps/udd.dump",
        error_count=0,
    )
    base.update(overrides)
    return ExecutionRecord(**base)


class TestRawBytesBigint:
    def test_insert_accepts_over_int32(self, tmp_path: Path) -> None:
        store = ExecutionStore(tmp_path / "runs.duckdb")
        store.insert(_record())
        row = store.query(
            "SELECT raw_bytes FROM execution_runs WHERE run_id = ?",
            ["norm-bigint"],
        ).fetchone()
        assert row[0] == OVER_INT32
        store.close()

    def test_existing_int32_database_migrates_in_place(
        self, tmp_path: Path
    ) -> None:
        # An older database wrote INTEGER columns: migration
        # ALTERs the type before the insert lands.
        db = tmp_path / "legacy.duckdb"
        store = ExecutionStore(db)
        store.insert(_record(raw_bytes=1000))
        store.close()
        # Widen it by hand to the legacy INTEGER type, then reopen:
        import duckdb

        con = duckdb.connect(str(db))
        con.execute(
            "ALTER TABLE execution_runs ALTER COLUMN raw_bytes"
            " SET DATA TYPE INTEGER"
        )
        con.execute(
            "ALTER TABLE execution_runs ALTER COLUMN parquet_bytes"
            " SET DATA TYPE INTEGER"
        )
        con.close()
        store = ExecutionStore(db)
        store.insert(_record())  # must not raise ConversionException
        row = store.query(
            "SELECT raw_bytes FROM execution_runs WHERE run_id = ?",
            ["norm-bigint"],
        ).fetchone()
        assert row[0] == OVER_INT32
        store.close()