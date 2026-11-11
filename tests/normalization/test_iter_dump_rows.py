"""Tests for the COPY row iterator (ADR-015).

``iter_dump_rows`` streams the COPY block of one table through
``pg_restore --data-only`` on a pipe: each line goes out as the
**full row** (all COPY columns, values TAB-split and unescaped) the
moment it is read — the reader yields **before the producer
finishes**. ``read_dump_records`` keeps its signature and
behaviour: the list of mapped records built from the iterator.
"""

from __future__ import annotations

import os
import stat
import textwrap
from pathlib import Path

import pytest

from debian_observer.normalization.udddump import (
    UDDDumpError,
    iter_dump_rows,
    read_dump_records,
)


# A fake pg_restore that emits many lines slowly: if the reader
# waited for the producer to finish, the first row would only
# arrive after the whole stream. Used via PATH override.
FAKE_PG_RESTORE = """\
#!/bin/sh
# Fake pg_restore: ignores args except --table/--file; emits a
# preamble, a COPY block of many rows, and the terminator.
printf -- '-- preamble comment\\n'
printf -- '\\\\restrict search_path\\n'
printf -- "SET statement_timeout = 0;\\n"
printf -- 'COPY public.packages (id, package, version, architecture) FROM stdin;\\n'
i=0
while [ $i -lt 4000 ]; do
    printf -- '%s\\tpkg%s\\t1.0\\tamd64\\n' "$i" "$i"
    i=$((i + 1))
done
printf -- '\\\\.\\n'
"""


@pytest.fixture()
def fake_pg_restore(tmp_path: Path, monkeypatch) -> str:
    """Put a slow fake pg_restore on PATH; return its path."""
    script = tmp_path / "pg_restore"
    script.write_text(textwrap.dedent(FAKE_PG_RESTORE))
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    return str(script)


class TestIterDumpRows:
    """The COPY block as an iterator of full rows."""

    def test_yields_full_rows_with_all_copy_columns(
        self, fake_pg_restore, tmp_path
    ) -> None:
        dump = tmp_path / "fake.dump"
        dump.write_bytes(b"")
        rows = list(iter_dump_rows(str(dump), table="packages"))
        assert len(rows) == 4000
        first = rows[0]
        # The FULL row: every COPY column, by name — not the original
        # record mapping.
        assert first["id"] == "0"
        assert first["package"] == "pkg0"
        assert first["version"] == "1.0"
        assert first["architecture"] == "amd64"

    def test_yields_before_the_producer_finishes(
        self, fake_pg_restore, tmp_path
    ) -> None:
        """The reader is an iterator, not a capture-then-return list.

        Pull ONE row and assert the producer is still alive (its
        stdout pipe is open) — impossible if iter_dump_rows read the
        whole stdout into memory before yielding.
        """
        dump = tmp_path / "fake.dump"
        dump.write_bytes(b"")
        it = iter_dump_rows(str(dump), table="packages")
        first = next(it)
        assert first["package"] == "pkg0"
        # The underlying pg_restore process is still running: its
        # stdout is still open (proc.poll() is None). The iterator
        # did not wait for EOF before the first row.
        proc = it._process  # noqa: SL001 - test hook
        assert proc.poll() is None
        it.close()

    def test_limit_stops_after_n_rows(self, fake_pg_restore, tmp_path) -> None:
        dump = tmp_path / "fake.dump"
        dump.write_bytes(b"")
        rows = list(iter_dump_rows(str(dump), table="packages", limit=10))
        assert len(rows) == 10

    def test_missing_pg_restore_is_a_clear_error(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setenv("PATH", str(tmp_path))  # no pg_restore anywhere
        dump = tmp_path / "fake.dump"
        dump.write_bytes(b"")
        with pytest.raises(UDDDumpError, match="pg_restore not found"):
            list(iter_dump_rows(str(dump), table="packages"))

    def test_no_copy_header_is_an_error(self, tmp_path, monkeypatch) -> None:
        script = tmp_path / "pg_restore"
        script.write_text("#!/bin/sh\nprintf -- '-- no copy block\\n'\n")
        script.chmod(0o755)
        monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
        dump = tmp_path / "fake.dump"
        dump.write_bytes(b"")
        with pytest.raises(UDDDumpError, match="no COPY header"):
            list(iter_dump_rows(str(dump), table="packages"))


class TestReadDumpRecordsKeepsOriginalBehaviour:
    """``read_dump_records`` = the original mapping over the iterator."""

    def test_signature_and_mapping_unchanged(
        self, fake_pg_restore, tmp_path
    ) -> None:
        dump = tmp_path / "fake.dump"
        dump.write_bytes(b"")
        records = read_dump_records(str(dump), table="packages", limit=5)
        assert isinstance(records, list)
        assert len(records) == 5
        rec = records[0]
        # the original record shape, exactly as before the iterator existed.
        assert rec["package"] == "pkg0"
        assert rec["version"] == "1.0"
        assert rec["distribution"] == "unknown"
        assert rec["release"] == "unknown"
        assert rec["architecture"] == "amd64"
        assert rec["component"] == "unknown"
        assert rec["source"] == "udddump"
        assert rec["suite"] is None
        assert rec["observed_at"]