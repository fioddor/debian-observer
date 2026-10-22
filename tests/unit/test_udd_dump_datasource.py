"""Tests for the UDD ``pg_dump`` file source (`ingest udddump`).

The source ingests one table of a UDD ``pg_dump`` custom-format file
through ``pg_restore`` **on the host** (PATH): no container, no live
database (ADR-006). The table is validated against the dump's table
of contents at startup, and rows stream from ``pg_restore
--data-only`` COPY blocks.
"""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from debian_observer.ingestion import acquisition
from debian_observer.ingestion.acquisition import (
    UDDDumpDataSource,
    UDDDumpError,
    list_dump_tables,
    parse_dump_toc_tables,
)


TOC_SAMPLE = """\
;
; Archive created at: Mon Oct  2 18:54:41 2026
;     CREATE DATABASE udd ...
;
\\connect - udd

3254; 1259 16388 TABLE public packages udd
3255; 1259 16401 TABLE public sources udd
...
3300; 0 16388 TABLE DATA public packages udd
3301; 0 16401 TABLE DATA public sources udd
3302; 0 16409 TABLE DATA public upload_history udd
3303; 1255 16528 SEQUENCE public packages_id_seq udd
3304; 1259 16529 INDEX public packages_pkey udd
"""

# A pg_restore --data-only stream: one COPY block for the table.
# Fields are TAB-separated; \N is NULL.
COPY_SAMPLE = (
    "COPY public.packages (id, package, source, version, distribution, "
    "release, architecture, section, priority, component) FROM stdin;\n"
    "1\tlibrte-dpdk\tdpdk\t25.11.3-1\tdebian\tbullseye-security\t"
    "amd64\tlibs\toptional\tmain\n"
    "2\tlibhello\thello\t1.0\tdebian\ttrixie\tarm64\tlibs\t\\N\tmain\n"
    "\\.\n"
)


class TestParseDumpTocTables:
    """The TOC parser yields the restorable (TABLE DATA) table names."""

    def test_extracts_table_data_names_in_toc_order(self) -> None:
        assert parse_dump_toc_tables(TOC_SAMPLE) == [
            "packages",
            "sources",
            "upload_history",
        ]

    def test_ignores_non_data_entries(self) -> None:
        tables = parse_dump_toc_tables(TOC_SAMPLE)

        # TABLE (schema), SEQUENCE and INDEX entries are not restorable
        # table data; only TABLE DATA names are listed.
        assert "packages_id_seq" not in tables
        assert "packages_pkey" not in tables

    def test_empty_toc_yields_no_tables(self) -> None:
        assert parse_dump_toc_tables("") == []

    def test_deduplicates_repeated_entries(self) -> None:
        text = (
            "3300; 0 16388 TABLE DATA public packages udd\n"
            "3300; 0 16388 TABLE DATA public packages udd\n"
            "3301; 0 16401 TABLE DATA public sources udd\n"
        )

        assert parse_dump_toc_tables(text) == ["packages", "sources"]


DUMP = "/srv/udd/udd.dump"  # host path — pg_restore runs locally


class TestListDumpTables:
    """``list_dump_tables`` runs ``pg_restore --list`` on the host."""

    def test_lists_tables_from_the_toc(self) -> None:
        with patch(
            "debian_observer.ingestion.acquisition._run_pg_restore",
            return_value=TOC_SAMPLE,
        ) as run:
            tables = list_dump_tables(DUMP)

        assert "packages" in tables
        assert "sources" in tables
        run.assert_called_once_with(["--list", DUMP])

    def test_dump_path_passes_through_verbatim(self) -> None:
        with patch(
            "debian_observer.ingestion.acquisition._run_pg_restore",
            return_value=TOC_SAMPLE,
        ) as run:
            list_dump_tables(DUMP)

        # no container translation, no docker exec — the host path IS the path
        args = run.call_args[0][0]
        assert args == ["--list", DUMP]
        assert not run.call_args.kwargs


class TestRunPgRestore:
    """The runner shells out to host ``pg_restore`` and wraps failures."""

    def test_invokes_pg_restore_directly(self) -> None:
        """No docker, no container, no user indirection — host PATH."""
        with patch.object(acquisition.subprocess, "run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0, stdout="TOC text", stderr=""
            )
            out = acquisition._run_pg_restore(
                ["--list", "/srv/udd/udd.dump"]
            )

        assert out == "TOC text"
        mock_run.assert_called_once_with(
            ["pg_restore", "--list", "/srv/udd/udd.dump"],
            capture_output=True,
            text=True,
            check=True,
        )

    def test_wraps_failures_in_udd_dump_error(self) -> None:
        # check=True inside subprocess.run raises CalledProcessError; a
        # plain return_value mock would never exercise the wrap.
        error = subprocess.CalledProcessError(
            returncode=1,
            cmd=["pg_restore", "--list", "/nope/udd.dump"],
            output="",
            stderr="pg_restore: [tar archiver] unrecognized section\n",
        )
        with patch.object(acquisition.subprocess, "run", side_effect=error):
            with pytest.raises(UDDDumpError) as excinfo:
                acquisition._run_pg_restore(["--list", "/nope/udd.dump"])

        assert "pg_restore" in str(excinfo.value)

    def test_wraps_missing_pg_restore_with_install_hint(self) -> None:
        """Absent pg_restore is a usage error that names the fix."""
        with patch.object(
            acquisition.subprocess, "run", side_effect=FileNotFoundError("pg_restore")
        ):
            with pytest.raises(UDDDumpError) as excinfo:
                acquisition._run_pg_restore(
                    ["--list", "/srv/udd/udd.dump"]
                )

        assert "postgresql-client" in str(excinfo.value)


class TestUDDDumpDataSource:
    """The dump source maps COPY rows to the UDD record shape."""

    def _source(self, **kwargs) -> UDDDumpDataSource:
        return UDDDumpDataSource(
            file="/srv/udd/udd.dump", table="packages", **kwargs
        )

    def test_name_is_udd_dump(self) -> None:
        assert self._source().name == "udd.dump"

    def test_retrieve_maps_copy_columns_by_name(self) -> None:
        with patch(
            "debian_observer.ingestion.acquisition._run_pg_restore",
            return_value=COPY_SAMPLE,
        ):
            records = self._source().retrieve()

        assert len(records) == 2
        first = records[0]
        assert first["package"] == "librte-dpdk"
        assert first["source_package"] == "dpdk"
        assert first["version"] == "25.11.3-1"
        assert first["distribution"] == "debian"
        assert first["release"] == "bullseye-security"
        assert first["architecture"] == "amd64"
        assert first["component"] == "main"
        assert first["section"] == "libs"
        assert first["priority"] == "optional"
        assert first["suite"] is None
        assert first["source"] == "udd.dump"
        assert first["observed_at"]

    def test_retrieve_maps_missing_values_to_none(self) -> None:
        with patch(
            "debian_observer.ingestion.acquisition._run_pg_restore",
            return_value=COPY_SAMPLE,
        ):
            records = self._source().retrieve()

        assert records[1]["priority"] is None

    def test_retrieve_limit_stops_early(self) -> None:
        with patch(
            "debian_observer.ingestion.acquisition._run_pg_restore",
            return_value=COPY_SAMPLE,
        ):
            records = self._source(limit=1).retrieve()

        assert len(records) == 1

    def test_retrieve_passes_the_table_flag(self) -> None:
        with patch(
            "debian_observer.ingestion.acquisition._run_pg_restore",
            return_value=COPY_SAMPLE,
        ) as run:
            self._source(limit=2).retrieve()

        # host pg_restore: no container kwargs, no docker indirection
        assert not run.call_args.kwargs
        args = run.call_args[0][0]
        assert args[:3] == ["--data-only", "--table", "packages"]
        # pg_restore demands an explicit output target for data restores;
        # --file - streams the COPY block to stdout (regression pin,
        # caught by the 2026-10-21 real-dump smoke).
        assert args[3:5] == ["--file", "-"]
        assert args[-1] == "/srv/udd/udd.dump"

    def test_retrieve_wraps_runner_failures(self) -> None:
        with patch(
            "debian_observer.ingestion.acquisition._run_pg_restore",
            side_effect=UDDDumpError("pg_restore: error"),
        ):
            with pytest.raises(UDDDumpError):
                self._source().retrieve()

    def test_retrieve_without_a_copy_block_is_an_error(self) -> None:
        with patch(
            "debian_observer.ingestion.acquisition._run_pg_restore",
            return_value="no COPY block here\n",
        ):
            with pytest.raises(UDDDumpError):
                self._source().retrieve()

    def test_compute_checksum_is_deterministic(self) -> None:
        with patch(
            "debian_observer.ingestion.acquisition._run_pg_restore",
            return_value=COPY_SAMPLE,
        ):
            source = self._source()
            first = source.compute_checksum()
            second = source.compute_checksum()

        assert first == second
        assert len(first) == 64


class TestTableShapes:
    """The shape registry drives the startup gate (ADR-012 perimeter).

    Today the registry carries the `packages` shape only; the gate
    must stay future-ready: adding a shape is data, not gate code.
    """

    def test_registry_pins_the_packages_shape(self) -> None:
        registry = acquisition.SUPPORTED_TABLE_SHAPES
        assert isinstance(registry, tuple)
        assert [shape.name for shape in registry] == ["packages"]
        assert registry[0].required_columns == frozenset({"package", "version"})

    def test_match_accepts_packages_shaped_columns(self) -> None:
        shape = acquisition.match_table_shape(
            ["id", "package", "version", "architecture", "component"]
        )
        assert shape is not None
        assert shape.name == "packages"

    def test_match_rejects_columns_without_the_requirements(self) -> None:
        assert acquisition.match_table_shape(["source", "count"]) is None

    def test_match_is_registry_driven_not_hardcoded(self) -> None:
        """Future-ready: a registered shape matches without new gate code."""
        releases = acquisition.TableShape(
            name="releases", required_columns=frozenset({"source", "version"})
        )
        with patch.object(acquisition, "SUPPORTED_TABLE_SHAPES", (releases,)):
            matched = acquisition.match_table_shape(["source", "version"])
            assert matched is not None
            assert matched.name == "releases"
            # and the old shape is gone with the registry, not hardcoded
            assert acquisition.match_table_shape(["package", "version"]) is None
            assert (
                acquisition.assert_supported_table_shape(
                    ["source", "version"], table="releases"
                ).name
                == "releases"
            )

    def test_assert_returns_the_matched_shape(self) -> None:
        shape = acquisition.assert_supported_table_shape(
            ["package", "version"], table="ci"
        )
        assert shape.name == "packages"

    def test_assert_error_names_cause_and_perimeter(self) -> None:
        with pytest.raises(UDDDumpError) as excinfo:
            acquisition.assert_supported_table_shape(
                ["source", "count"], table="sources_count"
            )
        message = str(excinfo.value)
        # the cause: the table and its COPY header
        assert "sources_count" in message
        assert "source, count" in message
        assert "not shaped like any supported table" in message
        # the perimeter: the supported shapes and their requirements
        assert "packages" in message
        assert "package, version" in message


class TestPeekDumpCopyColumns:
    """The shape gate peeks one stdout line; no data is restored."""

    def test_peek_reads_the_header_columns(self) -> None:
        proc = MagicMock()
        proc.stdout.readline.return_value = (
            "COPY public.ci (package, version, architecture) FROM stdin;\n"
        )
        with patch.object(acquisition.subprocess, "Popen", return_value=proc):
            columns = acquisition.peek_dump_copy_columns(
                "/srv/udd/udd.dump", table="ci"
            )
        assert columns == ["package", "version", "architecture"]
        proc.terminate.assert_called_once()
        proc.wait.assert_called_once()

    def test_peek_wraps_a_missing_runner(self) -> None:
        with patch.object(
            acquisition.subprocess,
            "Popen",
            side_effect=FileNotFoundError("pg_restore"),
        ):
            with pytest.raises(UDDDumpError) as excinfo:
                acquisition.peek_dump_copy_columns(
                    "/srv/udd/udd.dump", table="ci"
                )
        assert "postgresql-client" in str(excinfo.value)

    def test_peek_fails_when_no_header_is_emitted(self) -> None:
        proc = MagicMock()
        proc.stdout.readline.return_value = ""
        with patch.object(acquisition.subprocess, "Popen", return_value=proc):
            with pytest.raises(UDDDumpError) as excinfo:
                acquisition.peek_dump_copy_columns(
                    "/srv/udd/udd.dump", table="ci"
                )
        assert "COPY header" in str(excinfo.value)
        assert "ci" in str(excinfo.value)
