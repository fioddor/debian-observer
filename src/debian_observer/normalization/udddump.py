"""Normalization: UDD ``pg_dump`` file reader.

Reads one table of a UDD ``pg_dump`` custom-format file through
``pg_restore`` **on the host** (PATH): no container indirection and
no database is created or restored (ADR-006).

Moved from ``ingestion/acquisition.py`` (2026-10-23, ADR-012
amendment): the dump file is now a *raw artifact* downloaded by
``ingest udddump``; the reading — the table-of-contents gate, the
COPY-header shape gate and the COPY → records mapping — is
normalization work and lives here. There is no ``UDDDumpDataSource``:
the raw source name is ``udddump`` (stamped by the download, read
back from the manifest sidecar).

This module defines:

- ``UDDDumpError`` — a ``pg_restore`` run over a dump file failed
- ``parse_dump_toc_tables`` / ``list_dump_tables`` — the restorable
  (TABLE DATA) tables of a dump (the TOC gate)
- ``TableShape`` + ``SUPPORTED_TABLE_SHAPES`` + match/assert — the
  ingestible table perimeter (the shape gate)
- ``peek_dump_copy_columns`` — one stdout line of the table's COPY
  header, no data restored (the shape gate's probe)
- ``read_dump_records`` — the table's COPY block as UDD records
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


class UDDDumpError(RuntimeError):
    """A ``pg_restore`` run over a UDD dump file failed."""


def _run_pg_restore(args: list[str]) -> str:
    """Run ``pg_restore`` on the host and return its stdout.

    ``pg_restore`` is a host dependency (PATH) — the dump file is
    read directly; no container indirection and no database is
    created or restored (ADR-006).
    """
    command = ["pg_restore", *args]
    try:
        result = subprocess.run(
            command, capture_output=True, text=True, check=True
        )
    except FileNotFoundError as exc:
        raise UDDDumpError(
            "pg_restore not found on PATH; install postgresql-client"
        ) from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or "").strip().splitlines()
        tail = detail[-1] if detail else f"exit code {exc.returncode}"
        raise UDDDumpError(f"pg_restore failed: {tail}") from exc
    return result.stdout


# Only TABLE DATA entries name restorable tables; schema, sequence and
# index entries are ignored. "<oid>; <db-oid> <table-oid> TABLE DATA
# <schema> <table> [<owner>]".
_TOC_TABLE_DATA_RE = re.compile(
    r"^\s*\d+;\s*\d+\s+\d+\s+TABLE DATA\s+\S+\s+(\S+)",
    re.MULTILINE,
)


def parse_dump_toc_tables(toc_text: str) -> list[str]:
    """Return the restorable table names of a ``pg_restore --list`` TOC.

    Order is TOC order; repeated entries are dropped.
    """
    tables: list[str] = []
    for match in _TOC_TABLE_DATA_RE.finditer(toc_text):
        name = match.group(1)
        if name not in tables:
            tables.append(name)
    return tables


def list_dump_tables(file: str) -> list[str]:
    """List the restorable tables of a UDD pg_dump file (read-only TOC)."""
    return parse_dump_toc_tables(_run_pg_restore(["--list", file]))


# ---------------------------------------------------------------------------
# Table shape perimeter (the normalize udddump startup gate)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TableShape:
    """One ingestible table shape: required COPY columns.

    ``required_columns`` must all be present; ``required_any`` needs
    at least one of them. The `packages` shape mirrors the validator
    (`UDDValidator.REQUIRED_FIELDS` / `PACKAGE_FIELDS`): `version`
    plus any of `package`/`source` — the real `ci` table carries
    `source`+`version` and ingests fine (2026-10-22 real-dump smoke).
    """

    name: str
    required_columns: frozenset[str]
    required_any: frozenset[str] = frozenset()


# The registry drives the startup gate: a new shape is data, not gate
# code (future-ready — today only `packages` is ingestible).
SUPPORTED_TABLE_SHAPES: tuple[TableShape, ...] = (
    TableShape(
        name="packages",
        required_columns=frozenset({"version"}),
        required_any=frozenset({"package", "source"}),
    ),
)


def _render_shape(shape: TableShape) -> str:
    """Render one supported shape for gate error messages."""
    text = f"{shape.name} ({', '.join(sorted(shape.required_columns))}"
    if shape.required_any:
        text += f" + any of: {', '.join(sorted(shape.required_any))}"
    return text + ")"


def match_table_shape(columns: list[str]) -> TableShape | None:
    """Return the registered shape whose requirements ``columns`` carry."""
    for shape in SUPPORTED_TABLE_SHAPES:
        if not shape.required_columns.issubset(columns):
            continue
        if shape.required_any and not shape.required_any.intersection(columns):
            continue
        return shape
    return None


def assert_supported_table_shape(columns: list[str], *, table: str) -> TableShape:
    """Fail fast when a table's COPY header matches no registered shape.

    The error names the cause (the table and its COPY header) and the
    perimeter (the supported shapes and their required columns) — a
    usage message, never a traceback.
    """
    shape = match_table_shape(columns)
    if shape is None:
        supported = ", ".join(_render_shape(s) for s in SUPPORTED_TABLE_SHAPES)
        raise UDDDumpError(
            f"table {table!r} is not shaped like any supported table "
            f"(COPY header columns: {', '.join(columns) or '<none>'}); "
            f"supported shapes: {supported}"
        )
    return shape


def peek_dump_copy_columns(file: str, *, table: str) -> list[str]:
    """Peek the COPY header of a data-only restore: the table's columns.

    Streams ``pg_restore --data-only --table`` and scans past the
    preamble — ``--`` comments, ``\\`` directives and ``SET`` statements
    (pg_restore 17) — to the COPY header, then closes the stream (no
    data restored, ADR-006). This is the shape gate's probe: it costs
    a preamble, not one table.
    """
    command = [
        "pg_restore",
        "--data-only",
        "--table",
        table,
        "--file",
        "-",
        file,
    ]
    try:
        proc = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError as exc:
        raise UDDDumpError(
            "pg_restore not found on PATH; install postgresql-client"
        ) from exc
    try:
        assert proc.stdout is not None
        header = ""
        first_content = ""
        # Real dumps open with a preamble before the COPY block (2026-10-22
        # real-dump smoke): `--` comments, `\restrict` directives and `SET`
        # statements. Scan for the COPY header itself — cap the scan,
        # never restore data — and remember the first content line for
        # the error message when no header shows up.
        for _ in range(256):
            line = proc.stdout.readline()
            if not line:
                break
            stripped = line.strip()
            if not stripped or stripped.startswith(("--", "\\")):
                continue
            if _COPY_HEADER_RE.match(line):
                header = line
                break
            if not first_content:
                first_content = line
    finally:
        proc.terminate()
        proc.wait()
    if not header:
        if first_content:
            raise UDDDumpError(
                f"pg_restore emitted no COPY header for table {table!r} "
                f"(first non-comment line: {first_content.strip()!r})"
            )
        raise UDDDumpError(
            f"pg_restore emitted no COPY header for table {table!r} "
            f"(empty stream — the table may have no data, or the dump "
            "is unreadable)"
        )
    # header is only set on a COPY match — parse its column list.
    match = _COPY_HEADER_RE.match(header)
    assert match is not None
    return [column.strip() for column in match.group("columns").split(",")]


# A COPY block header: "COPY <schema.table> (<columns>) FROM stdin;".
# MULTILINE is a compile flag: .search()'s second argument is pos,
# not flags (bit me 2026-10-21).
_COPY_HEADER_RE = re.compile(
    r"^COPY \S+ \((?P<columns>[^)]*)\) FROM stdin;$", re.MULTILINE
)


def _unescape_copy_field(field: str) -> str | None:
    """Unescape one COPY data field: ``\\N`` is NULL; backslash escapes."""
    if field == r"\N":
        return None
    out: list[str] = []
    i = 0
    while i < len(field):
        char = field[i]
        if char == "\\" and i + 1 < len(field):
            nxt = field[i + 1]
            if nxt == "t":
                out.append("\t")
            elif nxt == "r":
                out.append("\r")
            elif nxt == "n":
                out.append("\n")
            else:
                out.append(nxt)
            i += 2
        else:
            out.append(char)
            i += 1
    return "".join(out)


def _parse_copy_block(
    stdout: str, *, source_name: str, limit: int | None
) -> list[dict[str, Any]]:
    """Map a ``pg_restore --data-only`` COPY block to UDD records.

    Columns are mapped by name from the COPY header — tables that are
    not packages-shaped still map (missing columns read as None and
    fall back to "unknown" like the live UDD source).
    """
    match = _COPY_HEADER_RE.search(stdout)
    if match is None:
        raise UDDDumpError(
            "pg_restore emitted no COPY block for the requested table"
        )
    columns = [c.strip() for c in match.group("columns").split(",")]
    observed_at = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []
    for line in stdout[match.end() :].splitlines():
        if line == r"\.":
            break
        if not line.strip():
            continue
        values = [_unescape_copy_field(field) for field in line.split("\t")]
        row: dict[str, Any] = dict(zip(columns, values))
        records.append(
            {
                "package": row.get("package") or "unknown",
                "source_package": row.get("source"),
                "version": row.get("version") or "unknown",
                "distribution": row.get("distribution") or "unknown",
                "release": row.get("release") or "unknown",
                "suite": None,  # UDD exposes pockets, not suite roles
                "architecture": row.get("architecture") or "unknown",
                "component": row.get("component") or "unknown",
                "section": row.get("section"),
                "priority": row.get("priority"),
                "source": source_name,
                "observed_at": observed_at,
            }
        )
        if limit is not None and len(records) >= limit:
            break
    return records


def read_dump_records(
    file: str, *, table: str, limit: int | None = None
) -> list[dict[str, Any]]:
    """Read one table's records out of a UDD ``pg_dump`` file.

    Streams the table's COPY block from ``pg_restore --data-only
    --table`` on the host (PATH dependency — install
    ``postgresql-client`` if absent; ADR-006: no database is
    created or restored) and maps its rows to the UDD record shape.
    ``--file -`` streams the COPY block to stdout: ``pg_restore``
    requires an explicit output target for data restores — caught by
    the 2026-10-21 real-dump smoke, invisible to unit mocks.

    Records are stamped with the raw source name ``udddump`` (the
    manifest sidecar written by ``ingest udddump`` carries the same
    name); ``limit`` truncates after that many records.
    """
    stdout = _run_pg_restore(
        [
            "--data-only",
            "--table",
            table,
            "--file",
            "-",
            file,
        ]
    )
    return _parse_copy_block(stdout, source_name="udddump", limit=limit)