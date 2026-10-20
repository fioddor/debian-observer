"""Raw artifact serialization: xz-compressed CSV (ADR-002).

Raw records are flat maps of strings (and ``None``). The raw
artifact stores them as RFC-4180 CSV compressed with LZMA
(``.csv.xz``), written with the standard library only.

``None`` and the empty string are conflated: ``None`` is written
as an empty field and read back as ``None``. The sources emit no
empty-string values, so the conflation is lossless (ADR-002).
"""

from __future__ import annotations

import csv
import hashlib
import json
import lzma
from pathlib import Path
from typing import Any

#: Suffix of every raw artifact written by the pipeline.
RAW_SUFFIX = ".csv.xz"

#: LZMA preset: maximum compression; raw is written once per run
#: and read almost never (ADR-002).
_COMPRESSION_PRESET = 9

_XZ_SUFFIXES = (".csv.xz", ".xz")
_JSONL_SUFFIX = ".jsonl"


def raw_field_names(records: list[dict[str, Any]]) -> list[str]:
    """Union of the record keys, in first-seen order."""
    fields: list[str] = []
    for record in records:
        for key in record:
            if key not in fields:
                fields.append(key)
    return fields


def write_raw_artifact(
    records: list[dict[str, Any]],
    artifact_path: Path,
) -> None:
    """Write records as one xz-compressed CSV raw artifact."""
    fields = raw_field_names(records)
    with lzma.open(
        artifact_path,
        "wt",
        preset=_COMPRESSION_PRESET,
        encoding="utf-8",
        newline="",
    ) as raw_file:
        writer = csv.writer(raw_file)
        writer.writerow(fields)
        for record in records:
            writer.writerow(
                [
                    "" if record.get(field) is None else record.get(field)
                    for field in fields
                ]
            )


def read_raw_artifact(path: Path) -> list[dict[str, Any]]:
    """Read a raw artifact into records.

    Handles the current ``.csv.xz`` format and legacy ``.jsonl``
    artifacts (ADR-002: legacy raw files remain readable).
    """
    path = Path(path)
    name = path.name
    if name.endswith(_XZ_SUFFIXES):
        return _read_csv_xz(path)
    if name.endswith(_JSONL_SUFFIX):
        return _read_jsonl(path)
    raise ValueError(f"Unknown raw artifact format: {name}")


def sha256_raw(path: Path) -> str:
    """SHA-256 of the raw artifact bytes as stored (compressed)."""
    digest = hashlib.sha256()
    with open(path, "rb") as raw_file:
        for chunk in iter(lambda: raw_file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_raw_input(input_file: Any) -> list[dict[str, Any]]:
    """Read raw records from a CLI input stream or file.

    JSON lines (stdin or ``.jsonl``) or an xz-compressed CSV
    artifact path (``.csv.xz``, ADR-002).
    """
    name = getattr(input_file, "name", "") or ""
    if name.endswith(_XZ_SUFFIXES):
        return read_raw_artifact(Path(name))
    return [json.loads(line) for line in input_file if line.strip()]


def _read_csv_xz(path: Path) -> list[dict[str, Any]]:
    """Read an xz-compressed CSV raw artifact."""
    with lzma.open(path, "rt", encoding="utf-8", newline="") as raw_file:
        reader = csv.DictReader(raw_file)
        return [
            {
                field: (None if row.get(field) == "" else row.get(field))
                for field in reader.fieldnames or []
            }
            for row in reader
        ]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read a legacy JSON-lines raw artifact."""
    with open(path, encoding="utf-8") as raw_file:
        return [
            json.loads(line)
            for line in raw_file
            if line.strip()
        ]