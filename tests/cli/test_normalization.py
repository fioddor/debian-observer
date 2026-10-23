"""CLI tests for validate/normalize over raw artifacts (ADR-012).

`normalize` is a command group with one subcommand per raw surface
(ADR-012 amendment, 2026-10-23): `artifacts` — pipeline `.csv.xz`
artifacts (positional; source name and run id are read from each
artifact's manifest sidecar, as written by ingest); `udddump` — UDD
`pg_dump` download artifacts (gates + eventization, filters
`--table`/`--limit`).
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from debian_observer.cli.main import cli
from debian_observer.ingestion.raw import write_raw_artifact


def _write_artifact(
    tmp_path: Path,
    *,
    source: str = "udd",
    run_id: str = "run-test-001",
    records: list[dict] | None = None,
) -> Path:
    """Write a raw artifact plus its manifest sidecar, as ingest does."""
    if records is None:
        records = [
            {"package": "vim", "version": "1.0-1", "source": source},
            {"package": "awk", "version": "1:5.2-1", "source": source},
        ]
    artifact = tmp_path / f"{source}-20261021-000000-{run_id}.csv.xz"
    write_raw_artifact(records, artifact)
    manifest = artifact.with_suffix(".manifest.json")
    manifest.write_text(
        json.dumps(
            {
                "dataset_name": f"{source}-{run_id}",
                "source": source,
                "source_version": None,
                "retrieved_at": "2026-10-21T00:00:00+00:00",
                "checksum": "sha256-test",
                "size_bytes": 123,
                "record_count": len(records),
                "license": None,
            }
        ),
        encoding="utf-8",
    )
    return artifact


def test_normalize_help_lists_both_surfaces():
    """`normalize` is a group: --help lists one subcommand per raw
    surface (artifacts + udddump, ADR-012 amendment, 2026-10-23)."""
    runner = CliRunner()
    result = runner.invoke(cli, ["normalize", "--help"])
    assert result.exit_code == 0
    for surface in ("artifacts", "udddump"):
        assert surface in result.output


def test_validate_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--help"])
    assert result.exit_code == 0


def test_validate_artifact(tmp_path):
    """Validate counts valid/invalid per artifact, exit 0 (report, not gate)."""
    runner = CliRunner()
    artifact = _write_artifact(tmp_path)
    result = runner.invoke(cli, ["validate", str(artifact)])
    assert result.exit_code == 0
    assert "Valid: 2, Invalid: 0" in result.output


def test_validate_multiple_artifacts(tmp_path):
    runner = CliRunner()
    artifact_a = _write_artifact(tmp_path, run_id="run-a")
    artifact_b = _write_artifact(
        tmp_path,
        run_id="run-b",
        records=[{"package": "vim", "version": "1.0-1", "source": "udd"}],
    )
    result = runner.invoke(cli, ["validate", str(artifact_a), str(artifact_b)])
    assert result.exit_code == 0
    assert "Valid: 2, Invalid: 0" in result.output
    assert "Valid: 1, Invalid: 0" in result.output


def test_validate_some_invalid(tmp_path):
    runner = CliRunner()
    artifact = _write_artifact(
        tmp_path,
        records=[
            {"package": "vim", "version": "1.0-1", "source": "udd"},
            {"package": "bad", "source": "udd"},
        ],
    )
    result = runner.invoke(cli, ["validate", str(artifact)])
    assert result.exit_code == 0
    assert "Valid: 1, Invalid: 1" in result.output


def test_validate_missing_manifest(tmp_path):
    """An artifact without a manifest sidecar is a clear error, not a traceback."""
    from debian_observer.ingestion.raw import write_raw_artifact

    runner = CliRunner()
    artifact = tmp_path / "udd-20261021-000000-run-x.csv.xz"
    write_raw_artifact([{"package": "vim", "version": "1.0"}], artifact)
    result = runner.invoke(cli, ["validate", str(artifact)])
    assert result.exit_code != 0
    assert "manifest" in result.output
    assert "Traceback" not in result.output


def test_normalize_artifact(tmp_path):
    runner = CliRunner()
    artifact = _write_artifact(tmp_path)
    result = runner.invoke(cli, ["normalize", "artifacts", str(artifact)])
    assert result.exit_code == 0
    lines = [l for l in result.output.strip().split("\n") if l.startswith("{")]
    assert len(lines) == 2
    event = json.loads(lines[0])
    assert event["event_type"] == "PackageVersionObserved"


def test_normalize_stamps_manifest_source_and_run_id(tmp_path):
    """The source and run id stamped on events come from the manifest."""
    runner = CliRunner()
    artifact = _write_artifact(tmp_path, source="debian.mirror", run_id="run-cli-9")
    result = runner.invoke(cli, ["normalize", "artifacts", str(artifact)])
    assert result.exit_code == 0
    event = json.loads(
        [l for l in result.output.strip().split("\n") if l.startswith("{")][0]
    )
    assert event["ingestion_run_id"] == "run-cli-9"


def test_normalize_multiple_artifacts(tmp_path):
    runner = CliRunner()
    artifact_a = _write_artifact(
        tmp_path,
        run_id="run-a",
        records=[{"package": "vim", "version": "1.0-1", "source": "udd"}],
    )
    artifact_b = _write_artifact(
        tmp_path,
        run_id="run-b",
        records=[{"package": "awk", "version": "1:5.2-1", "source": "udd"}],
    )
    result = runner.invoke(
        cli, ["normalize", "artifacts", str(artifact_a), str(artifact_b)]
    )
    assert result.exit_code == 0
    lines = [l for l in result.output.strip().split("\n") if l.startswith("{")]
    assert len(lines) == 2
    run_ids = {json.loads(l)["ingestion_run_id"] for l in lines}
    assert run_ids == {"run-a", "run-b"}


def test_normalize_invalid_record_dropped(tmp_path):
    runner = CliRunner()
    artifact = _write_artifact(
        tmp_path,
        records=[{"package": "vim", "source": "udd"}],
    )
    result = runner.invoke(cli, ["normalize", "artifacts", str(artifact)])
    assert result.exit_code == 0
    lines = [l for l in result.output.strip().split("\n") if l.startswith("{")]
    assert len(lines) == 0
    assert "1 invalid" in result.output


def test_normalize_writes_invalid_file(tmp_path):
    runner = CliRunner()
    artifact = _write_artifact(
        tmp_path,
        records=[
            {"package": "vim", "version": "1.0-1", "source": "udd"},
            {"package": "bad", "source": "udd"},
        ],
    )
    invalid_path = tmp_path / "invalid.jsonl"
    result = runner.invoke(
        cli,
        ["normalize", "artifacts", str(artifact), "--invalid", str(invalid_path)],
    )
    assert result.exit_code == 0
    invalid_lines = [
        l for l in invalid_path.read_text(encoding="utf-8").splitlines() if l.strip()
    ]
    assert len(invalid_lines) == 1
    assert json.loads(invalid_lines[0])["package"] == "bad"


def test_normalize_missing_manifest(tmp_path):
    from debian_observer.ingestion.raw import write_raw_artifact

    runner = CliRunner()
    artifact = tmp_path / "udd-20261021-000000-run-y.csv.xz"
    write_raw_artifact([{"package": "vim", "version": "1.0"}], artifact)
    result = runner.invoke(cli, ["normalize", "artifacts", str(artifact)])
    assert result.exit_code != 0
    assert "manifest" in result.output
    assert "Traceback" not in result.output


def test_validate_requires_an_artifact():
    """No positional artifact is a usage error."""
    runner = CliRunner()
    result = runner.invoke(cli, ["validate"])
    assert result.exit_code == 2


def _write_dump_artifact(
    tmp_path: Path,
    *,
    source: str = "udddump",
    run_id: str = "run-dump-001",
    payload: bytes = b"PGDMP-fake-dump\x00\x01",
) -> Path:
    """Write a downloaded udddump raw artifact + manifest sidecar.

    The surface `ingest udddump --url …` writes: the dump bytes
    byte-preserving and the manifest sidecar beside them (source
    `udddump`, dataset_name `udddump-<run-id>`, record_count null —
    unknown until this normalization).
    """
    artifact = tmp_path / f"udddump-20261023-000000-{run_id}.dump"
    artifact.write_bytes(payload)
    manifest = artifact.with_suffix(".manifest.json")
    manifest.write_text(
        json.dumps(
            {
                "dataset_name": f"{source}-{run_id}",
                "source": source,
                "source_version": None,
                "retrieved_at": "2026-10-23T00:00:00+00:00",
                "checksum": "sha256-test",
                "size_bytes": len(payload),
                "record_count": None,
                "license": None,
            }
        ),
        encoding="utf-8",
    )
    return artifact


class TestNormalizeUdddump:
    """`normalize udddump` — eventize one table of a dump artifact.

    The reading (TOC gate, shape gate, COPY -> records) is
    normalization's job now (ADR-012 amendment, 2026-10-23): the
    source name and run id come from the artifact's manifest sidecar,
    and the same raw can be re-eventized with other filters without
    re-downloading.
    """

    RECORDS = [
        {"package": "vim", "version": "1.0-1", "source": "udddump"},
        {"package": "awk", "version": "1:5.2-1", "source": "udddump"},
    ]

    def _gates_mocked(self, mock_module):
        """Patch the TOC list + COPY-header peek to pass both gates."""
        list_mock = mock_module.list_dump_tables
        list_mock.return_value = ["packages"]
        peek_mock = mock_module.peek_dump_copy_columns
        peek_mock.return_value = ["package", "version"]
        return list_mock, peek_mock

    @patch("debian_observer.cli.commands.normalize.list_dump_tables")
    @patch("debian_observer.cli.commands.normalize.peek_dump_copy_columns")
    @patch("debian_observer.cli.commands.normalize.read_dump_records")
    def test_udddump_eventizes_records_with_manifest_identity(
        self, read_mock, peek_mock, list_mock, tmp_path
    ):
        """Happy path: events are stamped with the manifest's source
        and run id, and the filters reach the reader."""
        artifact = _write_dump_artifact(tmp_path)
        read_mock.return_value = [dict(record) for record in self.RECORDS]
        list_mock.return_value = ["packages"]
        peek_mock.return_value = ["package", "version"]

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "normalize", "udddump", str(artifact),
                "--table", "packages",
                "--limit", "3",
            ],
        )

        assert result.exit_code == 0
        # the filters reach the reader; the artifact is the manifest's
        read_mock.assert_called_once_with(
            str(artifact), table="packages", limit=3
        )
        # both gates ran over the artifact
        list_mock.assert_called_once_with(str(artifact))
        peek_mock.assert_called_once_with(str(artifact), table="packages")
        # events as JSON lines, stamped from the manifest sidecar
        lines = [l for l in result.output.strip().split("\n") if l.startswith("{")]
        assert len(lines) == 2
        event = json.loads(lines[0])
        assert event["event_type"] == "PackageVersionObserved"
        assert event["ingestion_run_id"] == "run-dump-001"
        assert event["source"] == "udddump"

    @patch("debian_observer.cli.commands.normalize.list_dump_tables")
    @patch("debian_observer.cli.commands.normalize.peek_dump_copy_columns")
    @patch("debian_observer.cli.commands.normalize.read_dump_records")
    def test_udddump_gate_rejects_an_unknown_table(
        self, read_mock, peek_mock, list_mock, tmp_path
    ):
        """A table outside the dump TOC fails fast, listing tables."""
        artifact = _write_dump_artifact(tmp_path)
        list_mock.return_value = ["packages", "sources"]

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["normalize", "udddump", str(artifact), "--table", "pachages"],
        )

        assert result.exit_code != 0
        assert "pachages" in result.output
        # the error lists the restorable tables instead of a traceback
        assert "packages" in result.output
        assert "sources" in result.output
        assert "Traceback" not in result.output
        read_mock.assert_not_called()
        peek_mock.assert_not_called()

    @patch("debian_observer.cli.commands.normalize.list_dump_tables")
    @patch("debian_observer.cli.commands.normalize.peek_dump_copy_columns")
    @patch("debian_observer.cli.commands.normalize.read_dump_records")
    def test_udddump_gate_wraps_runner_errors(
        self, read_mock, peek_mock, list_mock, tmp_path
    ):
        """A failing pg_restore runner is a usage error, not a crash."""
        from debian_observer.normalization.udddump import UDDDumpError

        artifact = _write_dump_artifact(tmp_path)
        list_mock.side_effect = UDDDumpError(
            "pg_restore not found on PATH; install postgresql-client"
        )

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["normalize", "udddump", str(artifact), "--table", "packages"],
        )

        assert result.exit_code != 0
        assert "postgresql-client" in result.output
        assert "Traceback" not in result.output
        read_mock.assert_not_called()

    @patch("debian_observer.cli.commands.normalize.list_dump_tables")
    @patch("debian_observer.cli.commands.normalize.peek_dump_copy_columns")
    @patch("debian_observer.cli.commands.normalize.read_dump_records")
    def test_udddump_shape_gate_rejects_non_packages_shaped_tables(
        self, read_mock, peek_mock, list_mock, tmp_path
    ):
        """The shape gate fails fast: cause + perimeter, not a crash."""
        artifact = _write_dump_artifact(tmp_path)
        list_mock.return_value = ["sources_count"]
        peek_mock.return_value = ["source", "count"]

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["normalize", "udddump", str(artifact), "--table", "sources_count"],
        )

        assert result.exit_code != 0
        # the cause: the table and its COPY header
        assert "sources_count" in result.output
        assert "source, count" in result.output
        assert "not shaped like any supported table" in result.output
        # the perimeter: what is supported today
        assert "version + any of: package, source" in result.output
        assert "Traceback" not in result.output
        read_mock.assert_not_called()

    @patch("debian_observer.cli.commands.normalize.read_dump_records")
    def test_udddump_missing_manifest_is_a_clear_error(self, read_mock, tmp_path):
        """A dump artifact without a manifest sidecar has no provenance
        to recover: usage error, not a traceback."""
        artifact = tmp_path / "udddump-20261023-000000-run-nm.dump"
        artifact.write_bytes(b"PGDMP\x00")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["normalize", "udddump", str(artifact), "--table", "packages"],
        )

        assert result.exit_code != 0
        assert "manifest" in result.output
        assert "Traceback" not in result.output
        read_mock.assert_not_called()

    @patch("debian_observer.cli.commands.normalize.read_dump_records")
    def test_udddump_rejects_a_foreign_source(self, read_mock, tmp_path):
        """An artifact ingested from another source is not a udddump
        download: the error names the real source."""
        artifact = _write_dump_artifact(tmp_path, source="udd", run_id="run-udd")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["normalize", "udddump", str(artifact), "--table", "packages"],
        )

        assert result.exit_code != 0
        assert "udd" in result.output
        assert "udddump" in result.output
        assert "Traceback" not in result.output
        read_mock.assert_not_called()

    @patch("debian_observer.cli.commands.normalize.list_dump_tables")
    @patch("debian_observer.cli.commands.normalize.peek_dump_copy_columns")
    @patch("debian_observer.cli.commands.normalize.read_dump_records")
    def test_udddump_limit_unset_warns(
        self, read_mock, peek_mock, list_mock, tmp_path
    ):
        """Unset --limit normalizes the whole table with a warning."""
        artifact = _write_dump_artifact(tmp_path)
        read_mock.return_value = [dict(record) for record in self.RECORDS]
        list_mock.return_value = ["packages"]
        peek_mock.return_value = ["package", "version"]

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["normalize", "udddump", str(artifact), "--table", "packages"],
        )

        assert result.exit_code == 0
        assert "Warning" in result.stderr
        assert "--limit" in result.stderr
        assert "--limit unset; normalizing the whole table." in result.stderr
        # unset limit reaches the reader as None, not a string
        assert read_mock.call_args.kwargs["limit"] is None

    @patch("debian_observer.cli.commands.normalize.list_dump_tables")
    @patch("debian_observer.cli.commands.normalize.peek_dump_copy_columns")
    @patch("debian_observer.cli.commands.normalize.read_dump_records")
    def test_udddump_writes_invalid_records_file(
        self, read_mock, peek_mock, list_mock, tmp_path
    ):
        """Records failing validation go to --invalid (or are dropped)."""
        artifact = _write_dump_artifact(tmp_path)
        read_mock.return_value = [
            {"package": "vim", "version": "1.0-1", "source": "udddump"},
            {"package": "bad", "source": "udddump"},
        ]
        list_mock.return_value = ["packages"]
        peek_mock.return_value = ["package", "version"]

        runner = CliRunner()
        invalid_path = tmp_path / "invalid.jsonl"
        result = runner.invoke(
            cli,
            [
                "normalize", "udddump", str(artifact),
                "--table", "packages",
                "--invalid", str(invalid_path),
            ],
        )

        assert result.exit_code == 0
        invalid_lines = [
            l
            for l in invalid_path.read_text(encoding="utf-8").splitlines()
            if l.strip()
        ]
        assert len(invalid_lines) == 1
        assert json.loads(invalid_lines[0])["package"] == "bad"
        lines = [l for l in result.output.strip().split("\n") if l.startswith("{")]
        assert len(lines) == 1

    @patch("debian_observer.cli.commands.normalize.list_dump_tables")
    @patch("debian_observer.cli.commands.normalize.peek_dump_copy_columns")
    @patch("debian_observer.cli.commands.normalize.read_dump_records")
    def test_udddump_requires_a_table(self, read_mock, peek_mock, list_mock, tmp_path):
        """--table is the mandatory scope selector (ADR-012)."""
        artifact = _write_dump_artifact(tmp_path)
        list_mock.return_value = ["packages"]

        runner = CliRunner()
        result = runner.invoke(cli, ["normalize", "udddump", str(artifact)])

        assert result.exit_code == 2
        assert "--table" in result.output
        read_mock.assert_not_called()

    def test_udddump_help_states_the_surface(self):
        """--help shows what runs (host pg_restore) and what is not
        implemented (no database restore), per the CONTRIBUTING rule."""
        runner = CliRunner()
        result = runner.invoke(cli, ["normalize", "udddump", "--help"])

        assert result.exit_code == 0
        flat = " ".join(result.output.split())
        assert "--table" in result.output
        assert "--limit" in result.output
        assert "--output" in result.output
        assert "--invalid" in result.output
        assert "pg_restore" in flat
        assert "postgresql-client" in flat
