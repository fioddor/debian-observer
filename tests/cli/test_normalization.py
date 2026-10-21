"""CLI tests for validate/normalize over raw artifacts (ADR-012).

Artifacts are positional; the source name and run id are read from
each artifact's manifest sidecar, as written by ingest.
"""

from __future__ import annotations

import json
from pathlib import Path

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


def test_normalize_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["normalize", "--help"])
    assert result.exit_code == 0


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
    result = runner.invoke(cli, ["normalize", str(artifact)])
    assert result.exit_code == 0
    lines = [l for l in result.output.strip().split("\n") if l.startswith("{")]
    assert len(lines) == 2
    event = json.loads(lines[0])
    assert event["event_type"] == "PackageVersionObserved"


def test_normalize_stamps_manifest_source_and_run_id(tmp_path):
    """The source and run id stamped on events come from the manifest."""
    runner = CliRunner()
    artifact = _write_artifact(tmp_path, source="debian.mirror", run_id="run-cli-9")
    result = runner.invoke(cli, ["normalize", str(artifact)])
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
        cli, ["normalize", str(artifact_a), str(artifact_b)]
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
    result = runner.invoke(cli, ["normalize", str(artifact)])
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
        cli, ["normalize", str(artifact), "--invalid", str(invalid_path)]
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
    result = runner.invoke(cli, ["normalize", str(artifact)])
    assert result.exit_code != 0
    assert "manifest" in result.output
    assert "Traceback" not in result.output


def test_validate_requires_an_artifact():
    """No positional artifact is a usage error."""
    runner = CliRunner()
    result = runner.invoke(cli, ["validate"])
    assert result.exit_code == 2