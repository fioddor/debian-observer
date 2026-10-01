"""CLI tests for normalization commands."""
from __future__ import annotations

import json
from click.testing import CliRunner
from debian_observer.cli.main import cli


def test_normalize_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["normalize", "--help"])
    assert result.exit_code == 0


def test_normalize_valid_record():
    runner = CliRunner()
    input_data = json.dumps({"package": "vim", "version": "1.0"}) + "\n"
    result = runner.invoke(cli, ["normalize", "--source", "udd"], input=input_data)
    assert result.exit_code == 0
    lines = [l for l in result.output.strip().split("\n") if l and l.startswith("{")]
    assert len(lines) == 1
    event = json.loads(lines[0])
    assert event["event_type"] == "PackageVersionObserved"


def test_normalize_invalid_record():
    runner = CliRunner()
    input_data = json.dumps({"package": "vim"}) + "\n"
    result = runner.invoke(cli, ["normalize", "--source", "udd"], input=input_data)
    assert result.exit_code == 0
    lines = [l for l in result.output.strip().split("\n") if l and l.startswith("{")]
    assert len(lines) == 0


def test_validate_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--help"])
    assert result.exit_code == 0


def test_validate_all_valid():
    runner = CliRunner()
    input_data = json.dumps({"package": "vim", "version": "1.0"}) + "\n"
    result = runner.invoke(cli, ["validate", "--source", "udd"], input=input_data)
    assert result.exit_code == 0
    assert "Valid: 1, Invalid: 0" in result.output


def test_validate_some_invalid():
    runner = CliRunner()
    input_data = (
        json.dumps({"package": "vim", "version": "1.0"}) + "\n" +
        json.dumps({"package": "bad"}) + "\n"
    )
    result = runner.invoke(cli, ["validate", "--source", "udd"], input=input_data)
    assert result.exit_code == 0
    assert "Valid: 1, Invalid: 1" in result.output


def test_unknown_source_rejected(tmp_path):
    """Validate should reject unknown source types as usage errors."""
    runner = CliRunner()
    inline_jsonl = tmp_path / "inline.jsonl"
    inline_jsonl.write_text(json.dumps({"package": "vim", "version": "1.0"}) + "\n")
    result = runner.invoke(cli, ["validate", "--input", str(inline_jsonl), "--source", "mock"])
    assert result.exit_code == 2
    assert "Invalid value" in result.output
