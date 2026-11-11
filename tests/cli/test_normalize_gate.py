"""Tests for the startup gate with a selected eventizer.

With an eventizer selected, only ITS declared shape gates the table
at startup: a table outside the old perimeter (e.g. ``popcon``)
must not be rejected before the eventizer can say what it needs.
Without a selection, the old perimeter keeps guarding.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from debian_observer.cli.main import cli
from debian_observer.normalization.eventizers import EVENTIZERS
from debian_observer.normalization.udddump import TableShape


class _FakeEventizer:
    """A minimal eventizer whose shape is NOT the packages shape."""

    name = "fake"
    event_type = "PackageVersionObserved"
    shape = TableShape(
        name="fake", required_columns=frozenset({"package", "insts"})
    )

    def with_provenance(self, *, source_name: str, ingestion_run_id: str):
        return self

    def eventize(self, row):
        yield from ()


class TestStartupGateHonoursTheSelectedEventizer:
    def test_selected_eventizer_shape_decides(self, tmp_path: Path) -> None:
        """A popcon-shaped table passes with its eventizer selected.

        Red before the fix: the startup gate checked the table
        against the packages-only perimeter and rejected it with
        "not shaped like any supported table".
        """
        from tests.cli.test_normalization import _write_dump_artifact

        artifact = _write_dump_artifact(tmp_path)
        EVENTIZERS["udddump"]["fake"] = _FakeEventizer()
        try:
            runner = CliRunner()
            with patch(
                "debian_observer.cli.commands.normalize.list_dump_tables",
                return_value=["popcon"],
            ), patch(
                "debian_observer.cli.commands.normalize."
                "peek_dump_copy_columns",
                return_value=["package", "insts", "vote"],
            ), patch(
                "debian_observer.cli.commands.normalize.iter_dump_rows",
                return_value=iter([]),
            ):
                result = runner.invoke(
                    cli,
                    [
                        "normalize", "udddump", str(artifact),
                        "--table", "popcon",
                        "--eventizer", "fake",
                        "--events-dir", str(tmp_path / "events"),
                    ],
                )
            assert result.exit_code == 0, result.output
            assert "not shaped" not in result.output
        finally:
            del EVENTIZERS["udddump"]["fake"]

    def test_without_a_selection_the_old_perimeter_guards(
        self, tmp_path: Path
    ) -> None:
        """No eventizer selected → the packages perimeter still gates."""
        from tests.cli.test_normalization import _write_dump_artifact

        artifact = _write_dump_artifact(tmp_path)
        runner = CliRunner()
        with patch(
            "debian_observer.cli.commands.normalize.list_dump_tables",
            return_value=["popcon"],
        ), patch(
            "debian_observer.cli.commands.normalize.peek_dump_copy_columns",
            return_value=["package", "insts", "vote"],
        ):
            result = runner.invoke(
                cli,
                [
                    "normalize", "udddump", str(artifact),
                    "--table", "popcon",
                    "--events-dir", str(tmp_path / "events"),
                ],
            )
        assert result.exit_code != 0
        assert "not shaped like any supported table" in result.output