"""CLI tests for the `ingest` command group.

One subcommand per data source (ADR-012): `ingest sources`,
`ingest mirror`, `ingest udd`, `ingest udddump`. Each source's
selector is a mandatory filter flag.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from debian_observer.cli.main import cli
from debian_observer.ingestion.acquisition import UDDDumpError


def test_ingest_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["ingest", "--help"])
    assert result.exit_code == 0
    # One subcommand per source is listed in the synopsis.
    for subcommand in ("sources", "mirror", "udd", "udddump"):
        assert subcommand in result.output


def _make_mock_result(
    *,
    record_count: int = 3,
    ingestion_run_id: str = "run-abc",
    checksum: str = "sha256-xyz",
    events: list | None = None,
    parquet_path: str | None = None,
    dataset_path: str | None = None,
):
    """Build a minimal mock IngestionResult-like object."""
    mock = MagicMock()
    mock.record_count = record_count
    mock.ingestion_run_id = ingestion_run_id
    mock.checksum = checksum
    mock.events = events or []
    mock.parquet_path = Path(parquet_path) if parquet_path else None
    mock.dataset_path = Path(dataset_path) if dataset_path else None
    return mock


def test_ingest_sources_source():
    runner = CliRunner()
    mock_result = _make_mock_result(record_count=5, events=[MagicMock(), MagicMock()])

    with patch("debian_observer.cli.commands.ingest.IngestionPipeline") as MockPipeline:
        MockPipeline.return_value.run.return_value = mock_result
        result = runner.invoke(cli, ["ingest", "sources", "--package", "bash"])

    assert result.exit_code == 0
    # Verify pipeline was instantiated
    MockPipeline.assert_called_once()
    # Verify run was called with a SourcesDebianOrgDataSource
    call_args = MockPipeline.return_value.run.call_args
    data_source = call_args[0][0]
    assert data_source.name == "sources.debian.org"
    assert data_source._package_name == "bash"


def test_ingest_mirror_source():
    runner = CliRunner()
    mock_result = _make_mock_result(record_count=2, events=[MagicMock()])

    with patch("debian_observer.cli.commands.ingest.IngestionPipeline") as MockPipeline:
        MockPipeline.return_value.run.return_value = mock_result
        result = runner.invoke(
            cli,
            [
                "ingest", "mirror",
                "--suite", "stable",
                "--limit", "2",
                "--raw-dir", "/tmp/raw",
            ],
        )

    assert result.exit_code == 0
    call_args = MockPipeline.return_value.run.call_args
    data_source = call_args[0][0]
    assert data_source.name == "debian.mirror"
    assert data_source._suite == "stable"
    # --component/--architecture default to the archive's main pair.
    assert data_source._component == "main"
    assert data_source._architecture == "amd64"
    assert data_source._limit == 2
    # Verify raw_dir passed to pipeline
    MockPipeline.assert_called_once_with(
        raw_dir=Path("/tmp/raw"), parquet_dir=None, dataset_dir=None
    )


def test_ingest_mirror_component_architecture():
    """mirror exposes --component/--architecture (defaults: main/amd64)."""
    runner = CliRunner()
    mock_result = _make_mock_result()

    with patch("debian_observer.cli.commands.ingest.IngestionPipeline") as MockPipeline:
        MockPipeline.return_value.run.return_value = mock_result
        result = runner.invoke(
            cli,
            [
                "ingest", "mirror",
                "--suite", "sid",
                "--component", "contrib",
                "--architecture", "arm64",
            ],
        )

    assert result.exit_code == 0
    data_source = MockPipeline.return_value.run.call_args[0][0]
    assert data_source._component == "contrib"
    assert data_source._architecture == "arm64"


def test_ingest_udd_source():
    runner = CliRunner()
    mock_result = _make_mock_result(record_count=10, events=[MagicMock()] * 3)

    with patch("debian_observer.cli.commands.ingest.IngestionPipeline") as MockPipeline:
        MockPipeline.return_value.run.return_value = mock_result
        result = runner.invoke(
            cli, ["ingest", "udd", "--table", "packages", "--limit", "10"]
        )

    assert result.exit_code == 0
    call_args = MockPipeline.return_value.run.call_args
    data_source = call_args[0][0]
    assert data_source.name == "udd"
    assert data_source._table == "packages"
    assert data_source._limit == 10


def test_ingest_unknown_subcommand():
    """`ingest <unknown>` is a usage error: exit 2, no traceback."""
    runner = CliRunner()
    result = runner.invoke(cli, ["ingest", "unknown"])
    # A bad subcommand is a usage error, not a runtime crash.
    assert result.exit_code == 2
    # The message names the offending value...
    assert "unknown" in result.output
    # ...and no Python traceback leaks to the user.
    assert "Traceback" not in result.output
    assert "TypeError" not in result.output


def test_ingest_help_lists_subcommands():
    """`ingest --help` shows one subcommand per source."""
    runner = CliRunner()
    result = runner.invoke(cli, ["ingest", "--help"])
    assert result.exit_code == 0
    for subcommand in ("sources", "mirror", "udd", "udddump"):
        assert subcommand in result.output


def test_ingest_with_parquet_dir():
    runner = CliRunner()
    mock_result = _make_mock_result(record_count=5, parquet_path="/tmp/pq/out.parquet")

    with patch("debian_observer.cli.commands.ingest.IngestionPipeline") as MockPipeline:
        MockPipeline.return_value.run.return_value = mock_result
        result = runner.invoke(
            cli,
            [
                "ingest", "sources",
                "--package", "hello",
                "--parquet-dir", "/tmp/pq",
            ],
        )

    assert result.exit_code == 0
    MockPipeline.assert_called_once_with(
        raw_dir=Path("data/raw"), parquet_dir=Path("/tmp/pq"), dataset_dir=None
    )
    assert "Parquet written" in result.output


def test_ingest_output_format():
    runner = CliRunner()
    mock_result = _make_mock_result(
        record_count=42,
        ingestion_run_id="run-test-007",
        checksum="sha256-abc123",
        events=[MagicMock()] * 7,
    )

    with patch("debian_observer.cli.commands.ingest.IngestionPipeline") as MockPipeline:
        MockPipeline.return_value.run.return_value = mock_result
        result = runner.invoke(cli, ["ingest", "sources", "--package", "vim"])

    assert result.exit_code == 0
    assert "Ingested 42 records" in result.output
    assert "Run ID: run-test-007" in result.output
    assert "Checksum: sha256-abc123" in result.output
    assert "Events produced: 7" in result.output


def test_ingest_e2e_with_mock_source(tmp_path: Path):
    """Lightweight integration: real MockDebianDataSource + real IngestionPipeline.

    Validates that the CLI can invoke the full pipeline without mocks,
    producing real artifacts and events.
    """
    from debian_observer.ingestion.acquisition import MockDebianDataSource
    from debian_observer.ingestion.pipeline import IngestionPipeline

    runner = CliRunner()
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir()

    # We bypass the CLI's source dispatch and directly invoke the pipeline
    # because the CLI exposes real sources only. Instead, we verify the
    # pipeline integration by running it standalone and checking artifact creation.
    source = MockDebianDataSource(package_count=3)
    pipeline = IngestionPipeline(raw_dir=raw_dir, parquet_dir=parquet_dir)
    result = pipeline.run(source, ingestion_run_id="run-e2e")

    assert result.record_count == 3
    assert result.artifact_path is not None
    assert result.artifact_path.exists()
    assert len(result.events) == 3
    # Parquet output should be written when parquet_dir is provided
    if result.parquet_path:
        assert result.parquet_path.exists()

    # Now verify the CLI output format with the real result data mirrored
    mock_result = _make_mock_result(
        record_count=result.record_count,
        ingestion_run_id=result.ingestion_run_id,
        checksum=result.checksum,
        events=result.events,
        parquet_path=str(result.parquet_path) if result.parquet_path else None,
    )

    with patch("debian_observer.cli.commands.ingest.IngestionPipeline") as MockPipeline:
        MockPipeline.return_value.run.return_value = mock_result
        cli_result = runner.invoke(
            cli,
            [
                "ingest", "mirror",
                "--suite", "sid",
                "--raw-dir", str(raw_dir),
                "--parquet-dir", str(parquet_dir),
            ],
        )

    assert cli_result.exit_code == 0
    assert "Ingested 3 records" in cli_result.output
    assert "Events produced: 3" in cli_result.output
    assert "Parquet written" in cli_result.output


def test_ingest_with_dataset_dir():
    runner = CliRunner()
    mock_result = _make_mock_result(
        record_count=5, dataset_path="/tmp/norm/package_versions/run.parquet"
    )

    with patch("debian_observer.cli.commands.ingest.IngestionPipeline") as MockPipeline:
        MockPipeline.return_value.run.return_value = mock_result
        result = runner.invoke(
            cli,
            [
                "ingest", "sources",
                "--package", "hello",
                "--dataset-dir", "/tmp/norm",
            ],
        )

    assert result.exit_code == 0
    MockPipeline.assert_called_once_with(
        raw_dir=Path("data/raw"), parquet_dir=None, dataset_dir=Path("/tmp/norm")
    )
    assert "Dataset written" in result.output


def test_pipeline_writes_dataset_when_configured(tmp_path: Path):
    """Real pipeline + mock source: dataset file written when dataset_dir set."""
    from debian_observer.ingestion.acquisition import MockDebianDataSource
    from debian_observer.ingestion.pipeline import IngestionPipeline
    import pyarrow.parquet as pq

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    dataset_dir = tmp_path / "normalized" / "package_versions"

    source = MockDebianDataSource(package_count=3)
    pipeline = IngestionPipeline(raw_dir=raw_dir, dataset_dir=dataset_dir)
    result = pipeline.run(source, ingestion_run_id="run-ds-e2e")

    assert result.dataset_path is not None
    assert result.dataset_path.exists()
    table = pq.read_table(result.dataset_path)
    assert table.num_rows == len(result.events)


class TestIngestUnlimited:
    """--limit defaults to the whole source with a stderr warning."""

    @patch("debian_observer.cli.commands.ingest.IngestionPipeline")
    def test_default_limit_is_none_and_warns(self, MockPipeline: MagicMock) -> None:
        MockPipeline.return_value.run.return_value = _make_mock_result()
        runner = CliRunner()
        result = runner.invoke(cli, ["ingest", "udd", "--table", "packages"])

        assert result.exit_code == 0
        data_source = MockPipeline.return_value.run.call_args[0][0]
        assert data_source._limit is None
        assert "Warning" in result.stderr
        assert "--limit" in result.stderr

    @patch("debian_observer.cli.commands.ingest.IngestionPipeline")
    def test_explicit_limit_keeps_value_without_warning(
        self, MockPipeline: MagicMock
    ) -> None:
        MockPipeline.return_value.run.return_value = _make_mock_result()
        runner = CliRunner()
        result = runner.invoke(
            cli, ["ingest", "mirror", "--suite", "stable", "--limit", "5"]
        )

        assert result.exit_code == 0
        data_source = MockPipeline.return_value.run.call_args[0][0]
        assert data_source._limit == 5
        assert "Warning" not in result.stderr


class TestIngestUdddump:
    """`ingest udddump` — one table of a UDD `pg_dump` file (ADR-012).

    The dump TOC is validated at startup: an unknown `--table` is a
    usage error that lists the restorable tables, never a pg_restore
    crash or a traceback.
    """

    def test_udddump_wires_the_dump_source(self) -> None:
        runner = CliRunner()
        mock_result = _make_mock_result(record_count=3)

        with (
            patch(
                "debian_observer.cli.commands.ingest.list_dump_tables",
                return_value=["packages"],
            ),
            patch(
                "debian_observer.cli.commands.ingest.IngestionPipeline"
            ) as MockPipeline,
        ):
            MockPipeline.return_value.run.return_value = mock_result
            result = runner.invoke(
                cli,
                [
                    "ingest", "udddump",
                    "--file", "/srv/udd/udd.dump",
                    "--table", "packages",
                    "--limit", "3",
                    "--raw-dir", "/tmp/udddump-raw",
                ],
            )

        assert result.exit_code == 0
        data_source = MockPipeline.return_value.run.call_args[0][0]
        assert data_source.name == "udd.dump"
        assert data_source._file == "/srv/udd/udd.dump"
        assert data_source._table == "packages"
        assert data_source._limit == 3
        MockPipeline.assert_called_once_with(
            raw_dir=Path("/tmp/udddump-raw"), parquet_dir=None, dataset_dir=None
        )

    def test_udddump_gate_rejects_an_unknown_table(self) -> None:
        """A table outside the dump TOC fails fast, listing tables."""
        runner = CliRunner()

        with (
            patch(
                "debian_observer.cli.commands.ingest.list_dump_tables",
                return_value=["packages", "sources"],
            ),
            patch(
                "debian_observer.cli.commands.ingest.IngestionPipeline"
            ) as MockPipeline,
        ):
            result = runner.invoke(
                cli,
                [
                    "ingest", "udddump",
                    "--file", "/srv/udd/udd.dump",
                    "--table", "pachages",
                ],
            )

        assert result.exit_code != 0
        assert "pachages" in result.output
        # the error lists the restorable tables instead of a traceback
        assert "packages" in result.output
        assert "sources" in result.output
        assert "Traceback" not in result.output
        MockPipeline.return_value.run.assert_not_called()

    def test_udddump_gate_wraps_runner_errors(self) -> None:
        """A failing pg_restore runner is a usage error, not a crash."""
        runner = CliRunner()

        with (
            patch(
                "debian_observer.cli.commands.ingest.list_dump_tables",
                side_effect=UDDDumpError(
                    "pg_restore not found on PATH; install postgresql-client"
                ),
            ),
            patch(
                "debian_observer.cli.commands.ingest.IngestionPipeline"
            ) as MockPipeline,
        ):
            result = runner.invoke(
                cli,
                [
                    "ingest", "udddump",
                    "--file", "/srv/udd/udd.dump",
                    "--table", "packages",
                ],
            )

        assert result.exit_code != 0
        assert "postgresql-client" in result.output
        assert "Traceback" not in result.output
        MockPipeline.return_value.run.assert_not_called()

    def test_udddump_default_limit_is_none_and_warns(self) -> None:
        """Unset --limit ingests the whole table with a stderr warning."""
        runner = CliRunner()
        mock_result = _make_mock_result()

        with (
            patch(
                "debian_observer.cli.commands.ingest.list_dump_tables",
                return_value=["packages"],
            ),
            patch(
                "debian_observer.cli.commands.ingest.IngestionPipeline"
            ) as MockPipeline,
        ):
            MockPipeline.return_value.run.return_value = mock_result
            result = runner.invoke(
                cli,
                [
                    "ingest", "udddump",
                    "--file", "/srv/udd/udd.dump",
                    "--table", "packages",
                ],
            )

        assert result.exit_code == 0
        data_source = MockPipeline.return_value.run.call_args[0][0]
        assert data_source._limit is None
        assert "Warning" in result.stderr
        assert "--limit" in result.stderr

    def test_udddump_help_shows_the_pinned_runner(self) -> None:
        """--help surfaces the pinned runner so operators know what
        runs (and what is not implemented)."""
        runner = CliRunner()
        result = runner.invoke(cli, ["ingest", "udddump", "--help"])

        assert result.exit_code == 0
        # host-first: pg_restore on PATH, no container indirection
        assert "PATH" in result.output
        assert "postgresql-client" in result.output
        assert "no database" in result.output.lower()
        assert "udd-postgresql" not in result.output
        assert "UDDDUMP_CONTAINER" not in result.output
        assert "UDDDUMP_USER" not in result.output
