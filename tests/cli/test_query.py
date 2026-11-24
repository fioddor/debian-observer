"""CLI tests for the `query` command (event log over --events-dir)."""

from __future__ import annotations

from click.testing import CliRunner

from debian_observer.cli.main import cli


def test_query_help_mentions_events_dir():
    runner = CliRunner()
    result = runner.invoke(cli, ["query", "--help"])
    assert result.exit_code == 0
    # the event-log flag is content-named, not format-named (ADR-012 amendment)
    assert "--events-dir" in result.output
    assert "--parquet-dir" not in result.output
    # the same default event log as every other command
    assert "data/observed" in result.output
    assert "--dataset-dir" in result.output
    assert "--debug" in result.output
    assert "--describe" in result.output


def test_query_reads_events_dir(tmp_path):
    """--events-dir wires the event log into the query (real Parquet)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    runner = CliRunner()
    events_dir = tmp_path / "events"
    events_dir.mkdir()
    table = pa.Table.from_pylist([{"package_id": "hello:amd64", "version": "2.10-3"}])
    pq.write_table(table, events_dir / "run-001.parquet")

    result = runner.invoke(
        cli,
        ["query", "SELECT count(*) AS c FROM events", "--events-dir", str(events_dir)],
    )
    assert result.exit_code == 0


def test_query_dataset_dir_alone_reaches_the_database(tmp_path):
    """A query over a dataset never needs the event log.

    --dataset-dir points the query at consolidated Parquet tables;
    --events-dir is an optional extra source, not a precondition.
    The default events dir (data/observed) does not exist here.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    runner = CliRunner()
    dataset_dir = tmp_path / "datasets"
    dataset_dir.mkdir()
    ds_table = pa.Table.from_pylist([{"package_id": "vim:amd64", "version": "9.0"}])
    pq.write_table(ds_table, dataset_dir / "package_versions.parquet")

    result = runner.invoke(
        cli,
        [
            "query",
            "SELECT * FROM datasets",
            "--dataset-dir",
            str(dataset_dir),
        ],
    )
    assert result.exit_code == 0
    assert "vim:amd64" in result.output


def test_query_describe_dataset_dir_alone_describes_only_datasets(tmp_path):
    """--describe with --dataset-dir only reads the dataset structure.

    describe answers "what tables/columns are here", it never
    consults the event log. The default events dir does not exist.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    runner = CliRunner()
    dataset_dir = tmp_path / "datasets"
    dataset_dir.mkdir()
    ds_table = pa.Table.from_pylist([{"package_id": "vim:amd64", "version": "9.0"}])
    pq.write_table(ds_table, dataset_dir / "package_versions.parquet")

    result = runner.invoke(
        cli,
        ["query", "--dataset-dir", str(dataset_dir), "--describe"],
    )
    assert result.exit_code == 0
    assert "datasets" in result.output
    # the events view is absent because there is no event log
    assert "events" not in result.output


def test_query_events_sql_without_event_log_shows_clean_error(tmp_path):
    """SQL that touches `events` without an event log still errors cleanly."""
    runner = CliRunner()
    result = runner.invoke(
        cli, ["query", "SELECT * FROM events", "--dataset-dir", str(tmp_path)]
    )
    assert result.exit_code == 1
    assert "Error:" in result.output
    assert "events" in result.output or "No Parquet" in result.output
    assert len(result.output.splitlines()) < 10


def test_query_dataset_dir_with_hyphenated_name(tmp_path):
    """A query over a dataset never needs the event log.

    A dataset directory is one table named from the directory, and
    directory names carry hyphens (``deep-dependencies`` is the
    layout's own example). A hyphen is not a legal SQL identifier;
    the view name must be quoted or the CREATE VIEW dies in the
    parser. The describe answers with the same name.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    runner = CliRunner()
    dataset_dir = tmp_path / "deep-dependencies"
    dataset_dir.mkdir()
    ds_table = pa.Table.from_pylist([{"package_id": "vim:amd64", "version": "9.0"}])
    pq.write_table(ds_table, dataset_dir / "run-74923dbac560.parquet")

    # the query reaches the rows through the hyphenated view name
    result = runner.invoke(
        cli,
        [
            "query",
            'SELECT * FROM "deep-dependencies"',
            "--dataset-dir",
            str(dataset_dir),
        ],
    )
    assert result.exit_code == 0
    assert "vim:amd64" in result.output

    # the describe answers the same name the view is registered with
    result = runner.invoke(
        cli,
        ["query", "--dataset-dir", str(dataset_dir), "--describe"],
    )
    assert result.exit_code == 0
    assert "deep-dependencies" in result.output


def test_query_bad_sql_shows_clean_error(tmp_path):
    """A bad SQL query prints a clean error and exits non-zero."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    runner = CliRunner()
    events_dir = tmp_path / "events"
    events_dir.mkdir()
    table = pa.Table.from_pylist([{"x": 1}])
    pq.write_table(table, events_dir / "run-001.parquet")

    result = runner.invoke(
        cli,
        ["query", "SELECT * FROM no_such_table", "--events-dir", str(events_dir)],
    )
    assert result.exit_code == 1
    assert "Error:" in result.output
    # tracebacks span many lines; a clean error is short
    assert len(result.output.splitlines()) < 10


def test_query_debug_shows_traceback(tmp_path):
    """--debug re-raises the exception so the traceback is visible."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    runner = CliRunner()
    events_dir = tmp_path / "events"
    events_dir.mkdir()
    table = pa.Table.from_pylist([{"x": 1}])
    pq.write_table(table, events_dir / "run-001.parquet")

    result = runner.invoke(
        cli,
        [
            "query",
            "SELECT * FROM no_such_table",
            "--events-dir",
            str(events_dir),
            "--debug",
        ],
    )
    assert result.exit_code == 1
    # the raw exception bubbles through (not wrapped in ClickException)
    assert result.exception is not None
    assert "no_such_table" in str(result.exception)


def test_query_describe_shows_columns(tmp_path):
    """--describe lists available tables and their columns."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    runner = CliRunner()
    events_dir = tmp_path / "events"
    events_dir.mkdir()
    table = pa.Table.from_pylist([{"package_id": "a", "version": "1.0"}])
    pq.write_table(table, events_dir / "run-001.parquet")

    result = runner.invoke(
        cli,
        ["query", "--events-dir", str(events_dir), "--describe"],
    )
    assert result.exit_code == 0
    assert "events" in result.output
    assert "package_id" in result.output
    assert "version" in result.output


def test_query_dataset_dir_registers_view(tmp_path):
    """A ``*.parquet`` in --dataset-dir becomes a queryable view."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    runner = CliRunner()
    events_dir = tmp_path / "events"
    events_dir.mkdir()
    event_table = pa.Table.from_pylist([{"event_type": "test"}])
    pq.write_table(event_table, events_dir / "run-001.parquet")

    dataset_dir = tmp_path / "datasets"
    dataset_dir.mkdir()
    ds_table = pa.Table.from_pylist([{"package_id": "vim:amd64", "version": "9.0"}])
    pq.write_table(ds_table, dataset_dir / "package_versions.parquet")

    result = runner.invoke(
        cli,
        [
            "query",
            "SELECT * FROM datasets",
            "--events-dir",
            str(events_dir),
            "--dataset-dir",
            str(dataset_dir),
        ],
    )
    assert result.exit_code == 0
    assert "vim:amd64" in result.output
