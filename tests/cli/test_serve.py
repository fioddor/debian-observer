"""CLI tests for serve command.

Verifies that the serve command:
- Starts the API service via uvicorn
- Loads Parquet data when --parquet-dir is provided
- Reads PARQUET_DIR from environment when --parquet-dir is not provided
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from debian_observer.cli.main import cli


class TestServeHelp:
    """Validate serve command help and options."""

    def test_serve_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["serve", "--help"])
        assert result.exit_code == 0
        assert "--host" in result.output
        assert "--port" in result.output
        assert "--parquet-dir" in result.output


class TestServeInvocation:
    """Validate serve command starts uvicorn correctly."""

    def test_serve_starts_uvicorn(self):
        runner = CliRunner()
        with patch(
            "debian_observer.cli.commands.serve.uvicorn.run"
        ) as mock_uvicorn_run:
            result = runner.invoke(
                cli, ["serve", "--host", "0.0.0.0", "--port", "8000"]
            )
            assert result.exit_code == 0
            mock_uvicorn_run.assert_called_once()
            # Verify host/port were passed
            call_kwargs = mock_uvicorn_run.call_args.kwargs
            assert call_kwargs["host"] == "0.0.0.0"
            assert call_kwargs["port"] == 8000

    def test_serve_default_host_is_localhost(self):
        runner = CliRunner()
        with patch(
            "debian_observer.cli.commands.serve.uvicorn.run"
        ) as mock_uvicorn_run:
            result = runner.invoke(cli, ["serve"])
            assert result.exit_code == 0
            call_kwargs = mock_uvicorn_run.call_args.kwargs
            assert call_kwargs["host"] == "127.0.0.1"
            assert call_kwargs["port"] == 8000


class TestServeParquetLoading:
    """Validate Parquet data loading via CLI or environment."""

    def test_serve_loads_parquet_when_flag_provided(self):
        runner = CliRunner()
        with (
            patch("debian_observer.cli.commands.serve.uvicorn.run") as mock_uvicorn_run,
            patch("debian_observer.cli.commands.serve.EventLoader") as mock_loader_cls,
        ):
            mock_loader = MagicMock()
            mock_loader.load_from_parquet.return_value = 42
            mock_loader_cls.return_value = mock_loader

            with runner.isolated_filesystem():
                os.makedirs("data/parquet")
                result = runner.invoke(cli, ["serve", "--parquet-dir", "data/parquet"])

            assert result.exit_code == 0
            mock_loader_cls.assert_called_once()
            mock_loader.load_from_parquet.assert_called_once_with("data/parquet")
            assert "Loaded 42 events" in result.output

    def test_serve_reads_parquet_dir_from_env(self):
        runner = CliRunner(env={"PARQUET_DIR": "/app/data/parquet"})
        with (
            patch("debian_observer.cli.commands.serve.uvicorn.run") as mock_uvicorn_run,
            patch("debian_observer.cli.commands.serve.EventLoader") as mock_loader_cls,
        ):
            mock_loader = MagicMock()
            mock_loader.load_from_parquet.return_value = 7
            mock_loader_cls.return_value = mock_loader

            result = runner.invoke(cli, ["serve"])

            assert result.exit_code == 0
            mock_loader_cls.assert_called_once()
            mock_loader.load_from_parquet.assert_called_once_with("/app/data/parquet")
            assert "Loaded 7 events" in result.output

    def test_serve_flag_overrides_env(self):
        runner = CliRunner(env={"PARQUET_DIR": "/env/path"})
        with (
            patch("debian_observer.cli.commands.serve.uvicorn.run") as mock_uvicorn_run,
            patch("debian_observer.cli.commands.serve.EventLoader") as mock_loader_cls,
        ):
            mock_loader = MagicMock()
            mock_loader.load_from_parquet.return_value = 3
            mock_loader_cls.return_value = mock_loader

            result = runner.invoke(cli, ["serve", "--parquet-dir", "/flag/path"])

            assert result.exit_code == 0
            mock_loader.load_from_parquet.assert_called_once_with("/flag/path")

    def test_serve_skips_loader_when_no_parquet_configured(self):
        runner = CliRunner()
        with (
            patch("debian_observer.cli.commands.serve.uvicorn.run") as mock_uvicorn_run,
            patch("debian_observer.cli.commands.serve.EventLoader") as mock_loader_cls,
        ):
            result = runner.invoke(cli, ["serve"])

            assert result.exit_code == 0
            mock_loader_cls.assert_not_called()
