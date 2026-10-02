"""Tests for CLI consume command."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from click.testing import CliRunner

from debian_observer.cli.main import cli


class TestConsumeCommand:
    """CLI tests for the consume entrypoint."""

    def test_consume_help_shows_options(self) -> None:
        runner = CliRunner()
        result = runner.invoke(cli, ["consume", "--help"])
        assert result.exit_code == 0
        assert "--stream" in result.output
        assert "--subject" in result.output
        assert "--durable" in result.output

    def test_consume_requires_stream_option(self) -> None:
        runner = CliRunner()
        result = runner.invoke(cli, ["consume"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "--stream" in result.output

    def test_consume_accepts_valid_args(self) -> None:
        """Ensure the command parses all expected flags without blocking."""
        runner = CliRunner()
        with patch("debian_observer.cli.commands.consume.ConsumerApp") as mock_app:
            # Prevent the actual event loop / NATS connection
            mock_instance = mock_app.return_value
            mock_instance.run = AsyncMock()
            result = runner.invoke(
                cli,
                [
                    "consume",
                    "--stream",
                    "EVENTS",
                    "--subject",
                    "debian.observer.>",
                    "--durable",
                    "consumer-cli-01",
                ],
            )
            assert result.exit_code == 0
            assert (
                "consumer-cli-01" in result.output
                or "Starting consumer" in result.output
            )

    def test_consume_app_raises_exception(self) -> None:
        """Ensure the command exits with ClickException when ConsumerApp fails."""
        runner = CliRunner()
        with patch("debian_observer.cli.commands.consume.ConsumerApp") as mock_app:
            mock_instance = mock_app.return_value
            mock_instance.run = AsyncMock(side_effect=RuntimeError("NATS unavailable"))
            result = runner.invoke(
                cli,
                [
                    "consume",
                    "--stream",
                    "EVENTS",
                    "--subject",
                    "debian.observer.>",
                    "--durable",
                    "consumer-cli-01",
                ],
            )
            assert result.exit_code != 0
            assert "Consumer error" in result.output
            assert "NATS unavailable" in result.output
