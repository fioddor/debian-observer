"""Console telemetry must stream to stderr, not stdout.

The OpenTelemetry console exporters default to stdout, which pollutes
the output of `ingest`, `benchmark` and any other command whose
telemetry is flushed at process exit: piping or redirecting stdout
mixes JSON span/metric dumps with the command output. The contract
pinned here is that a command stdout carries only its own output
while console spans and metrics land on stderr.
"""

from __future__ import annotations

import subprocess
import sys

TELEMETRY_MARKERS = ("trace_id", "resource_metrics")


def test_command_stdout_is_free_of_console_telemetry(tmp_path):
    """Pipes and redirects of command stdout stay clean.

    Runs `benchmark` in a real subprocess (telemetry flushes at
    process exit) and checks that its stdout carries no span or
    metric JSON while stderr does.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "debian_observer.cli.main",
            "benchmark",
            "--records",
            "5",
            "--raw-dir",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    for marker in TELEMETRY_MARKERS:
        assert marker not in result.stdout, (
            f"telemetry marker {marker!r} leaked into stdout"
        )
    assert any(
        marker in result.stderr for marker in TELEMETRY_MARKERS
    ), "no console telemetry found on stderr"
