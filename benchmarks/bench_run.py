#!/usr/bin/env python3
"""Generic benchmark runner: measure an arbitrary command.

The measurement half of the `benchmark run` operator tool the
release2 plan describes (docs/release2.project.v0.4.md): run a
command N times and file, per iteration, wall seconds, peak RSS (the
sum of ``VmRSS`` over the command's process tree, sampled from
``/proc/<pid>/status`` every 0.5 s — no new dependency), output
bytes, exit code and the caller's labels, as one JSON lines record.

Kept as a script under ``benchmarks/`` — the plan's own rule: the
generic tool is the operator's, the experiments are invocations of
it, not subcommands of the CLI.

Usage::

    benchmarks/bench_run.py --label e3 --workers 4 \
        --out benchmarks/results/e3.jsonl -- debian-observer work britney ...

The record keys are the ``ExecutionRecord`` field names (timestamps
ISO 8601 UTC), so a run files straight into the analytics store.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Sequence

#: Sample cadence of the RSS watcher, seconds. Fast enough to catch
#: the parse's plateau, slow enough not to load the measured run.
RSS_SAMPLE_SECONDS = 0.5


def _process_tree_rss_kb(pid: int) -> int:
    """Sum ``VmRSS`` over a process tree, from /proc (no dependency)."""
    total = 0
    stack = [pid]
    while stack:
        current = stack.pop()
        try:
            with open(f"/proc/{current}/status", encoding="utf-8") as handle:
                for line in handle:
                    if line.startswith("VmRSS:"):
                        total += int(line.split()[1])
                        break
            children = Path(f"/proc/{current}/task/{current}/children")
            if children.exists():
                stack.extend(int(c) for c in children.read_text().split())
        except (OSError, ValueError):
            continue  # gone between the reads
    return total


class RSSWatcher:
    """Peak-RSS watcher for a set of pids, sampled off the main flow."""

    def __init__(self, pids: Sequence[int]) -> None:
        self._pids = list(pids)
        self._stop = threading.Event()
        self._peak = 0
        self._thread = threading.Thread(target=self._watch, daemon=True)

    def _watch(self) -> None:
        while not self._stop.is_set():
            try:
                live = [p for p in self._pids if Path(f"/proc/{p}").exists()]
                if live:
                    self._peak = max(
                        self._peak, sum(_process_tree_rss_kb(p) for p in live)
                    )
            except Exception:
                pass
            time.sleep(RSS_SAMPLE_SECONDS)

    def __enter__(self) -> "RSSWatcher":
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=2 * RSS_SAMPLE_SECONDS)

    @property
    def peak_rss_bytes(self) -> int:
        """The sampled peak of the summed tree RSS."""
        return self._peak * 1024


def measure(
    command: Sequence[str],
    *,
    label: str,
    implementation: str = "python",
    workers: int | None = None,
    coordinate: str | None = None,
    work_items: int | None = None,
    env: dict[str, str] | None = None,
    cwd: str | None = None,
    stdout_to: str | None = None,
) -> dict:
    """Run ``command`` once under measurement; return its record."""
    started = datetime.datetime.now(datetime.timezone.utc)
    wall_start = time.perf_counter()
    if stdout_to:
        out_handle = open(stdout_to, "w", encoding="utf-8")
    else:
        out_handle = subprocess.PIPE
    try:
        process = subprocess.Popen(
            list(command),
            stdout=out_handle,
            stderr=subprocess.STDOUT,
            env={**os.environ, **(env or {})},
            cwd=cwd,
        )
        with RSSWatcher([process.pid]) as watcher:
            _, _ = process.communicate()
        wall = time.perf_counter() - wall_start
        record = {
            "run_id": None,  # filled by the caller's out-file naming
            "run_at": started.isoformat(),
            "execution_type": f"benchmark-{label}",
            "implementation": implementation,
            "workers": workers,
            "coordinate": coordinate,
            "work_items": work_items,
            "duration_seconds": wall,
            "peak_rss_bytes": watcher.peak_rss_bytes,
            "exit_code": process.returncode,
        }
    finally:
        if stdout_to:
            out_handle.close()
    return record


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--out", required=True, help="JSONL file to append to")
    parser.add_argument("--implementation", default="python")
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--coordinate", default=None)
    parser.add_argument("--work-items", type=int, default=None)
    parser.add_argument("--stdout-to", default=None, help="capture the command's output here")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command and args.command[0] == "--" else args.command
    if not command:
        parser.error("a command to measure is required after --")
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    for _ in range(max(1, args.repeat)):
        record = measure(
            command,
            label=args.label,
            implementation=args.implementation,
            workers=args.workers,
            coordinate=args.coordinate,
            work_items=args.work_items,
            stdout_to=args.stdout_to,
        )
        with out.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
        print(json.dumps(record, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())