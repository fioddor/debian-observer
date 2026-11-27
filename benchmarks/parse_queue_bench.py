#!/usr/bin/env python3
"""The parse queue experiments (E1–E5 of the release3 plan §16).

Invocations of the generic runner (``benchmarks/bench_run.py``, the
``benchmark run`` measurement half): each experiment files its
numbers under ``benchmarks/results/`` as text (the question, the
command shape, the measurements).

* E1 — parse cost per snapshot over the window, compact-state size;
* E2 — pair-eventizer cost per pair (ADR-027's cheap-diff assumption);
* E3 — backfill scaling: 1, 2, 4 workers on one machine
  (the across-machines half needs a second machine — not available
  here; registered pending in the results file, not run);
* E4 — the failure runs of §15 (a worker killed mid-job);
* E5 — the parallel ceiling: CPU contention vs memory.

Usage (inside a devcontainer that reaches NATS)::

    debian-observer ingest britney --last 24 --raw-dir data/raw
    python3 benchmarks/parse_queue_bench.py --raw-dir data/raw \
        --nats-url nats://huevo-c4-nats:4222 --out-dir benchmarks/results \
        --experiments e1,e2,e3,e4,e5
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_run  # noqa: E402  the generic runner

from debian_observer.messaging.client import NATSClient
from debian_observer.messaging.parse_queue import (  # noqa: E402
    PARSE_STREAM_NAME,
    RAW_BUCKET,
    RESULTS_BUCKET,
    ParseJob,
    enqueue_snapshots,
    ensure_bucket,
    ensure_parse_queue,
    raw_object_name,
    result_object_name,
)
from debian_observer.normalization.britney import parse_snapshot
from debian_observer.normalization.eventizers.excuses import ExcusesEventizer
from debian_observer.ingestion.britney import sha256_bytes

#: The work-queue subjects and buckets are the module's constants —
#: the experiments must not invent their own.
WORKER_COMMAND = "debian-observer work britney"


def _banner(title: str) -> str:
    return f"\n{'=' * 72}\n{title}\n{'=' * 72}\n"


def _env_header() -> str:
    lines = [
        f"host: {platform.node()}",
        f"cpus: {os.cpu_count()}",
        f"python: {platform.python_version()}",
    ]
    for package in ("nats-py", "pyarrow", "PyYAML"):
        try:
            from importlib.metadata import version

            lines.append(f"{package}: {version(package)}")
        except Exception:
            pass
    return "\n".join(lines)


def load_window(raw_dir: Path) -> tuple[dict, list[tuple[str, bytes, str | None]]]:
    """The newest window manifest and its (snapshot_id, raw bytes, url)."""
    year_dirs = sorted((raw_dir / "britney").glob("*"))
    manifests: list[Path] = []
    for year_dir in year_dirs:
        manifests.extend(year_dir.glob("manifest-*.json"))
    if not manifests:
        raise SystemExit(f"no britney window manifest under {raw_dir}/britney/")
    newest = max(manifests, key=lambda p: p.stat().st_mtime)
    manifest = json.loads(newest.read_text(encoding="utf-8"))
    year = str(manifest.get("year"))
    window = []
    for entry in manifest.get("snapshots", []):
        snapshot_id = entry["snapshot_id"]
        path = raw_dir / "britney" / year / f"{snapshot_id}.yaml.gz"
        window.append((snapshot_id, path.read_bytes(), entry.get("url")))
    return manifest, window


# ---------------------------------------------------------------------------
# E1 — parse cost per snapshot; compact-state size
# ---------------------------------------------------------------------------


def experiment_e1(window: Sequence[tuple[str, bytes, str | None]], out: Path) -> dict[str, bytes]:
    """Parse every snapshot serially: time and compact-state size each."""
    from debian_observer.messaging.parse_worker import compact_state_parquet_bytes

    lines = [_banner("E1 — parse cost per snapshot; compact-state size")]
    lines.append(_env_header())
    lines.append(f"window: {len(window)} observed snapshots")
    lines.append("run: serial parse of each raw .yaml.gz (the queue's unit of work)")
    compact: dict[str, bytes] = {}
    times: dict[str, float] = {}
    for snapshot_id, gz, url in sorted(window):
        start = time.perf_counter()
        parsed = parse_snapshot(snapshot_id, gz, url=url)
        state = compact_state_parquet_bytes(parsed)
        times[snapshot_id] = time.perf_counter() - start
        compact[snapshot_id] = state
        lines.append(
            f"{snapshot_id}  items={len(parsed.states)}  "
            f"parse={times[snapshot_id]:.2f}s  "
            f"raw={len(gz)}B  compact={len(state)}B"
        )
    total = sum(times.values())
    ordered = sorted(times.values())
    lines.append(
        f"total={total:.1f}s  mean={total / len(times):.2f}s  "
        f"min={ordered[0]:.2f}s  max={ordered[-1]:.2f}s  "
        f"compact mean={sum(len(b) for b in compact.values()) / len(compact):.0f}B"
    )
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return compact


# ---------------------------------------------------------------------------
# E2 — pair-eventizer cost per pair
# ---------------------------------------------------------------------------


def experiment_e2(window: Sequence[tuple[str, bytes, str | None]], out: Path) -> None:
    """Eventize the window: the seed, then each pair, timed."""
    parsed = [parse_snapshot(sid, gz, url=url) for sid, gz, url in sorted(window)]
    eventizer = ExcusesEventizer()
    lines = [_banner("E2 — pair-eventizer cost per pair (ADR-027's assumption)")]
    lines.append(_env_header())
    lines.append(f"window: {len(parsed)} parsed snapshots")
    ordered = eventizer.order_window(parsed)
    costs: list[float] = []
    start = time.perf_counter()
    seed = list(eventizer.eventize_seed(ordered[0].states, snapshot=ordered[0].metadata))
    costs.append(time.perf_counter() - start)
    lines.append(f"seed {ordered[0].metadata.snapshot_id}: {len(seed)} events, {costs[0]*1000:.1f} ms")
    for prev, nxt in zip(ordered, ordered[1:]):
        start = time.perf_counter()
        events = list(
            eventizer.eventize_pair(
                prev.states, nxt.states, prev=prev.metadata, next=nxt.metadata
            )
        )
        costs.append(time.perf_counter() - start)
        lines.append(
            f"pair {prev.metadata.snapshot_id} -> {nxt.metadata.snapshot_id}: "
            f"{len(events)} events, {costs[-1]*1000:.1f} ms"
        )
    lines.append(
        f"pairs={len(costs) - 1}  pair mean={sum(costs[1:]) / max(1, len(costs) - 1)*1000:.1f} ms  "
        f"pair max={max(costs[1:])*1000:.1f} ms"
    )
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# The queue runs (E3/E4/E5): enqueue, workers, wait for the results
# ---------------------------------------------------------------------------


async def _queue_run(
    window: Sequence[tuple[str, bytes, str | None]],
    *,
    nats_url: str,
    slots: int,
    workers: int,
    kill_first_worker_after_first_result: bool = False,
    expected: dict[str, bytes] | None = None,
    timeout_seconds: int = 1800,
) -> dict[str, Any]:
    """One distributed-parse run over the window; its measurements.

    Enqueues the window (idempotent), starts ``workers`` worker
    processes of ``slots`` slots each, waits for every compact state
    to be back, then signals them. With
    ``kill_first_worker_after_first_result`` the first worker is
    SIGKILLed once one result is in (the §15 worker-dies row).
    """
    from debian_observer.messaging.parse_queue import RawSnapshot

    client = NATSClient(servers=[nats_url])
    await client.connect()
    js = client.jetstream
    try:
        await ensure_parse_queue(client)
        results = await ensure_bucket(client, RESULTS_BUCKET)
        # A clean slate per run: the previous run's objects are gone.
        for snapshot_id, _, _ in window:
            try:
                await results.delete(result_object_name(snapshot_id))
            except Exception:
                pass  # not there yet
        raw = [
            RawSnapshot(snapshot_id=sid, gz_bytes=gz, url=url)
            for sid, gz, url in window
        ]
        enqueue_start = time.perf_counter()
        stats = await enqueue_snapshots(client, raw)
        enqueue_wall = time.perf_counter() - enqueue_start

        logs = "/tmp/bench-worker-logs"
        os.makedirs(logs, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%H%M%S")
        processes = []
        for i in range(workers):
            log = open(f"{logs}/worker-{stamp}-{i}.log", "w", encoding="utf-8")
            process = subprocess.Popen(
                [
                    "debian-observer",
                    "work",
                    "britney",
                    "--nats-url",
                    nats_url,
                    "--concurrency",
                    str(slots),
                    "--drain",
                    "0",
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            processes.append((process, log))
        pids = [p.pid for p, _ in processes]

        killed = None
        start = time.perf_counter()

        async def present() -> int:
            count = 0
            for snapshot_id, _, _ in window:
                try:
                    await results.get_info(result_object_name(snapshot_id))
                    count += 1
                except Exception:
                    pass
            return count

        try:
            while True:
                count = await present()
                if (
                    kill_first_worker_after_first_result
                    and killed is None
                    and count >= 1
                and processes
                ):
                    processes[0][0].send_signal(signal.SIGKILL)
                    killed = processes[0][0].pid
                    await asyncio.sleep(0)
                if count >= len(window):
                    break
                if time.perf_counter() - start > timeout_seconds:
                    raise SystemExit("timeout waiting for the parse results")
                await asyncio.sleep(0.5)
            wall = time.perf_counter() - start
        finally:
            for process, log in processes:
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
            time.sleep(1.0)
            for process, log in processes:
                if process.poll() is None:  # drain handler missed it
                    process.kill()
                log.close()

        # The compact states came back: complete, and identical to the
        # serial parse (E1) when the caller pinned it.
        identical = None
        if expected is not None:
            identical = True
            for snapshot_id, _, _ in window:
                got = (await results.get(result_object_name(snapshot_id))).data
                if got != expected[snapshot_id]:
                    identical = False
        return {
            "slots": slots,
            "workers": workers,
            "window": len(window),
            "enqueue_wall": enqueue_wall,
            "jobs": stats.jobs,
            "duplicates_absorbed": stats.duplicates_absorbed,
            "raw_bytes": stats.raw_bytes_put,
            "wall": wall,
            "killed_worker_pid": killed,
            "identical_to_serial": identical,
        }
    finally:
        await client.close()


def _queue_lines(title: str, run: dict[str, Any], nats_url: str) -> list[str]:
    lines = [_banner(title)]
    lines.append(_env_header())
    lines.append(f"stream: {PARSE_STREAM_NAME} at {nats_url}")
    lines.append(
        f"command shape: {WORKER_COMMAND} --concurrency <slots> --drain 0 "
        "(started by the driver, signalled when the window is complete)"
    )
    lines.append(
        f"slots={run['slots']} workers={run['workers']} window={run['window']}  "
        f"enqueue={run['enqueue_wall']:.1f}s jobs={run['jobs']} "
        f"(duplicates absorbed {run['duplicates_absorbed']})  raw={run['raw_bytes']}B"
    )
    lines.append(f"wall (last enqueue -> all results back): {run['wall']:.1f}s")
    if run.get("killed_worker_pid"):
        lines.append(f"worker killed mid-run (SIGKILL): pid {run['killed_worker_pid']}")
    if run.get("identical_to_serial") is not None:
        lines.append(f"results identical to the serial parse: {run['identical_to_serial']}")
    return lines


async def main_async(args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest, window = load_window(Path(args.raw_dir))
    experiments = [e.strip() for e in args.experiments.split(",")]
    print(f"window: {len(window)} observed snapshots "
          f"(manifest {manifest.get('run_id')}, year {manifest.get('year')})")
    expected: dict[str, bytes] | None = None
    if "e1" in experiments:
        print("E1 ...")
        expected = experiment_e1(window, out_dir / "e1-parse-cost.txt")
    if "e2" in experiments:
        print("E2 ...")
        experiment_e2(window, out_dir / "e2-pair-cost.txt")
    if "e3" in experiments:
        lines = [_banner("E3 — backfill scaling: 1, 2, 4 workers on one machine")]
        for slots in (1, 2, 4):
            print(f"E3 slots={slots} ...")
            run = await _queue_run(window, nats_url=args.nats_url, slots=slots, workers=1, expected=expected)
            lines.extend(_queue_lines(f"E3 — {slots} worker slot(s)", run, args.nats_url))
        lines.append(
            "across machines: PENDING — no second machine in this\n"
            "environment; the deployment shape (a worker in its own\n"
            "container against the same NATS) is exercised by every run\n"
            "here, the multi-host half stays to run where two hosts exist."
        )
        (out_dir / "e3-backfill-scaling.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    if "e4" in experiments:
        print("E4 ...")
        lines = [_banner("E4 — the failure run: a worker dies mid-job")]
        run = await _queue_run(
            window,
            nats_url=args.nats_url,
            slots=2,
            workers=2,
            kill_first_worker_after_first_result=True,
            expected=expected,
        )
        lines.extend(_queue_lines("E4 — 2 workers, one SIGKILLed mid-run", run, args.nats_url))
        lines.append(
            "expected (§15): the killed worker's in-flight job is\n"
            "redelivered after ack_wait to the other worker; the final\n"
            "output is complete and identical to the clean serial parse."
        )
        (out_dir / "e4-failure-runs.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    if "e5" in experiments:
        lines = [_banner("E5 — the parallel ceiling: CPU contention vs memory")]
        for slots in (1, 2, 4, 8):
            print(f"E5 slots={slots} ...")
            run = await _queue_run(window, nats_url=args.nats_url, slots=slots, workers=1, expected=expected)
            lines.extend(_queue_lines(f"E5 — {slots} worker slot(s)", run, args.nats_url))
        lines.append(
            "the ceiling: wall stops improving when the slots exceed\n"
            "the CPUs (contention) or the peak RSS approaches the host\n"
            "memory — read both columns above against the host header."
        )
        (out_dir / "e5-parallel-ceiling.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("done")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--nats-url", default="nats://localhost:4222")
    parser.add_argument("--out-dir", default="benchmarks/results")
    parser.add_argument("--experiments", default="e1,e2,e3,e4,e5")
    args = parser.parse_args()
    asyncio.run(main_async(args))
    return 0


if __name__ == "__main__":
    sys.exit(main())