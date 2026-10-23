#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-or-later
# Purpose: Crash-test the external consumer (publish → consume → kill -9 →
#          resume → parity with consolidate), spec §19.4/§20 steps 3-6.
# Usage..: scripts/crash-test-consumer.sh [--keep]
#          Run from the repo root (or anywhere: paths resolve from this file).
# Needs..: docker (deploy/compose NATS) — reused if a NATS already answers on
#          the server URL — and the repo venv (PATH=$PWD/.venv/bin:$PATH).
# Design: A fresh JetStream stream per run (unique name) so no prior state
#          pollutes the parity check. The consumer is killed with -9 while
#          messages are still pending; JetStream redelivers the unacked
#          messages; the merge absorbs the duplicates. PASS criteria:
#          1) pending > 0 at kill time, 2) pending == 0 after resume,
#          3) the streamed table == the consolidated (batch) table,
#          4) a republish changes no message (Nats-Msg-Id dedup).
# Author.: Igor Zubiaurre <igor@zubiaurre.es>
#-----------------------------------------------------------------------------

set -euo pipefail
REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$REPO_ROOT"
export PATH="$REPO_ROOT/.venv/bin:$PATH"

WORK=$(mktemp -d /tmp/crash-consumer.XXXXXX)
DATASET="$WORK/dataset"
EXPECTED="$WORK/expected"
LOG_DIR="$WORK/events"
RUN_ID=$(date +%s)
STREAM="crash-$RUN_ID"
DURABLE="crash-consumer-$RUN_ID"
SERVER_URL="${DEBIAN_OBSERVER_SERVER:-nats://localhost:4222}"
COMPOSE_FILE="deploy/compose/docker-compose.yml"
STARTED_NATS=""
N_EVENTS=1000

log() { printf '[crash-test] %s\n' "$*"; }
die() { printf '[crash-test] FAIL: %s\n' "$*" >&2; exit 1; }
cleanup() {
  [ -n "$STARTED_NATS" ] && docker compose -f "$COMPOSE_FILE" stop nats >/dev/null 2>&1 || true
  [ "${1:-}" = "--keep" ] || rm -rf "$WORK"
}
trap 'cleanup "${1:-}"' EXIT

nats_up() { curl -fsS http://localhost:8222/healthz >/dev/null 2>&1; }

# pending messages of our durable consumer, via the NATS monitoring API
pending() {
  curl -fsS "http://localhost:8222/jsz?consumers=true&streams=true&offset=0&limit=1000" \
    | python3 -c '
import json, sys
data = json.load(sys.stdin)
def walk(o):
    if isinstance(o, dict):
        if o.get("name") == "'"$DURABLE"'" and ("num_pending" in o or "num_ack_pending" in o):
            print(int(o.get("num_pending", 0)) + int(o.get("num_ack_pending", 0))); return True
        for v in o.values():
            if walk(v): return True
    elif isinstance(o, list):
        for v in o:
            if walk(v): return True
    return False
sys.exit(0 if walk(data) else 1)
'
}

# 1. NATS up (reuse a healthy server; otherwise compose up)
if nats_up; then
  log "NATS already answering on :8222 — reusing it"
else
  log "starting NATS (docker compose -f $COMPOSE_FILE up -d nats)"
  docker compose -f "$COMPOSE_FILE" up -d nats
  STARTED_NATS=1
  for _ in $(seq 1 30); do nats_up && break; sleep 1; done
fi
nats_up || die "NATS did not become healthy"

# 2. a small deterministic event log (no network)
log "building a $N_EVENTS-event log in $LOG_DIR"
python3 - "$LOG_DIR" "$N_EVENTS" <<'PYEOF'
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from debian_observer.events import EventEnvelope
from debian_observer.storage.parquet import ParquetWriter

NOW = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc)
log_dir, n = Path(sys.argv[1]), int(sys.argv[2])
writer = ParquetWriter(output_dir=log_dir)
events = []
for i in range(n):
    events.append(EventEnvelope(
        event_id=uuid4(), event_type="PackageVersionObserved",
        source="udd", entity_id=f"pkg{i}:amd64",
        payload={"package_id": f"pkg{i}:amd64", "source_package": f"pkg{i}",
                 "version": "1.0-1", "distribution": "debian", "release": "sid",
                 "suite": None, "architecture": "amd64", "component": "main",
                 "section": "editors", "priority": "optional"},
        observed_at=NOW, effective_at=NOW))
for start in range(0, n, 250):  # several files: one per run
    writer.write_events(events[start:start + 250],
                        ingestion_run_id=f"crash-run-{start}")
PYEOF
[ "$(ls "$LOG_DIR"/*.parquet | wc -l)" -ge 1 ] || die "no log files written"

# 3. publish the log
log "publishing the log to stream $STREAM"
debian-observer publish --events-dir "$LOG_DIR" --stream "$STREAM" --server "$SERVER_URL"

# 4. consumer up, kill -9 mid-flight, pending must be > 0 at the kill
log "launching the example consumer (durable $DURABLE)"
DEBIAN_OBSERVER_SERVER="$SERVER_URL" \
DEBIAN_OBSERVER_STREAM="$STREAM" \
DEBIAN_OBSERVER_DURABLE="$DURABLE" \
DEBIAN_OBSERVER_DATASET_DIR="$DATASET" \
  python examples/consume_packages.py &
CONSUMER_PID=$!
sleep 2
PENDING_BEFORE=""
for _ in $(seq 1 30); do
  if PENDING_BEFORE=$(pending 2>/dev/null); then break; fi
  sleep 1
done
[ -n "$PENDING_BEFORE" ] || die "consumer $DURABLE not visible in JetStream"
[ "$PENDING_BEFORE" -gt 0 ] || die "nothing pending at kill time (consumer too fast — raise N_EVENTS)"
log "pending before kill: $PENDING_BEFORE (>0 as required)"
kill -9 "$CONSUMER_PID" || true
wait "$CONSUMER_PID" 2>/dev/null || true
log "consumer killed -9"

# 5. resume: relaunch, drain to pending == 0
log "relaunching the consumer (redelivery)"
DEBIAN_OBSERVER_SERVER="$SERVER_URL" \
DEBIAN_OBSERVER_STREAM="$STREAM" \
DEBIAN_OBSERVER_DURABLE="$DURABLE" \
DEBIAN_OBSERVER_DATASET_DIR="$DATASET" \
  python examples/consume_packages.py &
CONSUMER_PID=$!
PENDING_AFTER=""
for _ in $(seq 1 120); do
  if PENDING_AFTER=$(pending 2>/dev/null); then
    [ "$PENDING_AFTER" -eq 0 ] && break
  fi
  sleep 1
done
kill "$CONSUMER_PID" 2>/dev/null || true
wait "$CONSUMER_PID" 2>/dev/null || true
[ "${PENDING_AFTER:-1}" -eq 0 ] || die "pending did not drain to 0 (last: ${PENDING_AFTER:-unknown})"
log "pending after resume: 0"

# 6. parity: the streamed table == the batch (consolidate) table
log "parity check: streamed table vs consolidate --events-dir"
debian-observer consolidate --events-dir "$LOG_DIR" --dataset-dir "$EXPECTED"
python3 - "$DATASET" "$EXPECTED" <<'PYEOF'
import sys
from pathlib import Path
import pyarrow.parquet as pq

streamed = Path(sys.argv[1]) / "package_versions.parquet"
expected = Path(sys.argv[2]) / "package_versions.parquet"
a = pq.read_table(streamed).to_pylist()
b = pq.read_table(expected).to_pylist()
assert a == b, (
    f"table mismatch: streamed {len(a)} rows != expected {len(b)} rows"
)
print(f"parity OK: {len(a)} rows identical")
PYEOF

# 7. duplicate delivery: republish is a no-op (Nats-Msg-Id dedup)
log "republishing the log (Nats-Msg-Id dedup)"
OUT=$(debian-observer publish --events-dir "$LOG_DIR" --stream "$STREAM" --server "$SERVER_URL")
echo "$OUT" | grep -q "^Duplicates absorbed: $N_EVENTS$" \
  || die "republish did not absorb all $N_EVENTS duplicates:\n$OUT"
log "republish absorbed all $N_EVENTS duplicates"

log "PASS: crash → redelivery → parity → dedup all verified"
[ "${1:-}" = "--keep" ] && log "artifacts kept in $WORK"