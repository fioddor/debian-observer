"""The britney acquisition: fetch once, keep bytes, record gaps.

`ingest britney` downloads every listed snapshot of its window into
the raw store, politely (docs/release3.project.v0.1.md §4.1.3), and
describes the window in a manifest with gaps and failures as data
(§4.2, ADR-002). These pins hold that contract with gzip files served
by an embedded local HTTP server and injected sleepers — the battery
never sleeps for real and never reaches release.debian.org.
"""

from __future__ import annotations

import gzip
import json
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from debian_observer.cli.main import cli
from debian_observer.ingestion import britney

# ---------------------------------------------------------------------------
# Synthetic, deterministic snapshots
# ---------------------------------------------------------------------------

GENERATED_DATE_KEY = "generated-date"
SOURCES_KEY = "sources"


@dataclass
class ListedSnapshot:
    snapshot_id: str
    body: bytes  # the gz bytes as served
    items: list[str]
    generated_date: str


def listed(
    snapshot_id: str, items: list[str], generated_date: str
) -> ListedSnapshot:
    """One excuses document: a generated date and its item names."""
    doc = {
        GENERATED_DATE_KEY: generated_date,
        SOURCES_KEY: [{"item-name": name} for name in items],
    }
    return ListedSnapshot(
        snapshot_id=snapshot_id,
        body=gzip.compress(yaml.safe_dump(doc).encode()),
        items=items,
        generated_date=generated_date,
    )


# A window of three hourly snapshots, one item each.
SNAPSHOTS = [
    listed("20260601T010000Z_0001", ["alpha"], "2026-06-01T01:00:00Z"),
    listed("20260601T020000Z_0001", ["beta"], "2026-06-01T02:00:00Z"),
    listed("20260601T030000Z_0001", ["gamma"], "2026-06-01T03:00:00Z"),
]
# A window with a hole: nothing between 09:00 and 21:00 — a period with
# no published snapshot (§4.2), wider than the gap threshold.
GAPPY = [
    listed("20260601T090000Z_0001", ["alpha"], "2026-06-01T09:00:00Z"),
    listed("20260601T210000Z_0001", ["beta"], "2026-06-01T21:00:00Z"),
]
# One item twice: an invalid record, never a silent overwrite (§5.2).
DUPLICATE = listed(
    "20260601T040000Z_0001", ["alpha", "alpha"], "2026-06-01T04:00:00Z"
)
# Same generated-date as the 02:00 snapshot: a tie (§5.2).
TIED = listed("20260601T050000Z_0001", ["delta"], "2026-06-01T02:00:00Z")


def listed_unquoted(
    snapshot_id: str, generated_date: str, items: list[str]
) -> ListedSnapshot:
    """A snapshot whose generated-date YAML loads as a datetime.

    The real source serves the timestamp unquoted: YAML loads it as
    a datetime, exactly what the manifest must serialize (§4.2). The
    safe_dumped fixtures above keep it a string.
    """
    lines = [f"generated-date: {generated_date}", "sources:"]
    lines += [f"- {{item-name: {name}}}" for name in items]
    body = gzip.compress("\n".join(lines).encode())
    return ListedSnapshot(
        snapshot_id=snapshot_id,
        body=body,
        items=items,
        generated_date=generated_date,
    )


def index_html(entries: list[str]) -> str:
    """A directory listing naming exactly the given snapshots."""
    rows = "\n".join(f'<a href="{s}.yaml.gz">{s}.yaml.gz</a>' for s in entries)
    return f"<html><body>{rows}</body></html>"


def index_entries(snapshots: list[ListedSnapshot]) -> str:
    return index_html([s.snapshot_id for s in snapshots])


# ---------------------------------------------------------------------------
# Embedded local source server
# ---------------------------------------------------------------------------


class FakeSource:
    """An embedded britney source: index + gzip files, request counter.

    Serves at 127.0.0.1 only; every request is recorded. A file in
    ``broken`` is served as bytes that do not decompress.
    """

    def __init__(
        self,
        snapshots: list[ListedSnapshot],
        *,
        broken: set[str] | None = None,
    ) -> None:
        self.files = {s.snapshot_id + ".yaml.gz": s.body for s in snapshots}
        self.broken = {b + ".yaml.gz" for b in (broken or set())}
        self.requests: list[str] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                outer.requests.append(self.path)
                name = self.path.rsplit("/", 1)[-1]
                if name not in outer.files:
                    self.send_error(404, "no such snapshot")
                    return
                body = (
                    b"not-gzip-at-all" if name in outer.broken
                    else outer.files[name]
                )
                self.send_response(200)
                self.send_header("ETag", f'"etag-{name}"')
                self.send_header("Last-Modified", "Mon, 01 Jun 2026 01:00:00 GMT")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args: object) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True
        )
        self.thread.start()

    def index_url(self) -> str:
        return self.url + "/"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


@contextmanager
def local_source(
    snapshots: list[ListedSnapshot], *, broken: set[str] | None = None
) -> Iterator[FakeSource]:
    """An embedded source serving exactly the given snapshots."""
    fake = FakeSource(snapshots, broken=broken)
    try:
        yield fake
    finally:
        fake.close()


def window_refs(source: FakeSource, snapshots: list[ListedSnapshot]) -> list:
    """Refs over the embedded server, in listing order."""
    return britney.parse_index(
        index_entries(snapshots), base_url=source.index_url()
    )


def nosleep() -> Any:
    """A sleeper that records nothing and never sleeps."""
    def sleep(seconds: float) -> None:
        pass
    return sleep


def acquire(
    refs: list,
    source: FakeSource,
    tmp_path: Path,
    **kwargs: Any,
) -> Any:
    """acquire_window over the embedded source, never sleeping for real."""
    kwargs.setdefault("sleep", nosleep())
    return britney.acquire_window(refs, year=2026, raw_dir=tmp_path / "raw", **kwargs)


# ---------------------------------------------------------------------------
# Fetch once, keep bytes
# ---------------------------------------------------------------------------


def test_snapshots_are_stored_byte_preserving(tmp_path: Path) -> None:
    """Each file is downloaded once and kept as served: raw gz bytes.

    The manifest entry carries the URL, the retrieval time, the HTTP
    validators, the size and the SHA-256 of the stored bytes (§4.2).
    """
    with local_source(SNAPSHOTS) as source:
        refs = window_refs(source, SNAPSHOTS)
        result = acquire(refs, source, tmp_path)

        assert len(result.fetched) == len(SNAPSHOTS)
        entries = result.manifest["snapshots"]
        assert [e["snapshot_id"] for e in entries] == [
            s.snapshot_id for s in SNAPSHOTS
        ]
        for snapshot, entry in zip(SNAPSHOTS, entries):
            stored = (
                tmp_path / "raw" / britney.RAW_SUBDIR / "2026"
                / (snapshot.snapshot_id + ".yaml.gz")
            )
            assert stored.read_bytes() == snapshot.body
            assert entry["url"] == source.url + "/" + snapshot.snapshot_id + ".yaml.gz"
            assert entry["size_bytes"] == len(snapshot.body)
            assert entry["sha256"] == britney.sha256_bytes(snapshot.body)
            assert entry["etag"] == f'"etag-{snapshot.snapshot_id}.yaml.gz"'
            assert entry["retrieved_at"]


def test_reruns_skip_by_sha256(tmp_path: Path) -> None:
    """A re-run reads the store: nothing is downloaded twice.

    Idempotence is by SHA-256: the same window yields the same
    checksums, and the source sees no second request.
    """
    with local_source(SNAPSHOTS) as source:
        refs = window_refs(source, SNAPSHOTS)
        first = acquire(refs, source, tmp_path)
        requests_after_first = len(source.requests)
        second = acquire(refs, source, tmp_path)

        assert len(source.requests) == requests_after_first
        assert not second.fetched
        assert sorted(second.skipped) == sorted(s.snapshot_id for s in SNAPSHOTS)
        assert [e["sha256"] for e in second.manifest["snapshots"]] == [
            e["sha256"] for e in first.manifest["snapshots"]
        ]


def test_requests_are_polite(tmp_path: Path) -> None:
    """One request at a time, at least the interval apart, never 0.

    The sleeper is injected: the battery measures the spacing, it never
    sleeps for real. A zero interval is refused before any request.
    """
    calls: list[float] = []

    def sleep(seconds: float) -> None:
        calls.append(seconds)

    with local_source(SNAPSHOTS) as source:
        refs = window_refs(source, SNAPSHOTS)
        britney.acquire_window(
            refs,
            year=2026,
            raw_dir=tmp_path / "raw",
            min_interval=1.5,
            sleep=sleep,
        )
        # Every request but the first waits for the rest of the interval.
        assert len(calls) >= len(SNAPSHOTS) - 1
        assert all(0 < wait <= 1.5 for wait in calls)
        # A zero interval is never allowed: the source is a public service.
        with pytest.raises(ValueError):
            britney.acquire_window(
                refs, year=2026, raw_dir=tmp_path / "raw",
                min_interval=0, sleep=sleep,
            )


def test_a_failure_is_not_a_gap(tmp_path: Path) -> None:
    """A listed file that cannot be retrieved is FAILED, not a gap.

    The failure is retried with backoff, then recorded with its cause;
    a gap is a period with no published snapshot — the two never
    confuse (§4.2).
    """
    with local_source(SNAPSHOTS) as source:
        refs = window_refs(source, SNAPSHOTS)
        missing = SNAPSHOTS[1].snapshot_id
        source.files.pop(missing + ".yaml.gz")

        result = acquire(refs, source, tmp_path, attempts=2)

        assert [f.snapshot_id for f in result.failures] == [missing]
        assert "404" in result.failures[0].error
        assert result.manifest["failures"][0]["snapshot_id"] == missing
        assert result.manifest["gaps"] == []
        # The failure was retried: the source saw it twice.
        assert source.requests.count("/" + missing + ".yaml.gz") == 2


def test_an_empty_window_is_a_valid_result(tmp_path: Path) -> None:
    """A window that selects nothing is a result, never a crash.

    An index can legitimately list no snapshot of the window (a year
    with nothing published in the bounds): the run still describes
    the empty window — 0 snapshots, 0 gaps, 0 failures — with the
    year's index URL in its manifest.
    """
    with local_source(SNAPSHOTS) as source:
        result = acquire([], source, tmp_path)

    assert result.fetched == []
    assert result.skipped == []
    assert result.failures == []
    assert result.manifest["snapshots"] == []
    assert result.manifest["gaps"] == []
    assert result.manifest["index"]["url"] == britney.index_url(2026)
    assert result.manifest_path.exists()
    assert json.loads(result.manifest_path.read_text())["snapshots"] == []


def test_a_truncated_gzip_is_a_failure(tmp_path: Path) -> None:
    """A gzip that does not decompress never enters the store."""
    with local_source(SNAPSHOTS, broken={SNAPSHOTS[2].snapshot_id}) as source:
        refs = window_refs(source, SNAPSHOTS)

        result = acquire(refs, source, tmp_path)

        assert [f.snapshot_id for f in result.failures] == [SNAPSHOTS[2].snapshot_id]
        stored = (
            tmp_path / "raw" / britney.RAW_SUBDIR / "2026"
            / (SNAPSHOTS[2].snapshot_id + ".yaml.gz")
        )
        assert not stored.exists()


def test_a_gap_is_recorded_as_data(tmp_path: Path) -> None:
    """A period with no published snapshot lands in the manifest.

    The gap spans consecutive observed snapshots further apart than
    the threshold; it is data, not an error.
    """
    with local_source(GAPPY) as source:
        refs = window_refs(source, GAPPY)

        result = acquire(refs, source, tmp_path)

        assert not result.failures
        assert len(result.manifest["gaps"]) == 1
        gap = result.manifest["gaps"][0]
        assert gap["start"] == GAPPY[0].generated_date
        assert gap["end"] == GAPPY[1].generated_date
        assert gap["seconds"] >= britney.GAP_THRESHOLD_SECONDS


# ---------------------------------------------------------------------------
# Snapshot metadata (the minimal YAML read)
# ---------------------------------------------------------------------------


def test_snapshot_metadata_is_read_from_the_yaml(tmp_path: Path) -> None:
    """generated-date, item count and top-level keys land in the manifest.

    The read is the manifest's metadata only (§4.2): the parse is the
    work queue's job, not the acquisition's.
    """
    with local_source(SNAPSHOTS) as source:
        refs = window_refs(source, SNAPSHOTS)

        result = acquire(refs, source, tmp_path)

        first = result.manifest["snapshots"][0]
        assert first["generated_date"] == SNAPSHOTS[0].generated_date
        assert first["item_count"] == len(SNAPSHOTS[0].items)
        assert first["top_level_keys"] == sorted({GENERATED_DATE_KEY, SOURCES_KEY})
        assert result.manifest["loader"] in ("CSafeLoader", "SafeLoader")


def test_a_duplicate_item_name_is_rejected_with_cause(tmp_path: Path) -> None:
    """A duplicate item-name is an invalid record, never a silent overwrite.

    The snapshot is recorded as failed with its cause: the duplicated
    name (§5.2).
    """
    with local_source([DUPLICATE]) as source:
        refs = window_refs(source, [DUPLICATE])

        result = acquire(refs, source, tmp_path)

        assert [f.snapshot_id for f in result.failures] == [DUPLICATE.snapshot_id]
        assert DUPLICATE.items[0] in result.failures[0].error
        assert "duplicate" in result.failures[0].error


def test_a_generated_date_tie_is_an_error(tmp_path: Path) -> None:
    """Two snapshots of one window sharing a generated-date fail loudly.

    Snapshots are totally ordered by generated_date (§5.2): a tie
    cannot be ordered, and nothing is silently picked.
    """
    with local_source(SNAPSHOTS + [TIED]) as source:
        refs = window_refs(source, SNAPSHOTS + [TIED])

        with pytest.raises(britney.GeneratedDateTie):
            acquire(refs, source, tmp_path)


def test_the_manifest_serializes_a_parsed_generated_date(tmp_path: Path) -> None:
    """A generated-date YAML loads as a datetime lands as ISO in JSON.

    The real source serves the timestamp unquoted, so YAML hands the
    acquisition a datetime; the manifest is JSON, and every datetime
    that flows into it (each snapshot's generated-date) is serialized
    as an ISO 8601 UTC string — the manifest on disk round-trips
    through json.loads (§4.2), no TypeError.
    """
    snapshots = [
        listed_unquoted("20260601T010000Z_0001", "2026-06-01 01:00:00", ["alpha"])
    ]
    with local_source(snapshots) as source:
        refs = window_refs(source, snapshots)
        result = acquire(refs, source, tmp_path)

    entry = result.manifest["snapshots"][0]
    assert entry["generated_date"] == "2026-06-01T01:00:00Z"
    assert json.loads(json.dumps(result.manifest)) == result.manifest
    assert json.loads(result.manifest_path.read_text()) == result.manifest


# ---------------------------------------------------------------------------
# Window manifest and execution record
# ---------------------------------------------------------------------------


def test_the_manifest_describes_the_window(tmp_path: Path) -> None:
    """The manifest carries the source, the coordinates of the excuses,
    the index it was built from, and the run's failures (§4.2, §4.3)."""
    with local_source(SNAPSHOTS) as source:
        refs = window_refs(source, SNAPSHOTS)
        source.files.pop(SNAPSHOTS[1].snapshot_id + ".yaml.gz")

        result = acquire(refs, source, tmp_path)

        manifest = result.manifest
        assert manifest["source"] == "britney"
        assert manifest["coordinates"]["distribution"] == "debian"
        assert manifest["coordinates"]["from_suite"] == "unstable"
        assert manifest["coordinates"]["to_suite"] == "testing"
        assert manifest["index"]["url"] == source.index_url()
        assert manifest["index"]["listed"] == len(refs)
        assert len(manifest["failures"]) == 1
        assert result.manifest_path.exists()
        assert json.loads(result.manifest_path.read_text())["source"] == "britney"


def test_one_execution_record_per_run(tmp_path: Path) -> None:
    """Every run writes one ExecutionRecord with its file counts:
    fetched, skipped, failed, gaps (§8)."""
    import duckdb

    with local_source(SNAPSHOTS) as source:
        refs = window_refs(source, SNAPSHOTS)
        result = acquire(refs, source, tmp_path, run_id="run-britney")

        db = tmp_path / "execution_runs.duckdb"
        con = duckdb.connect(str(db), read_only=True)
        try:
            rows = con.execute(
                "SELECT run_id, execution_type, source_name, metadata "
                "FROM execution_runs WHERE run_id = ?",
                ["run-britney"],
            ).fetchall()
        finally:
            con.close()
        assert len(rows) == 1
        run_id, execution_type, source_name, metadata = rows[0]
        assert (run_id, execution_type, source_name) == (
            "run-britney", "ingestion", "britney"
        )
        counts = json.loads(metadata)
        assert counts["fetched"] == len(SNAPSHOTS)
        assert counts["failed"] == len(result.failures)
        assert counts["gaps"] == len(result.manifest["gaps"])


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def test_ingest_britney_help_documents_the_politeness_defaults() -> None:
    """The help shows the fetch perimeter: once, politely, gaps, failures."""
    result = CliRunner().invoke(cli, ["ingest", "britney", "--help"])
    assert result.exit_code == 0
    for word in (
        "--codename", "--min-interval", "--timeout",
        "--attempts", "--backoff-base", "SHA-256", "gap", "fail",
    ):
        assert word in result.output


def test_ingest_britney_acquires_the_window(tmp_path: Path) -> None:
    """The command fetches the window and echoes the counts and manifest."""
    with local_source(SNAPSHOTS) as source:
        refs = window_refs(source, SNAPSHOTS)
        with patch(
            "debian_observer.cli.commands.ingest.fetch_index"
        ) as fetch_index:
            fetch_index.return_value = britney.IndexResult(
                refs=refs,
                cache_path=tmp_path / "index.html",
                from_cache=False,
            )
            result = CliRunner().invoke(
                cli,
                [
                    "ingest", "britney",
                    "--year", "2026",
                    "--last", "2",
                    "--min-interval", "0.01",
                    "--raw-dir", str(tmp_path / "raw"),
                ],
            )

        assert result.exit_code == 0
        assert f"Fetched: {min(2, len(SNAPSHOTS))}" in result.output
        assert "Manifest:" in result.output
        assert "Gaps: " in result.output

def test_the_backoff_comes_from_the_knobs(tmp_path: Path) -> None:
    """The retry schedule comes from FETCH_ATTEMPTS and BACKOFF_BASE.

    The knobs are the module constants, never literals copied into the
    pin: a changed default changes the schedule this pin checks.
    """
    from debian_observer.ingestion.britney import BACKOFF_BASE, FETCH_ATTEMPTS

    waits: list[float] = []

    def sleep(seconds: float) -> None:
        waits.append(seconds)

    with local_source(SNAPSHOTS) as source:
        refs = window_refs(source, SNAPSHOTS)
        missing = SNAPSHOTS[1].snapshot_id
        source.files.pop(missing + ".yaml.gz")

        result = acquire(refs, source, tmp_path, min_interval=1, sleep=sleep)

        # One listed file failed after FETCH_ATTEMPTS tries, with the
        # BACKOFF_BASE exponential backoff between them.
        assert source.requests.count("/" + missing + ".yaml.gz") == FETCH_ATTEMPTS
        expected = [BACKOFF_BASE ** a * 1 for a in range(1, FETCH_ATTEMPTS)]
        assert all(wait in waits for wait in expected)
        assert [f.snapshot_id for f in result.failures] == [missing]


def test_the_cli_exposes_the_fetch_knobs(tmp_path: Path) -> None:
    """--timeout/--attempts/--backoff-base reach acquire_window.

    The defaults are the module constants (HTTP_TIMEOUT_SECONDS,
    FETCH_ATTEMPTS, BACKOFF_BASE), asserted as constants, never as
    literals copied into the pin; explicit values pass through.
    """
    from types import SimpleNamespace

    from debian_observer.ingestion.britney import (
        BACKOFF_BASE,
        FETCH_ATTEMPTS,
        HTTP_TIMEOUT_SECONDS,
    )

    fake_result = SimpleNamespace(
        fetched=[], skipped=[], failures=[],
        manifest={"gaps": [], "snapshots": [], "run_id": "run-knobs"},
        manifest_path=tmp_path / "m.json", year=2026,
    )
    index = SimpleNamespace(
        refs=[], cache_path=tmp_path / "i.html", from_cache=False
    )

    def run(extra: list[str]) -> Any:
        with patch(
            "debian_observer.cli.commands.ingest.fetch_index", return_value=index
        ), patch(
            "debian_observer.ingestion.britney.acquire_window",
            return_value=fake_result,
        ) as acquire_window:
            result = CliRunner().invoke(
                cli,
                ["ingest", "britney", "--year", "2026"]
                + extra
                + ["--raw-dir", str(tmp_path / "raw")],
            )
        return result, acquire_window

    result, acquire_window = run([])
    assert result.exit_code == 0
    assert acquire_window.call_args.kwargs["timeout"] == HTTP_TIMEOUT_SECONDS
    assert acquire_window.call_args.kwargs["attempts"] == FETCH_ATTEMPTS
    assert acquire_window.call_args.kwargs["backoff_base"] == BACKOFF_BASE

    result, acquire_window = run(
        ["--timeout", "7.5", "--attempts", "5", "--backoff-base", "3"]
    )
    assert result.exit_code == 0
    assert acquire_window.call_args.kwargs["timeout"] == 7.5
    assert acquire_window.call_args.kwargs["attempts"] == 5
    assert acquire_window.call_args.kwargs["backoff_base"] == 3.0
