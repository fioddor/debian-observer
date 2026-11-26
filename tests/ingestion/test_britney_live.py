"""The britney live endpoint is followed politely.

`ingest britney --live` follows the live excuses endpoint with
conditional requests and a poll floor (docs/release3.project.v0.1.md
§4.1.3, §7.4): no more often than every 15 minutes, one request at a
time, backfilled from the archive first when --from is given. These
pins hold that contract against an embedded local HTTP server and
injected sleepers — the battery never sleeps for real and never
reaches release.debian.org.
"""

from __future__ import annotations

import gzip
import json
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator
from contextlib import contextmanager
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from debian_observer.cli.main import cli
from debian_observer.ingestion import britney

LIVE_PATH = "/excuses.yaml.gz"


def live_yaml(items: list[str], generated_date: str) -> bytes:
    """The live document: a generated date and its item names."""
    doc = {
        "generated-date": generated_date,
        "sources": [{"item-name": name} for name in items],
    }
    return gzip.compress(yaml.safe_dump(doc).encode())


def live_yaml_unquoted(items: list[str], generated_date: str) -> bytes:
    """The live document as the real endpoint serves it: unquoted.

    The real endpoint writes the generated-date without quotes, so
    YAML loads it as a datetime — exactly what the manifest must
    serialize (§4.2). safe_dump would quote it into a string.
    """
    lines = [f"generated-date: {generated_date}", "sources:"]
    lines += [f"- {{item-name: {name}}}" for name in items]
    return gzip.compress("\n".join(lines).encode())


@dataclass
class LiveSource:
    """An embedded live endpoint: one file, one ETag, request log.

    Serves 304 to a conditional request whose If-None-Match matches
    the current ETag; every request's headers are recorded.
    """

    body: bytes = b""
    etag: str = '"live-etag-1"'
    last_modified: str = "Mon, 01 Jun 2026 01:00:00 GMT"
    requests: list[dict[str, str]] = field(default_factory=list)

    def serve(self) -> None:
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                headers = {
                    key.lower(): value for key, value in self.headers.items()
                }
                outer.requests.append({"path": self.path, **headers})
                if self.path != LIVE_PATH:
                    self.send_error(404, "no live file")
                    return
                if headers.get("if-none-match") == outer.etag:
                    self.send_response(304)
                    self.send_header("ETag", outer.etag)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("ETag", outer.etag)
                self.send_header("Last-Modified", outer.last_modified)
                self.send_header("Content-Length", str(len(outer.body)))
                self.end_headers()
                self.wfile.write(outer.body)

            def log_message(self, *args: object) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}{LIVE_PATH}"
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True
        )
        self.thread.start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


@contextmanager
def live_source() -> Iterator[LiveSource]:
    fake = LiveSource(body=live_yaml(["alpha"], "2026-06-01T01:00:00Z"))
    fake.serve()
    try:
        yield fake
    finally:
        fake.close()


def nosleep() -> Any:
    def sleep(seconds: float) -> None:
        pass
    return sleep


# ---------------------------------------------------------------------------
# Conditional requests and the poll floor
# ---------------------------------------------------------------------------


def test_conditional_requests_skip_unchanged_states(tmp_path: Path) -> None:
    """A poll that answers 304 stores nothing and reuses the validators.

    The second request carries If-None-Match with the ETag of the
    first response; the state file keeps the validators between polls
    (§4.1.3).
    """
    polls: list[float] = []

    def sleep(seconds: float) -> None:
        polls.append(seconds)

    with live_source() as source:
        britney.follow_live(
            raw_dir=tmp_path / "raw",
            url=source.url,
            max_polls=2,
            sleep=sleep,
            min_interval=0.01,
        )

        # Two polls, both reached the source.
        assert len(source.requests) == 2
        # The second poll was conditional with the first's ETag.
        assert source.requests[1]["if-none-match"] == source.etag
        # The 304 stored no new snapshot.
        stored = list((tmp_path / "raw" / britney.RAW_SUBDIR / "live").glob("*.yaml.gz"))
        assert len(stored) == 1
        state = json.loads(
            (tmp_path / "raw" / britney.RAW_SUBDIR / "live" / "state.json")
            .read_text()
        )
        assert state["etag"] == source.etag
        assert state["last_modified"] == source.last_modified


def test_a_new_state_is_stored_with_its_metadata(tmp_path: Path) -> None:
    """A new generated-date is one more snapshot: stored, described.

    The live snapshot id derives from the generated date; the entry
    carries the same manifest metadata as an archive snapshot
    (§7.4: archive and live share every step after acquisition).
    """
    with live_source() as source:
        result = britney.follow_live(
            raw_dir=tmp_path / "raw",
            url=source.url,
            max_polls=1,
            sleep=nosleep(),
            min_interval=0.01,
        )

        assert len(result.fetched) == 1
        snapshot_id = result.fetched[0]
        assert snapshot_id.startswith("live-")
        stored = (
            tmp_path / "raw" / britney.RAW_SUBDIR / "live"
            / (snapshot_id + ".yaml.gz")
        )
        assert stored.read_bytes() == source.body
        entry = result.manifest["snapshots"][0]
        assert entry["sha256"] == britney.sha256_bytes(source.body)
        assert entry["generated_date"] == "2026-06-01T01:00:00Z"
        assert entry["item_count"] == 1
        assert entry["url"] == source.url


def test_the_live_manifest_serializes_a_parsed_generated_date(
    tmp_path: Path,
) -> None:
    """A generated-date YAML loads as a datetime lands as ISO in JSON.

    The real endpoint serves the timestamp unquoted, so YAML hands the
    follower a datetime; the manifest is JSON, and the follower
    serializes it as an ISO 8601 UTC string — the whole manifest
    round-trips through json.loads (§4.2), no TypeError, and the
    manifest file is written to disk.
    """
    fake = LiveSource(body=live_yaml_unquoted(["alpha"], "2026-06-01 01:00:01"))
    fake.serve()
    try:
        result = britney.follow_live(
            raw_dir=tmp_path / "raw",
            url=fake.url,
            max_polls=1,
            sleep=nosleep(),
            min_interval=0.01,
        )
    finally:
        fake.close()

    entry = result.manifest["snapshots"][0]
    assert entry["generated_date"] == "2026-06-01T01:00:01Z"
    assert entry["item_count"] == 1
    assert json.loads(json.dumps(result.manifest)) == result.manifest
    assert json.loads(result.manifest_path.read_text()) == result.manifest


def test_the_poll_floor_is_enforced(tmp_path: Path) -> None:
    """The poll is never more often than every 15 minutes (§4.1.3)."""
    with pytest.raises(ValueError):
        britney.follow_live(
            raw_dir=tmp_path / "raw",
            url="http://127.0.0.1:1" + LIVE_PATH,
            poll_interval=60,
            max_polls=1,
            sleep=nosleep(),
            min_interval=0.01,
        )


def test_polls_wait_the_interval(tmp_path: Path) -> None:
    """Between two polls the follower waits the whole interval."""
    waits: list[float] = []

    def sleep(seconds: float) -> None:
        waits.append(seconds)

    with live_source() as source:
        britney.follow_live(
            raw_dir=tmp_path / "raw",
            url=source.url,
            poll_interval=britney.MIN_LIVE_POLL_INTERVAL,
            max_polls=2,
            sleep=sleep,
            min_interval=0.01,
        )

    assert waits  # the injected sleeper saw the waits
    poll_waits = [w for w in waits if w >= britney.MIN_LIVE_POLL_INTERVAL]
    assert len(poll_waits) >= 1  # one full interval between the two polls


def test_one_execution_record_for_the_live_run(tmp_path: Path) -> None:
    """The live run writes one ExecutionRecord, like an archive run (§8)."""
    import duckdb

    with live_source() as source:
        britney.follow_live(
            raw_dir=tmp_path / "raw",
            url=source.url,
            max_polls=1,
            sleep=nosleep(),
            min_interval=0.01,
            run_id="run-live",
        )

        con = duckdb.connect(str(tmp_path / "execution_runs.duckdb"), read_only=True)
        try:
            rows = con.execute(
                "SELECT run_id, execution_type, source_name, metadata "
                "FROM execution_runs WHERE run_id = ?",
                ["run-live"],
            ).fetchall()
        finally:
            con.close()
        assert len(rows) == 1
        run_id, execution_type, source_name, metadata = rows[0]
        assert (run_id, execution_type, source_name) == (
            "run-live", "ingestion", "britney"
        )
        counts = json.loads(metadata)
        assert counts["live"] is True


# ---------------------------------------------------------------------------
# CLI: --live and the backfill
# ---------------------------------------------------------------------------


def test_ingest_britney_help_documents_live() -> None:
    """The help shows --live and its poll floor."""
    result = CliRunner().invoke(cli, ["ingest", "britney", "--help"])
    assert result.exit_code == 0
    assert "--live" in result.output
    assert "--poll-interval" in result.output
    assert "15 min" in result.output


def test_live_with_from_backfills_first(tmp_path: Path) -> None:
    """`--live --from X` backfills the archive window, then follows."""
    from types import SimpleNamespace

    fake_result = SimpleNamespace(
        fetched=[], skipped=[], failures=[],
        manifest={"gaps": [], "snapshots": [], "run_id": "run-backfill"},
        manifest_path=tmp_path / "m.json", year=2026,
    )
    fake_live = {"fetched": [], "manifest": {"gaps": [], "snapshots": []}}

    calls: list[str] = []

    def fake_acquire(*args: object, **kwargs: object) -> object:
        calls.append("acquire")
        return fake_result

    def fake_follow(**kwargs: object) -> object:
        calls.append("follow")
        return fake_live

    index = SimpleNamespace(refs=[], cache_path=tmp_path / "i.html", from_cache=False)

    with patch(
        "debian_observer.cli.commands.ingest.fetch_index", return_value=index
    ), patch(
        "debian_observer.ingestion.britney.acquire_window", side_effect=fake_acquire
    ), patch(
        "debian_observer.ingestion.britney.follow_live", side_effect=fake_follow
    ):
        result = CliRunner().invoke(
            cli,
            [
                "ingest", "britney",
                "--live",
                "--from", "2026-06-01T00:00:00Z",
                "--year", "2026",
                "--raw-dir", str(tmp_path / "raw"),
            ],
        )

    assert result.exit_code == 0
    # Backfill first, then follow.
    assert calls == ["acquire", "follow"]


def test_live_without_from_follows_only(tmp_path: Path) -> None:
    """`--live` without --from does not touch the archive window."""
    from types import SimpleNamespace

    fake_live = {"fetched": [], "manifest": {"gaps": [], "snapshots": []}}
    index = SimpleNamespace(refs=[], cache_path=tmp_path / "i.html", from_cache=False)

    with patch(
        "debian_observer.cli.commands.ingest.fetch_index", return_value=index
    ), patch(
        "debian_observer.ingestion.britney.follow_live", return_value=fake_live
    ) as follow, patch(
        "debian_observer.ingestion.britney.acquire_window"
    ) as acquire:
        result = CliRunner().invoke(
            cli,
            [
                "ingest", "britney",
                "--live",
                "--year", "2026",
                "--raw-dir", str(tmp_path / "raw"),
            ],
        )

    assert result.exit_code == 0
    follow.assert_called_once()
    acquire.assert_not_called()

def test_the_cli_exposes_the_poll_interval(tmp_path: Path) -> None:
    """--poll-interval reaches follow_live: the module constant by
    default, the explicit value when given (a knob of --live alone)."""
    from types import SimpleNamespace

    index = SimpleNamespace(
        refs=[], cache_path=tmp_path / "i.html", from_cache=False
    )
    fake_live = {"fetched": [], "manifest": {"gaps": [], "snapshots": []}}

    def run(extra: list[str]) -> Any:
        with patch(
            "debian_observer.cli.commands.ingest.fetch_index", return_value=index
        ), patch(
            "debian_observer.ingestion.britney.follow_live",
            return_value=fake_live,
        ) as follow:
            result = CliRunner().invoke(
                cli,
                ["ingest", "britney", "--year", "2026"]
                + extra
                + ["--raw-dir", str(tmp_path / "raw")],
            )
        return result, follow

    result, follow = run(["--live"])
    assert result.exit_code == 0
    assert follow.call_args.kwargs["poll_interval"] == (
        britney.MIN_LIVE_POLL_INTERVAL
    )

    given = britney.MIN_LIVE_POLL_INTERVAL + 60
    result, follow = run(["--live", "--poll-interval", str(given)])
    assert result.exit_code == 0
    assert follow.call_args.kwargs["poll_interval"] == given
