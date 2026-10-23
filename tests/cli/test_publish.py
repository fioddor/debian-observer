"""CLI tests for the `publish` command — the JetStream view of the log.

`publish --events-dir <dir>` projects the append-only Parquet event
log onto NATS JetStream (ADR-004 amendment, 2026-10-23): flat
per-type subjects, `Nats-Msg-Id: <event_id>` server-side dedup, the
stream created if missing. The stream is a rebuildable view, never
a source of truth. Tests run against a fake NATS client (no
network): the fake JetStream deduplicates by `Nats-Msg-Id`, like
the real one does within the duplicate window.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

from click.testing import CliRunner

from debian_observer.cli.main import cli
from debian_observer.events import EventEnvelope
from debian_observer.storage.parquet import ParquetWriter

NOW = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc)

#: The ADR-004 flat subjects (per event type — NO per-package
#: suffix: the package id travels in the payload).
FLAT_SUBJECTS = {
    "debian.package.observed",
    "debian.package_version.observed",
    "debian.release.observed",
    "debian.migration.observed",
}


def _pvo_event(
    *, package_id: str = "vim:amd64", observed_at: datetime = NOW
) -> EventEnvelope:
    return EventEnvelope(
        event_id=uuid4(),
        event_type="PackageVersionObserved",
        source="udd",
        entity_id=package_id,
        payload={
            "package_id": package_id,
            "source_package": "vim",
            "version": "2:9.0-1",
            "distribution": "debian",
            "release": "sid",
            "suite": None,
            "architecture": "amd64",
            "component": "main",
            "section": "editors",
            "priority": "optional",
        },
        observed_at=observed_at,
        effective_at=observed_at,
    )


def _package_observed_event() -> EventEnvelope:
    return EventEnvelope(
        event_id=uuid4(),
        event_type="PackageObserved",
        source="udd",
        entity_id="vim",
        payload={"package_id": "vim", "name": "vim"},
        observed_at=NOW + timedelta(minutes=1),
        effective_at=NOW + timedelta(minutes=1),
    )


class FakeJetStream:
    """Stand-in JetStream context: records publishes, dedups by
    Nats-Msg-Id (the server's duplicate-window behaviour)."""

    def __init__(self) -> None:
        self.streams: dict[str, Any] = {}
        self.add_stream_calls: list[dict[str, Any]] = []
        self.msg_ids: dict[str, int] = {}
        self.published: list[tuple[str, bytes, dict[str, Any] | None]] = []

    async def stream_info(self, name: str) -> Any:
        if name in self.streams:
            return SimpleNamespace(name=name)
        raise KeyError(name)  # NotFoundError stand-in

    async def add_stream(self, **config: Any) -> Any:
        self.add_stream_calls.append(config)
        self.streams[config["name"]] = config
        return SimpleNamespace(name=config["name"])

    async def publish(
        self,
        subject: str,
        payload: bytes,
        timeout: float | None = None,
        stream: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> Any:
        msg_id = (headers or {}).get("Nats-Msg-Id")
        duplicate = msg_id is not None and msg_id in self.msg_ids
        if msg_id is not None and not duplicate:
            self.msg_ids[msg_id] = len(self.msg_ids) + 1
        self.published.append((subject, payload, headers))
        return SimpleNamespace(
            seq=self.msg_ids.get(msg_id, 0), duplicate=duplicate
        )


class FakeNATSClient:
    """Stand-in NATSClient: same surface, no network."""

    def __init__(self, servers: list[str] | None = None) -> None:
        self.servers = servers or ["nats://localhost:4222"]
        self.is_connected = False
        self._js = FakeJetStream()

    async def connect(self) -> None:
        self.is_connected = True

    async def close(self) -> None:
        self.is_connected = False

    @property
    def jetstream(self) -> FakeJetStream:
        return self._js


def _events_dir(tmp_path: Path) -> Path:
    events_dir = tmp_path / "events"
    writer = ParquetWriter(output_dir=events_dir)
    writer.write_events(
        [_pvo_event(), _pvo_event(package_id="bash:amd64")],
        ingestion_run_id="run-1",
    )
    writer.write_events([_package_observed_event()], ingestion_run_id="run-2")
    return events_dir


def _run_publish(
    monkeypatch, events_dir: Path, *extra: str
):
    monkeypatch.setattr(
        "debian_observer.cli.commands.publish.NATSClient", FakeNATSClient
    )
    runner = CliRunner()
    return runner.invoke(
        cli, ["publish", "--events-dir", str(events_dir), *extra]
    )


def test_publish_requires_events_dir(tmp_path: Path):
    """--events-dir is required: absent → usage error (exit 2)."""
    runner = CliRunner()
    result = runner.invoke(cli, ["publish"])

    assert result.exit_code == 2
    assert "--events-dir" in result.output
    assert "Traceback" not in result.output


def test_publish_empty_events_dir_is_an_error(tmp_path: Path):
    """An events dir without Parquet files fails naming the dir."""
    empty = tmp_path / "events"
    empty.mkdir()
    runner = CliRunner()
    result = runner.invoke(
        cli, ["publish", "--events-dir", str(empty)]
    )

    assert result.exit_code != 0
    assert str(empty) in result.output
    assert "Traceback" not in result.output


def test_publish_help_lists_the_options():
    runner = CliRunner()
    result = runner.invoke(cli, ["publish", "--help"])

    assert result.exit_code == 0
    assert "--events-dir" in result.output
    assert "--stream" in result.output
    assert "--server" in result.output
    # The defaults are surfaced (CONTRIBUTING --help rule).
    assert "debian-events" in result.output
    assert "nats://localhost:4222" in result.output
    # Honest perimeter: the stream is a rebuildable view.
    assert "source of truth" in result.output
    assert "--events-dir" in result.output


def test_publish_creates_the_stream_if_missing(
    tmp_path: Path, monkeypatch
):
    """The stream is ensured with the ADR-004 config (subjects,
    storage, limits) — created when missing, not re-created when it
    exists."""
    events_dir = _events_dir(tmp_path)
    fake = FakeNATSClient()
    monkeypatch.setattr(
        "debian_observer.cli.commands.publish.NATSClient",
        lambda servers=None: fake,
    )
    runner = CliRunner()
    args = ["publish", "--events-dir", str(events_dir)]

    result = runner.invoke(cli, args)
    assert result.exit_code == 0
    # Created exactly once, with the ADR-004 topology.
    assert len(fake._js.add_stream_calls) == 1
    config = fake._js.add_stream_calls[0]
    assert config["name"] == "debian-events"
    assert set(config["subjects"]) == FLAT_SUBJECTS

    # A second publish does NOT re-create the stream.
    result = runner.invoke(cli, args)
    assert result.exit_code == 0
    assert len(fake._js.add_stream_calls) == 1


def test_publish_publishes_all_events_flat_subjects(
    tmp_path: Path, monkeypatch
):
    """Every event goes to its flat per-type subject — no
    `.<package_id>` partitioning (ADR-004 amendment)."""
    events_dir = _events_dir(tmp_path)
    fake = FakeNATSClient()
    monkeypatch.setattr(
        "debian_observer.cli.commands.publish.NATSClient",
        lambda servers=None: fake,
    )
    runner = CliRunner()
    result = runner.invoke(
        cli, ["publish", "--events-dir", str(events_dir)]
    )

    assert result.exit_code == 0
    subjects = [subject for subject, _, _ in fake._js.published]
    assert len(subjects) == 3  # 2 PVO + 1 PackageObserved
    assert set(subjects) == {
        "debian.package_version.observed",
        "debian.package.observed",
    }
    # FLAT: every subject is one of the ADR-004 names — no suffix.
    assert set(subjects) <= FLAT_SUBJECTS

    # Echo: files, events, per-subject counts, stream, servers.
    assert "Files read: 2" in result.output
    assert "Events read: 3" in result.output
    assert "Published debian.package_version.observed: 2" in result.output
    assert "Published debian.package.observed: 1" in result.output
    assert "Duplicates absorbed: 0" in result.output
    assert "Stream: debian-events" in result.output
    assert "Servers: nats://localhost:4222" in result.output


def test_publish_sets_nats_msg_id_per_event(tmp_path: Path, monkeypatch):
    """Every message carries `Nats-Msg-Id: <event_id>` — JetStream
    deduplicates server-side within the duplicate window."""
    events_dir = _events_dir(tmp_path)
    fake = FakeNATSClient()
    monkeypatch.setattr(
        "debian_observer.cli.commands.publish.NATSClient",
        lambda servers=None: fake,
    )
    runner = CliRunner()
    result = runner.invoke(
        cli, ["publish", "--events-dir", str(events_dir)]
    )

    assert result.exit_code == 0
    published_ids = []
    for subject, payload, headers in fake._js.published:
        assert headers is not None
        assert "Nats-Msg-Id" in headers
        published_ids.append(headers["Nats-Msg-Id"])
    # One distinct msg-id per event (3 events), all real event ids.
    assert len(set(published_ids)) == 3
    assert len(fake._js.msg_ids) == 3


def test_publish_republish_is_a_noop(tmp_path: Path, monkeypatch):
    """Idempotent publish: re-publishing the same log is absorbed
    by Nats-Msg-Id dedup — the fake stream's message set is
    unchanged and the duplicates are counted."""
    events_dir = _events_dir(tmp_path)
    fake = FakeNATSClient()
    monkeypatch.setattr(
        "debian_observer.cli.commands.publish.NATSClient",
        lambda servers=None: fake,
    )
    runner = CliRunner()
    args = ["publish", "--events-dir", str(events_dir)]

    first = runner.invoke(cli, args)
    assert first.exit_code == 0
    assert "Duplicates absorbed: 0" in first.output
    assert len(fake._js.msg_ids) == 3

    second = runner.invoke(cli, args)
    assert second.exit_code == 0
    # The stream carried no new messages: all 3 were duplicates.
    assert "Duplicates absorbed: 3" in second.output
    assert len(fake._js.msg_ids) == 3


def test_publish_custom_stream_name(tmp_path: Path, monkeypatch):
    events_dir = _events_dir(tmp_path)
    result = _run_publish(
        monkeypatch, events_dir, "--stream", "debian-events-mirror"
    )

    assert result.exit_code == 0
    assert "Stream: debian-events-mirror" in result.output


def test_publish_custom_servers(tmp_path: Path, monkeypatch):
    events_dir = _events_dir(tmp_path)
    result = _run_publish(
        monkeypatch,
        events_dir,
        "--server",
        "nats://nats.example.com:4222",
    )

    assert result.exit_code == 0
    assert "Servers: nats://nats.example.com:4222" in result.output