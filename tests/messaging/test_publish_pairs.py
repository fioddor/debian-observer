"""Tests: publish the eventized pairs to the migration stream.

Pins docs/release3.project.v0.1.md §11.1 (ADR-026): the eventized
pairs publish to the dedicated stream `debian-migrations`, every
message carries `Nats-Msg-Id: <event_id>` (ADR-025 — JetStream
server-side dedup as an optimization), the events of a pair are
published only after every event of the previous pair (ADR-027 —
the pair eventizer stays sequential), and the stream keeps its own
retention: file storage, limits retention, 400 days / 2 GB (D3),
a duplicate window of hours, and no message-count limit (age and
bytes are the budget).

Tests run against a fake NATS client (no network): the fake
JetStream deduplicates by `Nats-Msg-Id`, like the real one does
within the stream's duplicate window.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import yaml
from click.testing import CliRunner

from debian_observer.cli.main import cli
from debian_observer.events.envelope import EventEnvelope
from debian_observer.ingestion.britney import sha256_bytes
from debian_observer.messaging.config import (
    MIGRATIONS_DUPLICATE_WINDOW_HOURS,
    MIGRATIONS_MAX_AGE_DAYS,
    MIGRATIONS_MAX_BYTES,
    MIGRATIONS_MAX_MESSAGES,
    MIGRATIONS_STREAM_NAME,
    MigrationStreamConfig,
)
from debian_observer.messaging.migrations import (
    publish_eventized_pairs,
)
from debian_observer.normalization.eventizers.excuses import SUBJECTS

# ---------------------------------------------------------------------------
# Spec values (§11.1): the retention this stream keeps on its own
# ---------------------------------------------------------------------------

#: Spec §11.1, D3: consumers replay from the start; the disk budget
#: is age and bytes, not a message count.
SPEC_MAX_AGE_DAYS = 400
SPEC_MAX_BYTES = 2 * 1024**3


# ---------------------------------------------------------------------------
# A fake NATS client (the tests/cli/test_publish.py pattern, no network)
# ---------------------------------------------------------------------------


class FakeJetStream:
    """Stand-in JetStream: records publishes, dedups by Nats-Msg-Id."""

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


# ---------------------------------------------------------------------------
# Eventized pairs (the chunks the pair eventizer yields)
# ---------------------------------------------------------------------------


def _envelope(event_type: str, entity_id: str) -> EventEnvelope:
    return EventEnvelope(
        event_id=uuid4(),
        event_type=event_type,
        source="britney",
        entity_id=entity_id,
        payload={"item_name": entity_id},
    )


#: Two pairs, as `ExcusesEventizer.iter_eventized` yields them:
#: the seed snapshot complete, then one pair of deltas.
PAIRS: list[tuple[str, list[EventEnvelope]]] = [
    (
        "s1",
        [
            _envelope("ExcuseObserved", "alpha"),
            _envelope("SnapshotObserved", "s1"),
        ],
    ),
    (
        "s2",
        [
            _envelope("ExcuseEntered", "beta"),
            _envelope("SnapshotObserved", "s2"),
        ],
    ),
]

PAIR_EVENT_COUNT = sum(len(events) for _, events in PAIRS)


def _published_msg_ids(js: FakeJetStream) -> list[str]:
    return [
        (headers or {}).get("Nats-Msg-Id")
        for _, _, headers in js.published
    ]


# ---------------------------------------------------------------------------
# The stream configuration (§11.1)
# ---------------------------------------------------------------------------


class TestMigrationStreamConfig:
    """`debian-migrations` keeps its own subjects and retention."""

    def test_the_stream_is_dedicated(self) -> None:
        config = MigrationStreamConfig()
        assert config.stream_name == MIGRATIONS_STREAM_NAME
        assert config.stream_name == "debian-migrations"
        assert config.stream_name != "debian-events"

    def test_subjects_carry_both_wildcards(self) -> None:
        config = MigrationStreamConfig()
        assert set(config.subjects) == {
            "debian.migration.excuse.>",
            "debian.migration.snapshot.>",
        }

    def test_the_existing_migration_subject_is_untouched(self) -> None:
        # D8: `debian.migration.observed` (MigrationObserved) stays on
        # `debian-events`; the migration stream does not claim it.
        config = MigrationStreamConfig()
        assert "debian.migration.observed" not in config.subjects

    def test_retention_is_limits_over_file_storage(self) -> None:
        config = MigrationStreamConfig()
        assert config.storage == "file"  # durable replay
        assert config.retention == "limits"  # replay from the start

    def test_max_age_and_bytes_come_from_the_spec(self) -> None:
        # Spec §11.1: 400 days / 2 GB (D3), revisited once the real
        # volume is measured. The message count is NOT a budget here.
        assert MIGRATIONS_MAX_AGE_DAYS == SPEC_MAX_AGE_DAYS
        assert MIGRATIONS_MAX_BYTES == SPEC_MAX_BYTES
        assert MIGRATIONS_MAX_MESSAGES == -1  # unlimited

    def test_duplicate_window_is_hours_not_minutes(self) -> None:
        # Spec §11.1: "hours, not minutes" — covers re-publishing a
        # window; the guarantee stays in the consumers.
        assert MIGRATIONS_DUPLICATE_WINDOW_HOURS >= 1

    def test_to_nats_config_carries_every_limit(self) -> None:
        config = MigrationStreamConfig()
        as_nats = config.to_nats_config()
        assert as_nats["name"] == "debian-migrations"
        assert as_nats["max_age"] == SPEC_MAX_AGE_DAYS * 24 * 60 * 60
        assert as_nats["max_bytes"] == SPEC_MAX_BYTES
        assert as_nats["max_msgs"] == -1
        assert (
            as_nats["duplicate_window"]
            == MIGRATIONS_DUPLICATE_WINDOW_HOURS * 60 * 60
        )

    def test_subject_for_routes_every_excuse_event_type(self) -> None:
        config = MigrationStreamConfig()
        for event_type, subject in SUBJECTS.items():
            assert config.subject_for(event_type) == subject
            assert subject.startswith("debian.migration.")

    def test_subject_for_unknown_fails_naming_the_types(self) -> None:
        config = MigrationStreamConfig()
        try:
            config.subject_for("NotAnEvent")
        except KeyError as exc:
            assert "NotAnEvent" in str(exc)
            for event_type in SUBJECTS:
                assert event_type in str(exc)
        else:  # pragma: no cover - the contract is the raise
            raise AssertionError("subject_for must fail on an unknown type")


# ---------------------------------------------------------------------------
# Publication: pairs, Nats-Msg-Id, ordering (§6.2, §6.3)
# ---------------------------------------------------------------------------


class TestPublishEventizedPairs:
    """`publish_eventized_pairs` publishes pair by pair, deduped."""

    async def test_ensures_the_migration_stream_exists(self) -> None:
        client = FakeNATSClient()
        await client.connect()
        await publish_eventized_pairs(iter(PAIRS), client=client)
        assert [c["name"] for c in client._js.add_stream_calls] == [
            "debian-migrations"
        ]

    async def test_publishes_every_event_with_its_id(self) -> None:
        client = FakeNATSClient()
        await client.connect()
        stats = await publish_eventized_pairs(iter(PAIRS), client=client)
        expected = [str(e.event_id) for _, events in PAIRS for e in events]
        assert _published_msg_ids(client._js) == expected
        assert stats.events == PAIR_EVENT_COUNT
        assert stats.duplicates_absorbed == 0

    async def test_every_message_carries_nats_msg_id(self) -> None:
        client = FakeNATSClient()
        await client.connect()
        await publish_eventized_pairs(iter(PAIRS), client=client)
        for subject, payload, headers in client._js.published:
            assert headers is not None
            assert "Nats-Msg-Id" in headers
            body = json.loads(payload.decode("utf-8"))
            assert headers["Nats-Msg-Id"] == body["event_id"]

    async def test_pairs_publish_in_order(self) -> None:
        # ADR-027: the events of a pair are published only after
        # every event of the previous pair was.
        client = FakeNATSClient()
        await client.connect()
        await publish_eventized_pairs(iter(PAIRS), client=client)
        subjects = [subject for subject, _, _ in client._js.published]
        first_pair_len = len(PAIRS[0][1])
        assert set(subjects[:first_pair_len]) <= {
            "debian.migration.excuse.observed",
            "debian.migration.snapshot.observed",
        }
        assert subjects[-1] == "debian.migration.snapshot.observed"
        # The pair boundary is a boundary: no pair-2 subject before
        # the last pair-1 event.
        assert "debian.migration.excuse.entered" not in subjects[:first_pair_len]

    async def test_republishing_the_same_window_is_absorbed(self) -> None:
        # ADR-025 + the duplicate window: a re-published window adds
        # zero messages (JetStream dedup as the optimization).
        client = FakeNATSClient()
        await client.connect()
        await publish_eventized_pairs(iter(PAIRS), client=client)
        stats = await publish_eventized_pairs(
            iter(PAIRS), client=client
        )
        assert stats.duplicates_absorbed == PAIR_EVENT_COUNT
        assert stats.events == 0  # nothing new
        # The server saw every message twice, the stream kept one.
        assert len(client._js.published) == 2 * PAIR_EVENT_COUNT
        assert len(client._js.msg_ids) == PAIR_EVENT_COUNT

    async def test_stats_name_the_stream_and_the_servers(self) -> None:
        client = FakeNATSClient(servers=["nats://elsewhere:4222"])
        await client.connect()
        stats = await publish_eventized_pairs(iter(PAIRS), client=client)
        assert stats.stream_name == "debian-migrations"
        assert stats.servers == ("nats://elsewhere:4222",)


# ---------------------------------------------------------------------------
# The CLI: `normalize britney --publish`
# ---------------------------------------------------------------------------


ALPHA = {
    "item-name": "alpha",
    "source": "alpha",
    "old-version": "1.0",
    "new-version": "1.1",
    "migration-policy-verdict": "PASS",
    "is-candidate": True,
    "dependencies": {"blocked-by": ["beta"]},
}


def _gz_doc(items: list[dict[str, Any]], generated_date: str) -> bytes:
    doc = {"generated-date": generated_date, "sources": items}
    return gzip.compress(yaml.safe_dump(doc).encode())


#: A two-snapshot window: the seed, then one entered item.
WINDOW: list[tuple[str, str, list[dict[str, Any]]]] = [
    ("20260601T010000Z_0001", "2026-06-01T01:00:00Z", [ALPHA]),
    (
        "20260601T020000Z_0001",
        "2026-06-01T02:00:00Z",
        [dict(ALPHA), {"item-name": "beta"}],
    ),
]
#: seed: observed + frame; pair: one entered + frame.
CLI_EVENT_COUNT = 4


def _stored_window(tmp_path: Path) -> tuple[Path, Path]:
    """A raw store plus its window manifest, as `ingest britney` writes."""
    raw = tmp_path / "raw"
    entries = []
    for snapshot_id, generated_date, items in WINDOW:
        body = _gz_doc(items, generated_date)
        path = raw / "britney" / "2026" / f"{snapshot_id}.yaml.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        entries.append(
            {
                "snapshot_id": snapshot_id,
                "url": f"https://release.debian.org/britney/update_excuses/2026/{snapshot_id}.yaml.gz",
                "sha256": sha256_bytes(body),
                "generated_date": generated_date,
                "item_count": len(items),
                "top_level_keys": ["generated-date", "sources"],
            }
        )
    manifest = {
        "source": "britney",
        "run_id": "run-ingest",
        "year": 2026,
        "snapshots": entries,
        "gaps": [],
        "failures": [],
    }
    manifest_path = tmp_path / "manifest-run-ingest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return raw, manifest_path


class TestNormalizeBritneyPublish:
    """`--publish` publishes the eventized pairs to the stream."""

    def _run(
        self, monkeypatch, tmp_path: Path, *extra: str
    ) -> tuple[Any, Any, dict[str, Any]]:
        raw, manifest = _stored_window(tmp_path)
        lake = tmp_path / "lake"
        made: dict[str, Any] = {}

        def _factory(servers: list[str] | None = None) -> FakeNATSClient:
            fake = FakeNATSClient(servers=servers)
            made["servers"] = fake.servers
            made["fake"] = fake
            return fake

        monkeypatch.setattr(
            "debian_observer.cli.commands.normalize.NATSClient", _factory
        )
        result = CliRunner().invoke(
            cli,
            [
                "normalize",
                "britney",
                str(manifest),
                "--raw-dir",
                str(raw),
                "--events-dir",
                str(lake),
                "--checkpoint",
                str(tmp_path / "checkpoint.json"),
                *extra,
            ],
        )
        return result, made.get("servers"), made

    def test_help_documents_the_publish_perimeter(self) -> None:
        result = CliRunner().invoke(cli, ["normalize", "britney", "--help"])
        assert result.exit_code == 0
        assert "--publish" in result.output
        assert "--server" in result.output
        assert "debian-migrations" in result.output

    def test_publish_publishes_the_window(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        result, servers, made = self._run(
            monkeypatch, tmp_path, "--publish", "--server", "nats://h:4222"
        )
        fake = made["fake"]
        assert result.exit_code == 0, result.output
        assert [c["name"] for c in fake._js.add_stream_calls] == [
            "debian-migrations"
        ]
        assert len(fake._js.published) == CLI_EVENT_COUNT
        assert servers == ["nats://h:4222"]
        # Every message carries its event id (Nats-Msg-Id).
        for _, _, headers in fake._js.published:
            assert "Nats-Msg-Id" in (headers or {})

    def test_without_publish_nothing_is_published(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        result, _, made = self._run(monkeypatch, tmp_path)
        assert result.exit_code == 0, result.output
        # No client was ever constructed: nothing connected,
        # nothing published, no stream added.
        assert made.get("fake") is None

    def test_publish_is_idempotent_across_runs(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        # Same window, fresh checkpoint each run: re-running a
        # published window publishes the same ids again — the
        # stream absorbs them (duplicates counted, nothing new).
        raw, manifest = _stored_window(tmp_path)
        lake = tmp_path / "lake"
        fake = FakeNATSClient()
        monkeypatch.setattr(
            "debian_observer.cli.commands.normalize.NATSClient",
            lambda servers=None: fake,
        )
        args = [
            "normalize",
            "britney",
            str(manifest),
            "--raw-dir",
            str(raw),
            "--events-dir",
            str(lake),
            "--publish",
        ]
        first = CliRunner().invoke(cli, args)
        assert first.exit_code == 0, first.output
        checkpoint = tmp_path / "lake" / "checkpoint-britney-excuses.json"
        checkpoint.unlink()
        second = CliRunner().invoke(cli, args)
        assert second.exit_code == 0, second.output
        assert len(fake._js.published) == 2 * CLI_EVENT_COUNT
        assert len(fake._js.msg_ids) == CLI_EVENT_COUNT