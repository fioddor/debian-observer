"""Messaging: the parse work queue — enqueue one job per snapshot.

The expensive half of the britney pipeline (ADR-027) — the parse of
one snapshot — runs as a JetStream **work queue**, its own stream
``britney-parse`` with work queue retention: one job = one snapshot,
and a job done is a message gone (docs/release3.project.v0.1.md
§11.2) — distinct from the replayable ``debian-migrations`` stream
(``messaging.migrations``), so neither retention evicts the
other's.

The enqueue side (the ingestor) publishes one job per **observed**
snapshot of an ingested window:

* the raw ``.yaml.gz`` goes to a NATS Object Store bucket (§7.3,
  D2): the worker reads it from the same NATS it already uses and
  **never fetches from the source** (§4.1.3);
* the job names its raw object — the message carries the object
  name, the archive URL and the SHA-256 (ADR-002), so the worker can
  verify what it read;
* the job id is **derived** — a uuid5 of the ``snapshot_id`` (the
  deterministic-identity pattern of ADR-025 / ADR-019) — and the
  message carries ``Nats-Msg-Id: <job_id>``, so JetStream absorbs a
  re-run server-side within the stream's duplicate window. An
  optimization: the guarantee is that the worker's writes are atomic
  and keyed by snapshot (``messaging.parse_worker``), so a
  redelivered job overwrites with identical content.

The worker side — the durable pull that works this queue — lives in
``messaging.parse_worker``.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any, Sequence

from debian_observer.ingestion.britney import sha256_bytes
from debian_observer.messaging.config import StreamConfig

__all__ = [
    "IN_PROGRESS_ACK_SECONDS",
    "PARSE_ACK_WAIT_SECONDS",
    "PARSE_JOB_NAMESPACE",
    "PARSE_MAX_DELIVER",
    "PARSE_QUEUE_DURABLE",
    "PARSE_STREAM_NAME",
    "PARSE_SUBJECT",
    "RAW_BUCKET",
    "RESULTS_BUCKET",
    "EnqueueStats",
    "ParseJob",
    "ParseQueueConfig",
    "RawSnapshot",
    "dead_letter_object_name",
    "enqueue_snapshots",
    "ensure_bucket",
    "ensure_parse_queue",
    "parse_job_id",
    "raw_object_name",
    "result_object_name",
]

# ---------------------------------------------------------------------------
# The queue (§11.2)
# ---------------------------------------------------------------------------

#: Spec §11.2: the stream is named `britney-parse` — its own stream,
#: work queue retention, one job = one snapshot.
PARSE_STREAM_NAME = "britney-parse"

#: The subject every parse job publishes to (and only jobs: the
#: stream carries no other traffic, so the whole stream is the queue).
PARSE_SUBJECT = "britney.parse.snapshot"

#: Spec §11.2: `ack_wait` above the worst-case parse under
#: contention. The probe saw one parse exceed 20 s with 4 workers and
#: be redelivered spuriously (ADR-027 evidence) — a redelivery is
#: harmless (idempotent writes) but wasteful; 120 s sits above the
#: slowest measured parse with margin, while a dead worker's job
#: still comes back within minutes.
PARSE_ACK_WAIT_SECONDS = 120

#: Spec §11.2: in-progress acknowledgements every ~10 s while
#: parsing — a slow job keeps its delivery alive, a dead one is
#: still redelivered after `ack_wait`.
IN_PROGRESS_ACK_SECONDS = 10

#: Spec §11.2: `max_deliver` 5, then a dead-letter record — a
#: poisoned snapshot does not loop forever and the queue continues.
PARSE_MAX_DELIVER = 5

#: Spec §11.2: one durable, pull — the workers on several machines
#: share it. Its name is part of the deployment's surface.
PARSE_QUEUE_DURABLE = "britney-parse-workers"

#: Spec §11.2: `max_ack_pending` is the total worker slots
#: (backpressure) — set per worker by the CLI's `--concurrency`.

# ---------------------------------------------------------------------------
# The raw transport (§7.3, D2)
# ---------------------------------------------------------------------------

#: The bucket the ingestor puts each raw ``.yaml.gz`` in; the worker
#: reads it from the same NATS (§7.3) and never from the source.
RAW_BUCKET = "britney-raw"

#: The bucket the worker puts each compact state back into (§7.3),
#: keyed by snapshot (§11.2: atomic, a redelivery overwrites with
#: identical content).
RESULTS_BUCKET = "britney-parse-results"


def raw_object_name(snapshot_id: str) -> str:
    """The Object Store name of a snapshot's raw bytes (§7.3)."""
    return f"{snapshot_id}.yaml.gz"


def result_object_name(snapshot_id: str) -> str:
    """The Object Store name of a snapshot's compact state (§7.3)."""
    return f"{snapshot_id}.parquet"


def dead_letter_object_name(snapshot_id: str) -> str:
    """The Object Store name of a snapshot's dead-letter record (§15)."""
    return f"dead-letter-{snapshot_id}.json"


# ---------------------------------------------------------------------------
# The job id (the ADR-025 pattern)
# ---------------------------------------------------------------------------

#: The frozen UUID namespace the parse job ids derive from. Chosen
#: once, frozen forever after: moving it would mint every job id
#: anew and break the re-run absorption of `Nats-Msg-Id`. Its value
#: carries no meaning — only its stability does (ADR-025).
PARSE_JOB_NAMESPACE = uuid.UUID("d1a5c0de-7a4e-4b8f-9c2d-3e6f1a2b4c5d")


def parse_job_id(snapshot_id: str) -> uuid.UUID:
    """Derive one job's identity: a uuid5 of its ``snapshot_id``.

    A re-run of the same window derives the same ids, so publishing
    it again is absorbed by ``Nats-Msg-Id`` dedup — the enqueue is
    idempotent (ADR-019: deterministic output keys).
    """
    return uuid.uuid5(PARSE_JOB_NAMESPACE, snapshot_id)


# ---------------------------------------------------------------------------
# The job message
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ParseJob:
    """One queue message: parse this raw snapshot object.

    Attributes:
        snapshot_id: the archive file stem — the run identity.
        raw_object: the Object Store name of the raw ``.yaml.gz`` —
            the worker reads it from the bucket (§7.3), never from
            the source (§4.1.3).
        sha256: SHA-256 of the raw bytes (ADR-002): the worker
            verifies what it read.
        url: the archive URL the bytes were fetched from (recorded,
            never re-fetched by the worker).
        job_id: the derived job identity (``Nats-Msg-Id``).
    """

    snapshot_id: str
    raw_object: str
    sha256: str
    url: str | None = None
    job_id: uuid.UUID | None = None

    def __post_init__(self) -> None:
        if self.job_id is None:
            object.__setattr__(
                self, "job_id", parse_job_id(self.snapshot_id)
            )

    def to_json_bytes(self) -> bytes:
        """The message body: plain JSON, the whole job."""
        return json.dumps(
            {
                "snapshot_id": self.snapshot_id,
                "raw_object": self.raw_object,
                "sha256": self.sha256,
                "url": self.url,
                "job_id": str(self.job_id),
            },
            sort_keys=True,
        ).encode("utf-8")

    @classmethod
    def from_json_bytes(cls, payload: bytes) -> "ParseJob":
        """Rebuild a job from its message body."""
        record = json.loads(payload.decode("utf-8"))
        job_id = record.get("job_id")
        return cls(
            snapshot_id=record["snapshot_id"],
            raw_object=record["raw_object"],
            sha256=record["sha256"],
            url=record.get("url"),
            job_id=uuid.UUID(job_id) if job_id else None,
        )


# ---------------------------------------------------------------------------
# The stream and bucket plumbing
# ---------------------------------------------------------------------------


class ParseQueueConfig(StreamConfig):
    """The declarative configuration of the ``britney-parse`` stream.

    Work queue retention (§11.2): a consumed job is removed, so the
    stream never grows with the archive and its disk budget is the
    pending jobs only — no age, no byte, no message-count limit is
    set. File storage: a redelivered job (a dead worker) survives a
    server restart.
    """

    def __init__(
        self,
        *,
        stream_name: str = PARSE_STREAM_NAME,
        subjects: list[str] | None = None,
        storage: str = "file",
        retention: str = "workqueue",
    ) -> None:
        super().__init__(
            stream_name=stream_name,
            subjects=subjects if subjects is not None else [PARSE_SUBJECT],
            storage=storage,
            retention=retention,
            max_messages=-1,
            max_age_days=0,
            max_bytes=-1,
        )

    def to_nats_config(self) -> dict[str, Any]:
        """Serialize to ``js.add_stream()`` keys: work queue retention."""
        return {
            "name": self.stream_name,
            "subjects": self.subjects,
            "storage": self.storage,
            "retention": self.retention,
        }


async def ensure_parse_queue(client: Any) -> Any:
    """Create the ``britney-parse`` stream if missing; return its info.

    The same lookup-then-create shape as ``migrations``: the stream
    is the queue, so ensuring it is ensuring the queue exists.
    """
    js = client.jetstream
    try:
        return await js.stream_info(PARSE_STREAM_NAME)
    except Exception:
        # NotFoundError (or the fake's stand-in) → create below; any
        # other failure resurfaces from add_stream.
        pass
    return await js.add_stream(**ParseQueueConfig().to_nats_config())


async def ensure_bucket(client: Any, bucket: str) -> Any:
    """Return the Object Store ``bucket``, creating it if missing.

    The raw and the results buckets are created lazily by whoever
    needs them first (ingestor or worker) — no deployment step
    beyond the NATS server itself (§7.3).
    """
    js = client.jetstream
    try:
        return await js.object_store(bucket)
    except Exception:
        # NotFoundError (or the fake's stand-in) → create below.
        pass
    return await js.create_object_store(bucket)


# ---------------------------------------------------------------------------
# The enqueue side
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RawSnapshot:
    """One observed snapshot, as the ingestor hands it to the queue.

    Attributes:
        snapshot_id: the archive file stem.
        gz_bytes: the raw ``.yaml.gz`` bytes as they sit in the raw
            store (byte for byte, ADR-002).
        url: the archive URL they were fetched from.
        sha256: SHA-256 of the bytes; computed when absent.
    """

    snapshot_id: str
    gz_bytes: bytes
    url: str | None = None
    sha256: str | None = None

    def __post_init__(self) -> None:
        if self.sha256 is None:
            object.__setattr__(self, "sha256", sha256_bytes(self.gz_bytes))


@dataclass(frozen=True)
class EnqueueStats:
    """Counts surfaced by an enqueue run."""

    #: Jobs published (one per observed snapshot).
    jobs: int = 0
    #: Jobs the stream absorbed as duplicates (a re-run).
    duplicates_absorbed: int = 0
    #: Raw objects put in the bucket (all of them, every run: an
    #: object put is idempotent — same bytes, same object).
    objects_put: int = 0
    #: Raw bytes carried into the bucket.
    raw_bytes_put: int = 0
    stream_name: str = PARSE_STREAM_NAME
    raw_bucket: str = RAW_BUCKET
    servers: tuple[str, ...] = ()


async def enqueue_snapshots(
    client: Any,
    snapshots: Sequence[RawSnapshot],
    *,
    stream_config: ParseQueueConfig | None = None,
    raw_bucket: str = RAW_BUCKET,
) -> EnqueueStats:
    """Put every snapshot's raw bytes in the bucket and publish its job.

    One job per observed snapshot (§7.3): the raw ``.yaml.gz`` goes
    to the Object Store bucket under its object name, then the job —
    naming that object, carrying its SHA-256 and the archive URL —
    publishes to the queue's subject with ``Nats-Msg-Id: <job_id>``
    (the derived uuid5 of the snapshot_id), so a re-run of the same
    window is absorbed server-side.

    The queue stream is created if missing (``ensure_parse_queue``),
    so the first enqueue bootstraps the queue.

    Returns the enqueue statistics.
    """
    config = stream_config or ParseQueueConfig()
    js = client.jetstream
    await ensure_parse_queue(client)
    store = await ensure_bucket(client, raw_bucket)

    jobs = 0
    duplicates = 0
    objects_put = 0
    raw_bytes_put = 0
    for snapshot in snapshots:
        job = ParseJob(
            snapshot_id=snapshot.snapshot_id,
            raw_object=raw_object_name(snapshot.snapshot_id),
            sha256=snapshot.sha256 or sha256_bytes(snapshot.gz_bytes),
            url=snapshot.url,
        )
        await store.put(job.raw_object, snapshot.gz_bytes)
        objects_put += 1
        raw_bytes_put += len(snapshot.gz_bytes)
        ack = await js.publish(
            PARSE_SUBJECT,
            job.to_json_bytes(),
            stream=config.stream_name,
            headers={"Nats-Msg-Id": str(job.job_id)},
        )
        if bool(getattr(ack, "duplicate", False)):
            duplicates += 1
        else:
            jobs += 1

    servers = tuple(
        client.servers
        if hasattr(client, "servers") and client.servers is not None
        else ()
    )
    return EnqueueStats(
        jobs=jobs,
        duplicates_absorbed=duplicates,
        objects_put=objects_put,
        raw_bytes_put=raw_bytes_put,
        stream_name=config.stream_name,
        raw_bucket=raw_bucket,
        servers=servers,
    )