"""Ingestion: the britney source — archive index and window selection.

britney, the release team's migration tool, publishes its excuses as a
periodic state, not a history. The state is public twice over: the
live endpoint ``https://release.debian.org/britney/excuses.yaml.gz``
serves the current run, and the archive
``https://release.debian.org/britney/update_excuses/<year>/`` holds one
``<timestamp>.yaml.gz`` per past run
(docs/release3.project.v0.1.md §4.1). This module is the acquisition
side of ``ingest britney`` (ADR-012: one subcommand per source): the
archive index with its raw-store cache, the window selection
(``--from``/``--to`` inclusive, ``--last``, ``--live``), the polite
fetch into the raw store, the window manifest with gaps as data
(ADR-002), and one execution record per run.

Coordinates (§4.3): a snapshot belongs to
``(distribution=debian, from=unstable, to=testing, the testing
codename at generated-date)`` — recorded in the window manifest.

The parse — snapshot → compact excuse state — is NOT here: it is the
parse job of the work queue and lives in normalization
(``normalize britney``, ADR-027). Only the manifest metadata
(``generated-date``, item count, top-level keys) is read from the YAML,
with safe loaders only.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

from debian_observer.ingestion.acquisition import USER_AGENT

# ---------------------------------------------------------------------------
# Source constants
# ---------------------------------------------------------------------------

#: Identifies the installed version to release.debian.org (§4.1.3 D6).
BRITNEY_USER_AGENT = f"{USER_AGENT} (+https://github.com/fioddor/debian-observer)"

#: Base URL of britney's published output.
BRITNEY_BASE_URL = "https://release.debian.org/britney"

#: The live endpoint: the current state, regenerated several times a day.
LIVE_URL = f"{BRITNEY_BASE_URL}/excuses.yaml.gz"

#: Seconds between two source requests (§4.1.3): one at a time, never 0.
MIN_REQUEST_INTERVAL = 1.0

#: Seconds before one source request is given up as lost: one file of
#: the archive answers in seconds, 60 is generous.
HTTP_TIMEOUT_SECONDS = 60.0

#: Download attempts per listed snapshot before it is recorded as a
#: failure: the archive is a public service, transient errors are common.
FETCH_ATTEMPTS = 3

#: Base of the exponential backoff between attempts: the wait before
#: attempt n grows as BACKOFF_BASE**n seconds.
BACKOFF_BASE = 2.0

#: Seconds between two live polls (§4.1.3): no more often than 15 min.
MIN_LIVE_POLL_INTERVAL = 15 * 60

#: Subtree of the raw store that belongs to this source.
RAW_SUBDIR = "britney"

#: Consecutive observed snapshots further apart than this many seconds
#: are a gap (a period with no published snapshot, §4.2). The archive
#: cadence is about one run per hour (§2.1); six hours without a
#: snapshot is a missing publication, not a quiet hour.
#:
#: Not a CLI knob, unlike the fetch knobs: it describes how the
#: archive publishes (the cadence of the source), not how a run is
#: driven — a window that could move it would invent or erase gaps
#: the publication rhythm does not support.
GAP_THRESHOLD_SECONDS = 6 * 3600

#: The coordinates of the excuses (§4.3).
COORDINATES = {
    "distribution": "debian",
    "from_suite": "unstable",
    "to_suite": "testing",
}

# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class BritneyIndexError(ValueError):
    """The archive index could not be read or parsed."""


class GeneratedDateTie(ValueError):
    """Two snapshots of one window share a generated-date (§5.2).

    Snapshots are totally ordered by generated_date; a tie means the
    window cannot be ordered, and the run fails loudly instead of
    silently picking one.
    """


class DuplicateItemName(ValueError):
    """An item-name appears twice in one snapshot (§5.2).

    A duplicate is an invalid record; its cause is carried in the
    message so nothing is silently overwritten.
    """


class TruncatedGzip(ValueError):
    """A downloaded gzip file does not decompress (§4.1.3: size checks
    before decompressing)."""


# ---------------------------------------------------------------------------
# Archive index
# ---------------------------------------------------------------------------

#: A listed file name, as it appears in the directory listing.
_HREF_RE = re.compile(r'href="([^"/]+\.yaml\.gz)"')

#: The timestamp a snapshot file name encodes, in either surface's
#: form. The two surfaces name files differently: the archive names
#: its runs with 14 flat digits (``YYYYMMDDHHMMSS.yaml.gz``, verified
#: against the real index), while the live follower names its OWN
#: files with the ISO ``T``/``Z`` separators (see
#: :func:`_live_snapshot_id`). Both forms carry a UTC timestamp, so
#: one pattern accepts both.
_TS_RE = re.compile(r"(\d{8})T(\d{6})Z|(\d{14})")


@dataclass(frozen=True)
class SnapshotRef:
    """One britney run as listed by the archive index.

    ``snapshot_id`` is the archive file stem (the run identity across
    the platform); ``timestamp`` is the UTC time the file name encodes,
    or ``None`` for a name the pattern does not fit — the index is the
    truth, so such names stay listed but cannot anchor a window.
    """

    snapshot_id: str
    url: str
    timestamp: datetime | None


@dataclass(frozen=True)
class IndexResult:
    """The archive index of one year, as used by a window selection.

    ``refs`` are the listed snapshots in listing order, ``cache_path``
    where the listing is kept in the raw store, ``from_cache`` whether
    this read was served from that cache (no request was made).
    """

    refs: list[SnapshotRef]
    cache_path: Path
    from_cache: bool = False


def index_url(year: int, base_url: str = BRITNEY_BASE_URL) -> str:
    """The archive index of one year (§4.1.2)."""
    return f"{base_url}/update_excuses/{year}/"


def snapshot_url(year: int, snapshot_id: str, base_url: str = BRITNEY_BASE_URL) -> str:
    """The archive URL of one listed snapshot."""
    return f"{index_url(year, base_url)}{snapshot_id}.yaml.gz"


def parse_snapshot_timestamp(snapshot_id: str) -> datetime | None:
    """The UTC time a snapshot id encodes, or None if it encodes none."""
    match = _TS_RE.search(snapshot_id)
    if match is None:
        return None
    if match.group(3) is not None:  # the archive's 14 flat digits
        digits = match.group(3)
    else:  # the live form's T/Z separators
        digits = match.group(1) + match.group(2)
    return datetime.strptime(digits, "%Y%m%d%H%M%S").replace(
        tzinfo=timezone.utc
    )


def parse_index(html: str, *, base_url: str) -> list[SnapshotRef]:
    """Read snapshot refs from a directory listing.

    The file names come from the listing only (§4.1.2): never from a
    computed pattern. Non-yaml.gz entries (``.html.gz`` siblings, the
    parent link) are not snapshots. Names appear in listing order;
    duplicates collapse to the first occurrence.
    """
    base = base_url.rstrip("/") + "/"
    refs: list[SnapshotRef] = []
    seen: set[str] = set()
    for name in _HREF_RE.findall(html):
        if name in seen:
            continue
        seen.add(name)
        snapshot_id = name[: -len(".yaml.gz")]
        url = name if "://" in name else base + name
        refs.append(
            SnapshotRef(
                snapshot_id=snapshot_id,
                url=url,
                timestamp=parse_snapshot_timestamp(snapshot_id),
            )
        )
    return refs


def _index_cache_path(year: int, raw_dir: Path) -> Path:
    return raw_dir / RAW_SUBDIR / f"index-{year}.html"


def _http_get(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    timeout: float = HTTP_TIMEOUT_SECONDS,
) -> tuple[bytes, dict[str, str]]:
    """One conditional GET with the platform's User-Agent (§4.1.3 D6)."""
    request = urllib.request.Request(
        url,
        headers={"User-Agent": BRITNEY_USER_AGENT, **(headers or {})},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read()
        validators = {
            key.lower(): value
            for key, value in response.headers.items()
            if key.lower() in ("etag", "last-modified", "content-length")
        }
    return body, validators


def fetch_index(
    year: int,
    *,
    raw_dir: Path,
    base_url: str = BRITNEY_BASE_URL,
    fetch: Callable[[str], bytes] | None = None,
    timeout: float = HTTP_TIMEOUT_SECONDS,
) -> IndexResult:
    """Fetch (or read from the raw-store cache) one year's index.

    The listing is downloaded once per year and cached under the raw
    store (§4.1.3: every file downloaded once); re-runs read the cache.
    Raises :class:`BritneyIndexError` when the listing cannot be read.
    """
    cache_path = _index_cache_path(year, raw_dir)
    if cache_path.exists():
        html = cache_path.read_text(encoding="utf-8")
        return IndexResult(
            refs=parse_index(html, base_url=index_url(year, base_url)),
            cache_path=cache_path,
            from_cache=True,
        )
    try:
        body = fetch(index_url(year, base_url)) if fetch else _http_get(
            index_url(year, base_url), timeout=timeout
        )[0]
        html = body.decode("utf-8", errors="replace")
    except (urllib.error.URLError, OSError) as error:
        raise BritneyIndexError(
            f"could not read the britney archive index of {year}: {error}"
        ) from error
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(html, encoding="utf-8")
    return IndexResult(
        refs=parse_index(html, base_url=index_url(year, base_url)),
        cache_path=cache_path,
        from_cache=False,
    )


# ---------------------------------------------------------------------------
# Window selection
# ---------------------------------------------------------------------------


def _as_utc(value: datetime | str | None) -> datetime | None:
    """A window bound as a UTC datetime (naive means UTC, §8)."""
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def select_window(
    refs: list[SnapshotRef],
    *,
    from_ts: datetime | str | None = None,
    to_ts: datetime | str | None = None,
    last: int | None = None,
) -> list[SnapshotRef]:
    """Select the window's snapshots from the index.

    ``from_ts``/``to_ts`` are inclusive UTC bounds on the snapshot
    timestamp; ``last`` keeps the N most recent of what remains. A
    listed name without a parsable timestamp cannot be bounded and is
    skipped. ``last`` must be a positive integer when given.
    """
    if last is not None and last < 1:
        raise ValueError(f"--last must be a positive integer, got {last}")
    lo, hi = _as_utc(from_ts), _as_utc(to_ts)
    dated = [ref for ref in refs if ref.timestamp is not None]
    window = [
        ref
        for ref in dated
        if (lo is None or ref.timestamp >= lo)  # type: ignore[operator]
        and (hi is None or ref.timestamp <= hi)  # type: ignore[operator]
    ]
    window.sort(key=lambda ref: (ref.timestamp, ref.snapshot_id))  # type: ignore[index,return-value]
    if last is not None:
        window = window[-last:]
    return window

# ---------------------------------------------------------------------------
# Snapshot metadata (the manifest's minimal YAML read)
# ---------------------------------------------------------------------------

#: The safe YAML loader of this environment: libyaml's CSafeLoader when
#: the installed PyYAML provides it, the pure-Python SafeLoader as a
#: measured fallback (§9.2, §17). Never FullLoader or Loader.
import yaml as _yaml

try:
    from yaml import CSafeLoader as _SafeLoader

    LOADER_NAME = "CSafeLoader"
except ImportError:  # pragma: no cover - environment-dependent fallback
    from yaml import SafeLoader as _SafeLoader

    LOADER_NAME = "SafeLoader"


def sha256_bytes(data: bytes) -> str:
    """SHA-256 of raw bytes as stored (ADR-002)."""
    return hashlib.sha256(data).hexdigest()


def read_snapshot_metadata(snapshot_id: str, gz_bytes: bytes) -> dict[str, Any]:
    """Read a snapshot's manifest metadata from its gzip bytes.

    Only what the manifest needs (§4.2): ``generated-date``, the item
    count and the top-level keys — so a format change is detected,
    not silently absorbed. The parse itself (the compact excuse
    state) is the work queue's job, not the acquisition's. A
    duplicate item-name is an invalid record and raises with its
    cause (§5.2).
    """
    try:
        text = gzip.decompress(gz_bytes).decode("utf-8", errors="replace")
    except (OSError, EOFError) as error:
        raise TruncatedGzip(
            f"snapshot {snapshot_id} does not decompress: {error}"
        ) from error
    document = _yaml.load(text, Loader=_SafeLoader)
    if not isinstance(document, dict):
        raise ValueError(
            f"snapshot {snapshot_id} is not a YAML mapping: "
            f"{type(document).__name__}"
        )
    items = document.get("sources") or []
    if not isinstance(items, list):
        raise ValueError(
            f"snapshot {snapshot_id} has a non-list sources: "
            f"{type(items).__name__}"
        )
    seen: set[str] = set()
    for item in items:
        name = item.get("item-name") if isinstance(item, dict) else None
        if name is not None and name in seen:
            raise DuplicateItemName(
                f"duplicate item-name in snapshot {snapshot_id}: {name}"
            )
        if name is not None:
            seen.add(name)
    return {
        "generated_date": document.get("generated-date"),
        "item_count": len(items),
        "top_level_keys": sorted(document.keys()),
    }


# ---------------------------------------------------------------------------
# Polite acquisition (§4.1.3)
# ---------------------------------------------------------------------------


class _Politeness:
    """One request at a time, at least ``min_interval`` seconds apart.

    The sleeper and the clock are injected: the battery measures the
    spacing, it never sleeps for real. An interval of zero or less is
    refused at construction — the source is a public service.
    """

    def __init__(
        self,
        min_interval: float,
        sleep: Callable[[float], None],
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if min_interval <= 0:
            raise ValueError(f"min_interval must be positive, got {min_interval}")
        self._min_interval = min_interval
        self._sleep = sleep
        self._monotonic = monotonic
        self._last: float | None = None

    def wait(self) -> None:
        """Sleep until the next request may go out."""
        if self._last is not None:
            elapsed = self._monotonic() - self._last
            remaining = self._min_interval - elapsed
            if remaining > 0:
                self._sleep(remaining)
        self._last = self._monotonic()


@dataclass(frozen=True)
class SnapshotFailure:
    """A listed file that could not be retrieved (§4.2).

    Distinct from a gap: the file was listed and the retrieval failed
    — the HTTP error, the truncated gzip, the invalid record.
    """

    snapshot_id: str
    url: str
    error: str


@dataclass
class WindowResult:
    """What one `ingest britney` window run produced.

    ``manifest`` is the window manifest as a plain dict (written to
    ``manifest_path`` as JSON); ``fetched``/``skipped`` are snapshot
    ids, ``failures`` are :class:`SnapshotFailure` entries.
    """

    year: int
    fetched: list[str]
    skipped: list[str]
    failures: list[SnapshotFailure]
    manifest: dict[str, Any]
    manifest_path: Path


def _retrieved_now() -> str:
    """The retrieval timestamp of a fetch, UTC ISO-8601."""
    return datetime.now(timezone.utc).isoformat()


def _iso_z(moment: datetime) -> str:
    """A UTC timestamp as ISO-8601 with the Z suffix (the archive's form)."""
    return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _manifest_generated_date(value: Any) -> Any:
    """A generated-date as the manifest can carry it (§4.2).

    The manifest is JSON, and the real source serves the
    generated-date unquoted — YAML hands the acquisition a
    ``datetime`` (or ``date``), which json cannot serialize. Every
    generated-date that flows into a manifest entry goes through
    here: datetimes become ISO 8601 UTC (the same form the file
    names carry), anything else (a quoted string, None) passes
    through untouched.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None:  # YAML timestamps are naive; UTC
            value = value.replace(tzinfo=timezone.utc)
        return _iso_z(value)
    if isinstance(value, date):
        return value.isoformat()
    return value


def _fetch_with_retries(
    ref: SnapshotRef,
    politeness: _Politeness,
    *,
    sleep: Callable[[float], None],
    attempts: int,
    backoff_base: float,
    min_interval: float,
    timeout: float,
) -> tuple[bytes | None, dict[str, str], str | None]:
    """One snapshot's download with backoff.

    Returns ``(body, validators, error)``: exactly one of body/error
    is set. Backoff sleeps through the injected sleeper — the battery
    never sleeps for real.
    """
    error: str | None = None
    for attempt in range(attempts):
        if attempt:
            sleep(backoff_base**attempt * min_interval)
        politeness.wait()
        try:
            body, validators = _http_get(ref.url, timeout=timeout)
            declared = validators.get("content-length")
            if declared is not None and int(declared) != len(body):
                raise TruncatedGzip(
                    f"snapshot {ref.snapshot_id} is truncated: "
                    f"content-length {declared} != {len(body)} bytes"
                )
            return body, validators, None
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as err:
            error = str(err)
    return None, {}, error


def acquire_window(
    refs: list[SnapshotRef],
    *,
    year: int,
    raw_dir: Path,
    min_interval: float = MIN_REQUEST_INTERVAL,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    timeout: float = HTTP_TIMEOUT_SECONDS,
    attempts: int = FETCH_ATTEMPTS,
    backoff_base: float = BACKOFF_BASE,
    run_id: str | None = None,
    codename: str | None = None,
) -> WindowResult:
    """Fetch the window's listed snapshots politely, once, into the store.

    Every file is downloaded once (§4.1.3): a file already in the raw
    store is skipped and read back from disk — re-runs are idempotent
    by SHA-256. A listed file that fails (HTTP error, truncated gzip)
    is retried with backoff, then recorded as a *failure*; a period
    with no published snapshot is a *gap*; the two never confuse.
    Gaps are data: consecutive observed snapshots further apart than
    :data:`GAP_THRESHOLD_SECONDS` bound one gap entry (§4.2). Two
    snapshots sharing a generated-date raise
    :class:`GeneratedDateTie` (§5.2). One window manifest describes
    the run (§4.2) and one ExecutionRecord is written per run (§8)
    via :func:`record_execution`.
    """
    if min_interval <= 0:
        raise ValueError(
            f"min_interval must be positive (one request at a time, "
            f"at least 1 s apart), got {min_interval}"
        )
    run_id = run_id or f"run-{uuid.uuid4().hex[:12]}"
    started = time.monotonic()
    politeness = _Politeness(min_interval, sleep, monotonic)
    store_dir = raw_dir / RAW_SUBDIR / str(year)
    store_dir.mkdir(parents=True, exist_ok=True)

    # An empty window is a valid result: the manifest still describes
    # the year's index (0 snapshots, 0 gaps, 0 failures). The listing
    # URL keeps its own name so the module's index_url() stays
    # callable in the empty branch — shadowing it made the empty
    # window crash with UnboundLocalError instead of recording.
    listing_url = (
        refs[0].url.rsplit("/", 1)[0] + "/" if refs else index_url(year)
    )
    fetched: list[str] = []
    skipped: list[str] = []
    failures: list[SnapshotFailure] = []
    entries: list[dict[str, Any]] = []
    observed: list[tuple[datetime, dict[str, Any]]] = []

    for ref in refs:
        path = store_dir / f"{ref.snapshot_id}.yaml.gz"
        if path.exists():
            body = path.read_bytes()
            skipped.append(ref.snapshot_id)
            validators: dict[str, str] = {}
        else:
            body, validators, error = _fetch_with_retries(
                ref,
                politeness,
                sleep=sleep,
                attempts=attempts,
                backoff_base=backoff_base,
                min_interval=min_interval,
                timeout=timeout,
            )
            if error is not None:
                failures.append(
                    SnapshotFailure(
                        snapshot_id=ref.snapshot_id, url=ref.url, error=error
                    )
                )
                continue
            path.write_bytes(body)
            fetched.append(ref.snapshot_id)
        try:
            metadata = read_snapshot_metadata(ref.snapshot_id, body)
        except (TruncatedGzip, DuplicateItemName, ValueError, _yaml.YAMLError) as err:
            if ref.snapshot_id in fetched and path.exists():
                path.unlink()
            fetched = [s for s in fetched if s != ref.snapshot_id]
            failures.append(
                SnapshotFailure(
                    snapshot_id=ref.snapshot_id, url=ref.url, error=str(err)
                )
            )
            continue
        entry = {
            "snapshot_id": ref.snapshot_id,
            "url": ref.url,
            "retrieved_at": _retrieved_now(),
            "etag": validators.get("etag"),
            "last_modified": validators.get("last-modified"),
            "size_bytes": len(body),
            "sha256": sha256_bytes(body),
            "generated_date": _manifest_generated_date(metadata["generated_date"]),
            "item_count": metadata["item_count"],
            "top_level_keys": metadata["top_level_keys"],
            "from_store": ref.snapshot_id in skipped,
        }
        entries.append(entry)
        if ref.timestamp is not None:
            observed.append((ref.timestamp, entry))

    # Snapshots are totally ordered by generated_date; a tie is an
    # error (§5.2), never a silent pick.
    dates = [entry["generated_date"] for _, entry in observed]
    if len(dates) != len(set(dates)):
        tied = sorted({d for d in dates if dates.count(d) > 1})
        raise GeneratedDateTie(
            "two snapshots of the window share a generated-date: "
            + ", ".join(str(d) for d in tied)
        )

    observed.sort(key=lambda pair: pair[0])
    gaps: list[dict[str, Any]] = []
    for (t_prev, _), (t_next, _) in zip(observed, observed[1:]):
        seconds = int((t_next - t_prev).total_seconds())
        if seconds > GAP_THRESHOLD_SECONDS:
            gaps.append(
                {
                    "start": _iso_z(t_prev),
                    "end": _iso_z(t_next),
                    "seconds": seconds,
                }
            )

    manifest = {
        "source": "britney",
        "run_id": run_id,
        "year": year,
        "retrieved_at": _retrieved_now(),
        "coordinates": {**COORDINATES, "codename": codename},
        "index": {"url": listing_url, "listed": len(refs)},
        "snapshots": entries,
        "skipped": sorted(skipped),
        "failures": [
            {"snapshot_id": f.snapshot_id, "url": f.url, "error": f.error}
            for f in failures
        ],
        "gaps": gaps,
        "loader": LOADER_NAME,
    }
    manifest_path = store_dir / f"manifest-{run_id}.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    result = WindowResult(
        year=year,
        fetched=fetched,
        skipped=skipped,
        failures=failures,
        manifest=manifest,
        manifest_path=manifest_path,
    )
    # One execution record per run (§8): files fetched/skipped/failed/gaps.
    record_execution(
        result,
        raw_dir=raw_dir,
        duration_seconds=time.monotonic() - started,
        codename=codename,
    )
    return result


# ---------------------------------------------------------------------------
# Execution record (§8: one per run)
# ---------------------------------------------------------------------------


def record_execution(
    result: WindowResult,
    *,
    raw_dir: Path,
    duration_seconds: float,
    codename: str | None = None,
) -> None:
    """Write this run's ExecutionRecord: files fetched/skipped/failed/gaps."""
    from debian_observer.analytics.models import ExecutionRecord
    from debian_observer.analytics.store import ExecutionStore

    counts = {
        "fetched": len(result.fetched),
        "skipped": len(result.skipped),
        "failed": len(result.failures),
        "gaps": len(result.manifest["gaps"]),
        "year": result.year,
        "codename": codename,
    }
    record = ExecutionRecord(
        run_id=result.manifest["run_id"],
        run_at=datetime.now(timezone.utc),
        execution_type="ingestion",
        source_name="britney",
        records_processed=len(result.manifest["snapshots"]),
        duration_seconds=duration_seconds,
        raw_bytes=sum(e["size_bytes"] for e in result.manifest["snapshots"]),
        raw_path=str(result.manifest_path),
        error_count=len(result.failures),
        error_message=result.failures[0].error if result.failures else None,
        host_info=None,
        metadata=json.dumps(counts),
    )
    ExecutionStore(str(raw_dir.parent / "execution_runs.duckdb")).insert(record)