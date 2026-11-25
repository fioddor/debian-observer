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

import re
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
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

#: Seconds between two live polls (§4.1.3): no more often than 15 min.
MIN_LIVE_POLL_INTERVAL = 15 * 60

#: Subtree of the raw store that belongs to this source.
RAW_SUBDIR = "britney"

#: Consecutive observed snapshots further apart than this many seconds
#: are a gap (a period with no published snapshot, §4.2). The archive
#: cadence is about one run per hour (§2.1); six hours without a
#: snapshot is a missing publication, not a quiet hour.
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
    timeout: float = 60,
) -> tuple[bytes, dict[str, str]]:
    """One conditional GET with the platform's User-Agent (§4.1.3 D6)."""
    request = urllib.request.Request(
        url,
        headers={"User-Agent": BRITNEY_USER_AGENT, **(headers or {})},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read()
        validators = {
            key: value
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
            index_url(year, base_url)
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