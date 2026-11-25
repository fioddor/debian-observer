"""The britney archive index and the window selection (`ingest britney`).

The truth of the archive is the file listing of
``update_excuses/<year>/`` — never a computed name pattern
(docs/release3.project.v0.1.md §4.1.2). These pins hold that contract:
the index is fetched once and cached in the raw store, the window is
selected from the listed names only, and ``snapshot_id`` is the
archive file stem.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from debian_observer.cli.main import cli
from debian_observer.ingestion import britney

# ---------------------------------------------------------------------------
# Synthetic, deterministic index fixture
# ---------------------------------------------------------------------------

#: A realistic directory listing: yaml.gz snapshots with varied run
#: suffixes, the sibling html.gz files that must be ignored, and one
#: name that carries no parsable timestamp.
INDEX_SNAPSHOTS = (
    "20260601T010000Z_0001",
    "20260601T010000Z_0002",
    "20260601T050000Z_0001",
    "20260601T090000Z_0001",
    "20260602T010000Z_0001",
    "20260602T130000Z_0001",
    "20260603T010000Z_0001",
)
INDEX_IGNORED = ("20260601T010000Z_0001.html.gz", "README")
INDEX_UNPARSABLE = "not-a-timestamp.yaml.gz"


def index_html(year: int) -> str:
    """A directory listing naming every fixture entry exactly once."""
    names = (
        [f"{s}.yaml.gz" for s in INDEX_SNAPSHOTS]
        + list(INDEX_IGNORED)
        + [INDEX_UNPARSABLE]
    )
    rows = "\n".join(f'<a href="{n}">{n}</a>' for n in names)
    return f"<html><body><h1>Index of /britney/update_excuses/{year}/</h1>\n{rows}\n</body></html>"


#: The archive's own file names: 14 flat digits, no T/Z separator. The
#: real archive (verified against release.debian.org) names its runs
#: this way; the T/Z names of INDEX_SNAPSHOTS are the live follower's
#: file-naming shape (see britney._live_snapshot_id), kept here so both
#: surfaces stay pinned.
ARCHIVE_FLAT_SNAPSHOTS = (
    "20260101000001",
    "20260101120002",
    "20260102000003",
)


def listing_html(names: list[str]) -> str:
    """A directory listing naming exactly the given file names."""
    rows = "\n".join(f'<a href="{n}">{n}</a>' for n in names)
    return f"<html><body>{rows}</body></html>"


def parsed(snapshot_id: str) -> datetime:
    """The UTC timestamp a well-formed snapshot id encodes."""
    return datetime.strptime(snapshot_id[:16], "%Y%m%dT%H%M%SZ").replace(
        tzinfo=timezone.utc
    )


def test_the_index_lists_only_the_yaml_gz_files() -> None:
    """The listing is the truth: every yaml.gz name, nothing else.

    The html.gz siblings and the directory decoration never become
    snapshots; a name without a parsable timestamp stays in the index
    but cannot anchor a window.
    """
    year = 2026
    refs = britney.parse_index(index_html(year), base_url=britney.index_url(year))
    listed = {r.snapshot_id for r in refs}
    assert listed == set(INDEX_SNAPSHOTS) | {INDEX_UNPARSABLE.removesuffix(".yaml.gz")}
    # The html.gz files are not snapshots.
    for name in INDEX_IGNORED:
        assert name not in {r.url.rsplit("/", 1)[-1] for r in refs}
    # Every listed snapshot points at the archive URL of its file name.
    for ref in refs:
        assert ref.url.endswith(f"{ref.snapshot_id}.yaml.gz")


def test_snapshot_id_is_the_archive_file_stem() -> None:
    """snapshot_id is the file name minus .yaml.gz — the run identity."""
    year = 2026
    refs = britney.parse_index(index_html(year), base_url=britney.index_url(year))
    by_id = {r.snapshot_id: r for r in refs}
    assert "20260601T010000Z_0002" in by_id
    assert by_id["20260601T010000Z_0002"].timestamp == parsed(
        "20260601T010000Z_0002"
    )


def test_the_index_is_fetched_once_and_cached_in_the_raw_store(
    tmp_path: Path,
) -> None:
    """The index fetcher caches the listing under the raw store.

    A second window over the same year reads the cache; the source is
    reached once (§4.1.3: the archive is a public service).
    """
    year = 2026
    raw_dir = tmp_path / "raw"
    html = index_html(year)
    calls: list[str] = []

    def fake_fetch(url: str) -> bytes:
        calls.append(url)
        return html.encode()

    first = britney.fetch_index(year, raw_dir=raw_dir, fetch=fake_fetch)
    second = britney.fetch_index(year, raw_dir=raw_dir, fetch=fake_fetch)

    assert calls == [britney.index_url(year)]
    assert {r.snapshot_id for r in first.refs} == {
        r.snapshot_id for r in second.refs
    }
    # The cache file lives under the raw store, in the source's subtree.
    assert first.cache_path.exists()
    assert first.cache_path.parent == raw_dir / britney.RAW_SUBDIR
    assert first.from_cache is False
    assert second.from_cache is True


def test_the_window_is_inclusive_and_utc() -> None:
    """--from/--to keep both boundary snapshots (§8: inclusive, UTC)."""
    year = 2026
    refs = britney.parse_index(index_html(year), base_url=britney.index_url(year))
    lo = parsed("20260601T050000Z")
    hi = parsed("20260602T130000Z")
    window = britney.select_window(refs, from_ts=lo, to_ts=hi)
    ids = [r.snapshot_id for r in window]
    assert ids == [
        "20260601T050000Z_0001",
        "20260601T090000Z_0001",
        "20260602T010000Z_0001",
        "20260602T130000Z_0001",
    ]


def test_naive_bounds_are_read_as_utc() -> None:
    """A bound without a timezone is UTC, never local time."""
    year = 2026
    refs = britney.parse_index(index_html(year), base_url=britney.index_url(year))
    window = britney.select_window(
        refs,
        from_ts=datetime(2026, 6, 2, 1, 0, 0),  # naive == UTC
        to_ts=datetime(2026, 6, 2, 13, 0, 0),
    )
    assert [r.snapshot_id for r in window] == [
        "20260602T010000Z_0001",
        "20260602T130000Z_0001",
    ]


def test_last_selects_the_most_recent_snapshots() -> None:
    """--last N is the N most recent listed snapshots of the index."""
    year = 2026
    refs = britney.parse_index(index_html(year), base_url=britney.index_url(year))
    window = britney.select_window(refs, last=3)
    expected = sorted(INDEX_SNAPSHOTS, key=lambda s: parsed(s))[-3:]
    expected.sort(key=lambda s: parsed(s))
    assert [r.snapshot_id for r in window] == expected


def test_the_window_skips_unparsable_names() -> None:
    """A name without a timestamp cannot anchor a window."""
    year = 2026
    refs = britney.parse_index(index_html(year), base_url=britney.index_url(year))
    assert all(
        r.timestamp is not None for r in britney.select_window(refs, last=100)
    )


def test_a_negative_or_zero_last_is_rejected() -> None:
    """--last 0 or negative is a usage error, not an empty window."""
    year = 2026
    refs = britney.parse_index(index_html(year), base_url=britney.index_url(year))
    with pytest.raises(ValueError):
        britney.select_window(refs, last=0)
    with pytest.raises(ValueError):
        britney.select_window(refs, last=-1)


def test_the_archive_s_flat_names_carry_timestamps() -> None:
    """The archive's real names are 14 flat digits, no T/Z separator.

    Both surfaces name files with a UTC timestamp — the archive with
    flat digits, the live follower with T/Z separators — and a name
    of either form anchors a window (§4.1.2).
    """
    year = 2026
    names = [f"{s}.yaml.gz" for s in ARCHIVE_FLAT_SNAPSHOTS]
    refs = britney.parse_index(
        listing_html(names), base_url=britney.index_url(year)
    )
    assert {r.snapshot_id for r in refs} == set(ARCHIVE_FLAT_SNAPSHOTS)
    expected = {
        "20260101000001": datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
        "20260101120002": datetime(2026, 1, 1, 12, 0, 2, tzinfo=timezone.utc),
        "20260102000003": datetime(2026, 1, 2, 0, 0, 3, tzinfo=timezone.utc),
    }
    assert {r.snapshot_id: r.timestamp for r in refs} == expected
    # The live-style T/Z name still anchors: one parser, two formats.
    live_id = "20260101T000004Z"
    live_refs = britney.parse_index(
        listing_html([f"{live_id}.yaml.gz"]),
        base_url=britney.index_url(year),
    )
    assert live_refs[0].timestamp == datetime(
        2026, 1, 1, 0, 0, 4, tzinfo=timezone.utc
    )


def test_the_window_is_not_empty_over_flat_archive_names() -> None:
    """A window over the archive's flat names selects snapshots.

    The index is the truth of whichever surface it came from: the
    archive's flat names bound a window exactly like the live-style
    names — never an empty window against a well-formed listing.
    """
    year = 2026
    names = [f"{s}.yaml.gz" for s in ARCHIVE_FLAT_SNAPSHOTS]
    refs = britney.parse_index(
        listing_html(names), base_url=britney.index_url(year)
    )
    window = britney.select_window(
        refs,
        from_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
        to_ts=datetime(2026, 1, 1, 23, 59, 59, tzinfo=timezone.utc),
    )
    assert [r.snapshot_id for r in window] == [
        "20260101000001",
        "20260101120002",
    ]
    assert (
        britney.select_window(refs, last=2)[-1].snapshot_id
        == "20260102000003"
    )


# ---------------------------------------------------------------------------
# CLI surface
# ---------------------------------------------------------------------------


def test_ingest_help_lists_britney() -> None:
    """`ingest --help` lists one subcommand per source — britney included."""
    result = CliRunner().invoke(cli, ["ingest", "--help"])
    assert result.exit_code == 0
    assert "britney" in result.output


def test_ingest_britney_help_documents_the_window() -> None:
    """The subcommand help shows the window selectors and their meaning."""
    result = CliRunner().invoke(cli, ["ingest", "britney", "--help"])
    assert result.exit_code == 0
    for word in ("--from", "--to", "--last", "--year", "--raw-dir"):
        assert word in result.output


def test_ingest_britney_selects_from_a_fetched_index(tmp_path: Path) -> None:
    """The command fetches the index and echoes the selected window."""
    year = 2026
    refs = britney.parse_index(
        index_html(year), base_url=britney.index_url(year)
    )

    from types import SimpleNamespace

    def fake_fetch_index(year_: int, **kwargs: object) -> object:
        return SimpleNamespace(
            refs=refs, cache_path=tmp_path / "cache.html", from_cache=False
        )

    with patch(
        "debian_observer.cli.commands.ingest.fetch_index",
        side_effect=fake_fetch_index,
    ) as fetch:
        result = CliRunner().invoke(
            cli,
            [
                "ingest", "britney",
                "--year", str(year),
                "--last", "2",
                "--raw-dir", str(tmp_path / "raw"),
            ],
        )

    assert result.exit_code == 0
    fetch.assert_called_once()
    expected = sorted(INDEX_SNAPSHOTS, key=lambda s: parsed(s))[-2:]
    for snapshot_id in expected:
        assert snapshot_id in result.output