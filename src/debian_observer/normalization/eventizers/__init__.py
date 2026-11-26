"""Normalization: eventizer registry (ADR-014).

A source owns a registry of eventizers; ``normalize <source>
--eventizer NAME`` selects one (default ``package-versions``, retro-compatible
behaviour). Each eventizer is an object with:

- ``name`` — the CLI-visible eventizer name;
- ``event_type`` — the event type it emits;
- ``shape`` — the ``TableShape`` its table must carry (the shape
  gate checks the **selected eventizer's** shape, not the first
  registry match — a first match cannot discriminate when one
  shape is a superset of another);
- ``eventize(row)`` — the **full row** (all COPY columns, as
  yielded by ``iter_dump_rows``) to an iterator of events and/or
  invalid-record dicts.
"""

from __future__ import annotations

from .dependencies import DependenciesEventizer
from .excuses import ExcusesEventizer
from .package_versions import PackageVersionsEventizer
from .popcon import PopconEventizer

# The registry maps each source name to its eventizers. A new
# eventization of the same source is one entry here plus one
# module beside this one — no CLI change (ADR-014).
EVENTIZERS: dict[str, dict[str, object]] = {
    "udddump": {
        "package-versions": PackageVersionsEventizer(),
        "dependencies": DependenciesEventizer(),
        "popcon": PopconEventizer(),
    },
    # The britney source (R3) has no retro-compatible default: the
    # pair eventizer is its only eventization (§9.1) — `--eventizer
    # excuses` is required, `None` names no default here.
    "britney": {
        "excuses": ExcusesEventizer(),
    },
}


def list_eventizers(source: str) -> list[str]:
    """Names the eventizers a source offers (the CLI perimeter)."""
    return sorted(EVENTIZERS.get(source, {}))


def get_eventizer(source: str, name: str | None):
    """Return the source's eventizer ``name`` (default: the
    retro-compatible one).

    Unknown sources raise ``KeyError`` naming the known sources;
    unknown eventizer names raise ``ValueError`` listing the
    source's eventizers (fail fast, name the perimeter).
    """
    if source not in EVENTIZERS:
        known = ", ".join(sorted(EVENTIZERS))
        raise KeyError(
            f"source {source!r} has no eventizers; known sources: {known}"
        )
    eventizers = EVENTIZERS[source]
    if name is None:
        # The retro-compatible behaviour is the default.
        return eventizers["package-versions"]
    if name not in eventizers:
        available = ", ".join(sorted(eventizers))
        raise ValueError(
            f"eventizer {name!r} is not offered by source {source!r}; "
            f"available eventizers: {available}"
        )
    return eventizers[name]