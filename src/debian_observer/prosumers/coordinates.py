"""Prosumers: the reusable coordinate filter.

The coordinate selectors (``distribution``, ``release``,
``architecture``, ``components``) are one piece, reused by every
consumer of the event log. The selectors are **optional**: one
that is not given does not filter — an absent ``release`` lets
every release through. A row missing a filtered column is
rejected (it cannot be inside); a row missing an unfiltered
column passes.

``architecture`` keeps its ride-along rule: a filter for ``amd64``
selects ``amd64`` **and** ``all`` (a graph needs both).
``components`` is the '+'-separated list from the CLI (e.g.
``main+non-free-firmware``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CoordinateFilter:
    """Optional coordinate selectors, applied to event rows."""

    distribution: str | None = None
    release: str | None = None
    architecture: str | None = None
    components: str | None = None

    @classmethod
    def from_args(
        cls,
        *,
        distribution: str | None = None,
        release: str | None = None,
        architecture: str | None = None,
        components: str | None = None,
    ) -> "CoordinateFilter":
        """Build from CLI args (empty strings mean: not given)."""
        return cls(
            distribution=distribution or None,
            release=release or None,
            architecture=architecture or None,
            components=components or None,
        )

    @property
    def component_set(self) -> frozenset[str] | None:
        """The parsed '+'-separated component list (None = no filter)."""
        if self.components is None:
            return None
        return frozenset(
            c.strip() for c in self.components.split("+") if c.strip()
        )

    def selects(self, row: dict[str, Any]) -> bool:
        """Whether an event row is inside the selection.

        An absent selector does not filter. A filtered column that
        the row does not carry rejects the row.
        """
        if self.distribution is not None:
            if row.get("distribution") != self.distribution:
                return False
        if self.release is not None:
            if row.get("release") != self.release:
                return False
        if self.architecture is not None:
            arch = row.get("architecture")
            if arch is None or arch not in (self.architecture, "all"):
                return False
        component_set = self.component_set
        if component_set is not None:
            component = row.get("component")
            if component is None or component not in component_set:
                return False
        return True