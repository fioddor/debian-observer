"""Normalization: the ``dependencies`` eventizer.

One ``DependencyObserved`` per clause of ``depends``/
``pre_depends``. The eventizer **observes, nothing else**: the
condition travels verbatim (split on ``,`` only), with its position
discriminator, whether it holds ``|`` alternatives and how many.
It does not parse the grammar inside a condition, does not pick an
alternative and does not resolve a target.

Minimum validation only: a clause that uses the deprecated ``<``/
``>`` operators (Debian Policy §7.1) becomes an invalid record with
the cause ``deprecated operator``, never a silent drop.
"""

from __future__ import annotations

import re
from typing import Any, Iterator

from debian_observer.events.envelope import EventEnvelope
from debian_observer.normalization.events import EventFactory
from debian_observer.normalization.udddump import TableShape


class DependenciesEventizer:
    """``packages`` rows → ``DependencyObserved`` events (per clause)."""

    name = "dependencies"
    event_type = "DependencyObserved"
    shape = TableShape(
        name="dependencies",
        required_columns=frozenset(
            {
                "package",
                "version",
                "architecture",
                "distribution",
                "release",
                "depends",
                "pre_depends",
            }
        ),
    )

    def __init__(self) -> None:
        self._factory: EventFactory | None = None

    def with_provenance(
        self, *, source_name: str, ingestion_run_id: str
    ) -> "DependenciesEventizer":
        """Bind the manifest provenance (source, run id) of this run."""
        self._factory = EventFactory(source_name, ingestion_run_id)
        return self

    # -- the field split ------------------------------------------------

    #: Deprecated operators (Policy 7.1): bare `<` / `>` (not the
    #: modern `<<` / `>>`) are accepted by the archive's own tooling
    #: but deprecated; the eventizer keeps such clauses as invalid
    #: records instead of events.
    _DEPRECATED_RE = re.compile(r"(?:^|[^<>=])([<>])(?:[^<>=]|$)")

    @staticmethod
    def _split_clauses(field: str) -> list[str]:
        """Split a relationship field into clauses (trimmed, kept)."""
        return [c.strip() for c in field.split(",") if c.strip()]

    @staticmethod
    def _has_deprecated_operator(clause: str) -> bool:
        """Bare `<`/`>` (not `<<`, `<=`, `>=`, `>>`) — Policy 7.1.

        The four modern two-char operators are consumed whole: only
        a lone operator char (not part of any of them) is deprecated.
        """
        i = 0
        while i < len(clause):
            char = clause[i]
            if char not in "<>":
                i += 1
                continue
            two = clause[i : i + 2]
            if two in ("<<", "<=", ">=", ">>"):
                i += 2
                continue
            # a lone operator char, or a run like "><": deprecated
            return True
        return False

    def eventize(
        self, row: dict[str, Any]
    ) -> Iterator[Any]:
        """Full row → one ``DependencyObserved`` per clause."""
        if self._factory is None:
            self._factory = EventFactory("udddump", "")
        factory = self._factory
        for kind, column in (
            ("depends", "depends"),
            ("pre-depends", "pre_depends"),
        ):
            field = row.get(column)
            if not field or not str(field).strip():
                continue
            for clause_index, clause in enumerate(
                self._split_clauses(str(field))
            ):
                if self._has_deprecated_operator(clause):
                    yield {
                        "reason": "invalid",
                        "cause": (
                            f"deprecated operator (Debian Policy 7.1): "
                            f"{clause!r}"
                        ),
                        "record": {
                            "package": row.get("package"),
                            "version": row.get("version"),
                            "kind": kind,
                            "clause_index": clause_index,
                            "condition": clause,
                        },
                    }
                    continue
                yield self._event(
                    factory, row, kind, clause_index, clause
                )

    @staticmethod
    def _event(
        factory: EventFactory,
        row: dict[str, Any],
        kind: str,
        clause_index: int,
        clause: str,
    ) -> EventEnvelope:
        from datetime import datetime, timezone

        from debian_observer.events.types import (
            DependencyObservedPayload,
            ObservationMeta,
        )

        now = datetime.now(timezone.utc)
        package = row.get("package") or "unknown"
        architecture = row.get("architecture") or "unknown"
        observation = ObservationMeta(
            source=factory.source_name,
            observed_at=now,
            effective_at=now,
            ingestion_run_id=factory.ingestion_run_id,
        )
        payload = DependencyObservedPayload(
            observation=observation,
            distribution=row.get("distribution") or "unknown",
            release=row.get("release") or "unknown",
            architecture=architecture,
            component=row.get("component") or "unknown",
            package=package,
            version=row.get("version") or "unknown",
            kind=kind,
            clause_index=clause_index,
            condition=clause,
            is_complex="|" in clause,
            alternatives=len(
                [a for a in clause.split("|") if a.strip()]
            ),
        )
        return EventEnvelope.from_payload(
            event_type="DependencyObserved",
            source=factory.source_name,
            entity_id=f"{package}:{architecture}",
            payload_model=payload,
            ingestion_run_id=factory.ingestion_run_id,
            observed_at=now,
            effective_at=now,
        )