"""Normalization: the ``package-versions`` eventizer.

The fixed field mapping (validator → transformer → event factory
per record) behind the eventizer interface: same inputs, same
outputs as before the eventizers existed. It is the default of
``--eventizer``.
"""

from __future__ import annotations

from typing import Any, Iterator

from debian_observer.normalization.events import EventFactory
from debian_observer.normalization.transformer import PackageVersionTransformer
from debian_observer.normalization.udddump import SUPPORTED_TABLE_SHAPES
from debian_observer.normalization.validator import UDDValidator


def _packages_shape():
    """The ``packages`` shape from the registry (single source)."""
    for shape in SUPPORTED_TABLE_SHAPES:
        if shape.name == "packages":
            return shape
    raise AssertionError("the packages shape is missing from the registry")


class PackageVersionsEventizer:
    """UDD ``packages`` rows → ``PackageVersionObserved``."""

    name = "package-versions"
    event_type = "PackageVersionObserved"

    def __init__(self) -> None:
        self._validator = UDDValidator()
        self._transformer = PackageVersionTransformer("udddump")
        self._factory = EventFactory("udddump", ingestion_run_id="")
        self.shape = _packages_shape()

    def with_provenance(
        self, *, source_name: str, ingestion_run_id: str
    ) -> "PackageVersionsEventizer":
        """Bind the manifest provenance (source, run id) of this run.

        The registry holds provenance-free instances; a run binds
        its own (the manifest sidecar carries it).
        """
        self._transformer = PackageVersionTransformer(source_name)
        self._factory = EventFactory(source_name, ingestion_run_id)
        return self

    def eventize(
        self, row: dict[str, Any]
    ) -> Iterator[Any]:
        """Full row → events and/or invalid-record dicts (the fixed
        mapping).

        The **full row** arrives (all COPY columns, as yielded by
        ``iter_dump_rows``); the fixed mapping picks its fields
        (package/source/version/distribution/release/architecture/
        component, defaults for the rest) exactly as
        ``read_dump_records`` did before the eventizer existed.
        """
        from pydantic import ValidationError

        record = self._map_row_to_record(row)
        if not self._validator.is_valid(record):
            reason = self._validator.why_invalid(record) or "invalid record"
            yield {"reason": "invalid", "cause": reason, "record": record}
            return
        try:
            version = self._transformer.transform(record)
        except ValidationError as exc:
            cause = "; ".join(
                ".".join(str(loc) for loc in e["loc"]) + ": " + e["msg"]
                for e in exc.errors()
            )
            yield {"reason": "invalid", "cause": cause, "record": record}
            return
        yield self._factory.from_package_version(version)

    @staticmethod
    def _map_row_to_record(row: dict[str, Any]) -> dict[str, Any]:
        """COPY → record mapping, verbatim from udddump.py."""
        return {
            "package": row.get("package") or "unknown",
            "source_package": row.get("source"),
            "version": row.get("version") or "unknown",
            "distribution": row.get("distribution") or "unknown",
            "release": row.get("release") or "unknown",
            "suite": None,  # UDD exposes pockets, not suite roles
            "architecture": row.get("architecture") or "unknown",
            "component": row.get("component") or "unknown",
            "section": row.get("section"),
            "priority": row.get("priority"),
            "source": row.get("source_name") or "udddump",
            "observed_at": None,
        }