"""Canonical event envelope for Debian Observatory domain events.

The ``EventEnvelope`` is the universal wrapper used for all messages
published to the event backbone (NATS JetStream in R1). It provides:

- Identity          — ``event_id`` for idempotency and deduplication
- Typing            — ``event_type`` for routing and deserialization
- Versioning        — ``schema_version`` for forward/backward compatibility
- Provenance        — ``source``, ``observed_at``, ``effective_at``
- Referencing       — ``entity_id`` links the event to a canonical domain entity
- Extensibility     — ``payload`` carries the domain-specific data as a dict

All envelope fields are primitive or JSON-native types so that the
envelope can be stored directly in Parquet without complex nesting.
The ``payload`` is serialized as a JSON string column for analytical
storage; consumers deserializing for processing get the parsed dict.

Usage::

    from debian_observer.events import EventEnvelope
    from debian_observer.events.types import PackageObservedPayload

    payload = PackageObservedPayload(package=package, observation=obs)
    envelope = EventEnvelope.from_payload(
        event_type="PackageObserved",
        source="udd",
        entity_id=package.package_id,
        payload_model=payload,
    )
    json_bytes = envelope.model_dump_json().encode()
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from debian_observer.common.types import EventType, SchemaVersion

# ---------------------------------------------------------------------------
# UTC helper
# ---------------------------------------------------------------------------


def _utc_now() -> datetime:
    """Return the current UTC datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Event Envelope
# ---------------------------------------------------------------------------


class EventEnvelope(BaseModel):
    """Universal wrapper for all Debian Observatory domain events.

    This model is designed to be:

    * **Immutable**       — frozen; instances are hashable
    * **Self-describing** — event_type + schema_version tell consumers
      exactly what is in the payload
    * **Provenance-rich** — observed_at + source + ingestion_run_id
      preserve lineage
    * **Idempotent**      — event_id allows duplicate detection
    * **Storage-ready**   — serializes to Parquet via ``to_parquet_row()``

    The ``payload`` field is intentionally a ``dict[str, Any]`` at the
    envelope level to allow generic handling (routing, logging,
    Parquet storage). Strongly-typed payload models live in
    ``debian_observer.events.types`` and should be used when
    constructing envelopes.

    Attributes:
        event_id: UUID v4 uniquely identifying this event instance.
        event_type: Canonical event type name (e.g. "PackageObserved").
        schema_version: Schema version for this event type (starts at 1).
        source: Data source that produced this event (udd, snapshot, etc.).
        observed_at: When the pipeline observed the underlying fact (UTC).
        effective_at: When the underlying fact became true in the domain (UTC).
        entity_id: Canonical identifier of the entity this event concerns.
        ingestion_run_id: Identifier grouping events from a single ingestion.
        payload: Domain-specific event data as a JSON-compatible dict.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,
        str_strip_whitespace=True,
    )

    event_id: uuid.UUID = Field(
        default_factory=uuid.uuid4,
        description="UUID v4 uniquely identifying this event instance.",
    )
    event_type: str = Field(
        ...,
        description="Canonical event type name.",
        examples=["PackageObserved", "PackageVersionObserved"],
    )
    schema_version: int = Field(
        default=SchemaVersion.V1,
        ge=1,
        description="Schema version for this event type.",
    )
    source: str = Field(
        ...,
        description="Data source that produced this event.",
        examples=["udd", "snapshot", "archive"],
    )
    observed_at: datetime = Field(
        default_factory=_utc_now,
        description="When the pipeline observed the underlying fact (UTC).",
    )
    effective_at: datetime | None = Field(
        default=None,
        description="When the underlying fact became true in the domain (UTC).",
    )
    entity_id: str = Field(
        ...,
        description="Canonical identifier of the entity this event concerns.",
    )
    ingestion_run_id: str | None = Field(
        default=None,
        description="Identifier grouping events from a single ingestion run.",
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Domain-specific event data as a JSON-compatible dict.",
    )

    # -----------------------------------------------------------------------
    # Validators
    # -----------------------------------------------------------------------

    @field_validator("event_type", "source", "entity_id", mode="before")
    @classmethod
    def _validate_required_str(cls, value: Any) -> Any:
        if not value or not isinstance(value, str):
            raise ValueError("Field must be a non-empty string")
        return value

    @field_validator("observed_at", "effective_at")
    @classmethod
    def _ensure_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @model_validator(mode="before")
    @classmethod
    def _sync_timestamps(cls, data: Any) -> Any:
        """Ensure observed_at and effective_at default to the same timestamp.

        Prevents race conditions where two independent ``_utc_now()`` calls
        produce different values, which can trigger the ordering validator.
        """
        if isinstance(data, dict):
            has_obs = data.get("observed_at") is not None
            has_eff = data.get("effective_at") is not None
            if not has_obs and not has_eff:
                now = _utc_now()
                data["observed_at"] = now
                data["effective_at"] = now
            elif has_obs and not has_eff:
                data["effective_at"] = data["observed_at"]
            elif has_eff and not has_obs:
                data["observed_at"] = data["effective_at"]
        return data

    @field_validator("effective_at")
    @classmethod
    def _effective_not_after_observed(cls, value: datetime, info: Any) -> datetime:
        """Raise if effective_at is after observed_at.

        A fact cannot become effective after it was observed.
        """
        observed = info.data.get("observed_at")
        if observed is not None and value > observed:
            raise ValueError(
                "effective_at cannot be later than observed_at: "
                f"effective={value.isoformat()}, observed={observed.isoformat()}"
            )
        return value

    @field_validator("payload", mode="before")
    @classmethod
    def _validate_payload(cls, value: Any) -> Any:
        """Ensure payload is a JSON-serializable dict."""
        if value is None:
            return {}
        if isinstance(value, BaseModel):
            return value.model_dump(mode="json")
        if not isinstance(value, dict):
            raise ValueError(f"payload must be a dict, got {type(value).__name__}")
        return value

    # -----------------------------------------------------------------------
    # Construction helpers
    # -----------------------------------------------------------------------

    @classmethod
    def from_payload(
        cls,
        *,
        event_type: EventType,
        source: str,
        entity_id: str,
        payload_model: BaseModel,
        schema_version: int = SchemaVersion.V1,
        observed_at: datetime | None = None,
        effective_at: datetime | None = None,
        ingestion_run_id: str | None = None,
        event_id: uuid.UUID | None = None,
    ) -> EventEnvelope:
        """Construct an envelope from a strongly-typed payload model.

        This is the preferred factory method. It extracts the payload
        data from the typed model and embeds it in the envelope while
        preserving type safety at construction time.

        Args:
            event_type: Canonical event type string.
            source: Data source identifier.
            entity_id: Canonical entity identifier.
            payload_model: A Pydantic BaseModel containing the event payload.
            schema_version: Schema version (default 1).
            observed_at: Observation timestamp (default now).
            effective_at: Domain-effective timestamp (default now).
            ingestion_run_id: Optional ingestion run grouping ID.
            event_id: Optional explicit event ID (default random UUIDv4).

        Returns:
            A frozen EventEnvelope ready for publishing.
        """
        now = _utc_now()
        return cls(
            event_id=event_id or uuid.uuid4(),
            event_type=event_type,
            schema_version=schema_version,
            source=source,
            observed_at=observed_at or now,
            effective_at=effective_at or now,
            entity_id=entity_id,
            ingestion_run_id=ingestion_run_id,
            payload=payload_model.model_dump(mode="json"),
        )

    # -----------------------------------------------------------------------
    # Serialization helpers for messaging and storage
    # -----------------------------------------------------------------------

    def to_json_bytes(self) -> bytes:
        """Serialize to JSON bytes for messaging (NATS, HTTP, etc.)."""
        return self.model_dump_json().encode("utf-8")

    def to_parquet_row(self) -> dict[str, Any]:
        """Flatten to a Parquet/Arrow-compatible row dict.

        The ``payload`` dict is serialized as a JSON string so that
        Parquet can store it in a single string column without
        complex nested schema requirements. This preserves the full
        event for replay while keeping the analytical schema flat.
        """
        return {
            "event_id": str(self.event_id),
            "event_type": self.event_type,
            "schema_version": self.schema_version,
            "source": self.source,
            "observed_at": self.observed_at.isoformat(),
            "effective_at": self.effective_at.isoformat(),
            "entity_id": self.entity_id,
            "ingestion_run_id": self.ingestion_run_id,
            "payload_json": json.dumps(
                self.payload, sort_keys=True, ensure_ascii=False
            ),
        }

    def get_payload_model(self) -> Any:
        """Attempt to deserialize the payload into a typed model.

        Returns:
            An instance of the appropriate payload type if ``event_type``
            is recognized, otherwise returns the raw payload dict.

        Note:
            This is a convenience for consumers. The event backbone
            does not require typed deserialization; consumers may
            work directly with ``self.payload`` if they prefer.
        """
        from debian_observer.events.types import (
            MigrationObservedPayload,
            PackageObservedPayload,
            PackageVersionObservedPayload,
            ReleaseObservedPayload,
        )

        mapping: dict[str, type[BaseModel]] = {
            "PackageObserved": PackageObservedPayload,
            "PackageVersionObserved": PackageVersionObservedPayload,
            "ReleaseObserved": ReleaseObservedPayload,
            "MigrationObserved": MigrationObservedPayload,
        }

        model_cls = mapping.get(self.event_type)
        if model_cls is None:
            return self.payload
        return model_cls.model_validate(self.payload)
