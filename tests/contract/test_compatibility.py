"""Contract compatibility tests for Debian Observatory event models.

These tests enforce the data contract that R2 will depend on.
They validate:

- Round-trip serialize → deserialize identity
- Forward compatibility tolerance in payload dicts
- Canonical envelope key presence
- ``event_id`` and ``schema_version`` invariants
- Payload model round-trips for each event type

All tests use the current Pydantic models directly (no JSON Schema).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import pytest

from debian_observer.common.types import MigrationStatus
from debian_observer.events import (
    EventEnvelope,
    MigrationObservedPayload,
    PackageObservedPayload,
    PackageVersionObservedPayload,
    ReleaseObservedPayload,
)
from debian_observer.events.types import ObservationMeta

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_observation_meta(
    source: str = "udd",
    ts: datetime | None = None,
) -> ObservationMeta:
    """Return a deterministic ObservationMeta for contract tests."""
    if ts is None:
        ts = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
    return ObservationMeta(source=source, observed_at=ts, effective_at=ts)


def _make_envelope(**kwargs: Any) -> EventEnvelope:
    """Build a minimal valid EventEnvelope with deterministic defaults."""
    defaults: dict[str, Any] = {
        "event_type": "PackageObserved",
        "source": "udd",
        "entity_id": "package:libc6",
    }
    defaults.update(kwargs)
    return EventEnvelope(**defaults)


# =============================================================================
# 1. Round-trip serialization
# =============================================================================


class TestRoundTripSerialization:
    """Every event must survive serialize → deserialize without loss."""

    def test_event_envelope_json_bytes_roundtrip(self) -> None:
        """EventEnvelope → JSON bytes → EventEnvelope must be identity."""
        obs = _make_observation_meta()
        payload = PackageObservedPayload(
            package_id="libc6:amd64",
            name="libc6",
            section="libs",
            architecture="amd64",
            observation=obs,
        )
        original = EventEnvelope.from_payload(
            event_type="PackageObserved",
            source="udd",
            entity_id="libc6:amd64",
            payload_model=payload,
        )
        json_bytes = original.to_json_bytes()
        restored = EventEnvelope.model_validate_json(json_bytes.decode())
        assert restored == original
        assert restored.event_id == original.event_id
        assert restored.payload == original.payload

    def test_event_envelope_dict_roundtrip(self) -> None:
        """EventEnvelope → dict → EventEnvelope must be identity."""
        original = _make_envelope(
            event_type="ReleaseObserved",
            source="snapshot",
            entity_id="release:bookworm",
            payload={"name": "bookworm", "status": "stable"},
        )
        d = original.model_dump()
        restored = EventEnvelope.model_validate(d)
        assert restored == original

    def test_datetime_fields_preserve_utc(self) -> None:
        """Timezone-aware datetimes must survive round-trip as UTC."""
        ts = datetime(2024, 3, 1, 14, 0, 0, tzinfo=timezone.utc)
        original = _make_envelope(observed_at=ts, effective_at=ts)
        d = original.model_dump()
        restored = EventEnvelope.model_validate(d)
        assert restored.observed_at == original.observed_at
        assert restored.effective_at == original.effective_at
        assert restored.observed_at.tzinfo is not None

    def test_payload_roundtrip_with_nested_data(self) -> None:
        """Nested payload dictionaries must survive round-trip intact."""
        payload: dict[str, Any] = {
            "name": "libc6",
            "version": "2.37-12",
            "tags": ["essential", "library"],
            "meta": {"priority": "required", "size": 123456},
        }
        original = _make_envelope(
            event_type="PackageVersionObserved",
            payload=payload,
        )
        restored = EventEnvelope.model_validate_json(original.model_dump_json())
        assert restored.payload == payload

    def test_serialized_output_is_valid_json(self) -> None:
        """model_dump_json() output must be parseable by the standard json module."""
        envelope = _make_envelope(payload={"key": "value"})
        json_str = envelope.model_dump_json()
        parsed = json.loads(json_str)
        assert isinstance(parsed, dict)
        assert parsed["event_type"] == envelope.event_type
        assert parsed["payload"]["key"] == "value"


# =============================================================================
# 2. Forward compatibility
# =============================================================================


class TestForwardCompatibility:
    """Payload dicts must tolerate unknown fields from future producers."""

    def test_extra_payload_fields_allowed(self) -> None:
        """An envelope carrying a future payload field must still validate."""
        payload: dict[str, Any] = {
            "name": "libc6",
            "version": "2.37-12",
            "future_field": "future_value",
            "future_metric": 42.0,
        }
        envelope = _make_envelope(
            event_type="PackageVersionObserved",
            payload=payload,
        )
        assert envelope.payload["future_field"] == "future_value"
        assert envelope.payload["future_metric"] == 42.0

    def test_extra_payload_nested_fields_allowed(self) -> None:
        """Deeply nested future keys in payload must not break validation."""
        payload: dict[str, Any] = {
            "name": "nginx",
            "meta": {
                "priority": "optional",
                "future_tag": "web-server",
            },
        }
        envelope = _make_envelope(
            event_type="PackageObserved",
            payload=payload,
        )
        assert envelope.payload["meta"]["future_tag"] == "web-server"


# =============================================================================
# 3–5. Contract stability assertions (R1 → R2 guarantee)
# =============================================================================


class TestR1R2ContractStability:
    """Hard assertions that the event contract will not change silently for R2."""

    REQUIRED_ENVELOPE_KEYS = {
        "event_id",
        "event_type",
        "schema_version",
        "source",
        "observed_at",
        "effective_at",
        "entity_id",
        "payload",
    }

    def test_envelope_always_has_required_keys(self) -> None:
        """Every serialised envelope must expose the canonical set of keys."""
        envelope = _make_envelope()
        keys = set(envelope.model_dump().keys())
        assert self.REQUIRED_ENVELOPE_KEYS.issubset(
            keys
        ), f"Missing required keys: {self.REQUIRED_ENVELOPE_KEYS - keys}"

    def test_event_id_format_is_uuid(self) -> None:
        """event_id must be a valid UUID instance (consumers may depend on this)."""
        envelope = _make_envelope()
        assert isinstance(envelope.event_id, uuid.UUID)
        # Must round-trip through string representation
        uuid_str = str(envelope.event_id)
        parsed = uuid.UUID(uuid_str)
        assert parsed == envelope.event_id

    def test_schema_version_is_positive_int(self) -> None:
        """schema_version must remain a positive integer for all event types."""
        envelope = _make_envelope()
        assert isinstance(envelope.schema_version, int)
        assert envelope.schema_version >= 1

    def test_default_schema_version_is_v1(self) -> None:
        """Unspecified events must default to schema version 1."""
        envelope = _make_envelope()
        assert envelope.schema_version == 1

    @pytest.mark.parametrize(
        "event_type",
        [
            "PackageObserved",
            "PackageVersionObserved",
            "ReleaseObserved",
            "MigrationObserved",
        ],
    )
    def test_schema_version_positive_across_types(self, event_type: str) -> None:
        """All declared event types must carry a positive schema_version."""
        envelope = _make_envelope(event_type=event_type)
        assert isinstance(envelope.schema_version, int)
        assert envelope.schema_version >= 1


# =============================================================================
# 6. Payload model serialization round-trip for each type
# =============================================================================


class TestPayloadModelRoundTrip:
    """Each typed payload must survive envelope → JSON → envelope → typed model."""

    def test_package_observed_payload_roundtrip(self) -> None:
        """PackageObservedPayload round-trip through envelope."""
        obs = _make_observation_meta()
        original = PackageObservedPayload(
            package_id="python3",
            name="python3",
            section="python",
            architecture="all",
            maintainer="Debian Python Team",
            description="Python interpreter",
            observation=obs,
        )
        envelope = EventEnvelope.from_payload(
            event_type="PackageObserved",
            source="udd",
            entity_id="python3",
            payload_model=original,
        )
        json_bytes = envelope.to_json_bytes()
        restored_env = EventEnvelope.model_validate_json(json_bytes.decode())
        recovered = restored_env.get_payload_model()
        assert isinstance(recovered, PackageObservedPayload)
        assert recovered == original

    def test_package_version_observed_payload_roundtrip(self) -> None:
        """PackageVersionObservedPayload round-trip through envelope."""
        obs = _make_observation_meta()
        original = PackageVersionObservedPayload(
            package_id="libc6:amd64",
            version="2.37-12",
            distribution="sid",
            architecture="amd64",
            component="main",
            priority="required",
            observation=obs,
        )
        envelope = EventEnvelope.from_payload(
            event_type="PackageVersionObserved",
            source="udd",
            entity_id="libc6:amd64:2.37-12",
            payload_model=original,
        )
        json_bytes = envelope.to_json_bytes()
        restored_env = EventEnvelope.model_validate_json(json_bytes.decode())
        recovered = restored_env.get_payload_model()
        assert isinstance(recovered, PackageVersionObservedPayload)
        assert recovered == original

    def test_release_observed_payload_roundtrip(self) -> None:
        """ReleaseObservedPayload round-trip through envelope."""
        obs = _make_observation_meta()
        original = ReleaseObservedPayload(
            release_id="bookworm",
            name="stable",
            codename="bookworm",
            status="stable",
            version="12.0",
            description="Debian 12 'Bookworm'",
            observation=obs,
        )
        envelope = EventEnvelope.from_payload(
            event_type="ReleaseObserved",
            source="snapshot",
            entity_id="bookworm",
            payload_model=original,
        )
        json_bytes = envelope.to_json_bytes()
        restored_env = EventEnvelope.model_validate_json(json_bytes.decode())
        recovered = restored_env.get_payload_model()
        assert isinstance(recovered, ReleaseObservedPayload)
        assert recovered == original

    def test_migration_observed_payload_roundtrip(self) -> None:
        """MigrationObservedPayload round-trip through envelope."""
        obs = _make_observation_meta()
        original = MigrationObservedPayload(
            migration_id="mig-001",
            source="sid",
            target="trixie",
            package="nginx",
            version="1.24.0-1",
            architecture="amd64",
            status=MigrationStatus.PENDING,
            observation=obs,
        )
        envelope = EventEnvelope.from_payload(
            event_type="MigrationObserved",
            source="udd",
            entity_id="mig-001",
            payload_model=original,
        )
        json_bytes = envelope.to_json_bytes()
        restored_env = EventEnvelope.model_validate_json(json_bytes.decode())
        recovered = restored_env.get_payload_model()
        assert isinstance(recovered, MigrationObservedPayload)
        assert recovered == original
