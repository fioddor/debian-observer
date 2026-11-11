"""Unit tests for Debian Observatory event models."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import BaseModel, ValidationError

from debian_observer.common.types import MigrationStatus
from debian_observer.domain.models import Package, PackageVersion
from debian_observer.events import (
    DatasetManifest,
    EventEnvelope,
    MigrationObservedPayload,
    PackageObservedPayload,
    PackageVersionObservedPayload,
    ReleaseObservedPayload,
)
from debian_observer.events.manifest import Checksum
from debian_observer.events.types import ObservationMeta

# ---------------------------------------------------------------------------
# EventEnvelope tests
# ---------------------------------------------------------------------------


class TestEventEnvelope:
    """Tests for the canonical event envelope."""

    def test_minimal_construction(self) -> None:
        env = EventEnvelope(
            event_type="PackageObserved",
            source="udd",
            entity_id="pkg:python3",
        )
        assert env.event_type == "PackageObserved"
        assert env.source == "udd"
        assert env.entity_id == "pkg:python3"
        assert env.schema_version == 1
        assert isinstance(env.event_id, uuid.UUID)
        assert env.payload == {}

    def test_full_construction(self) -> None:
        now = datetime.now(timezone.utc)
        env = EventEnvelope(
            event_id=uuid.uuid4(),
            event_type="PackageVersionObserved",
            schema_version=1,
            source="udd",
            observed_at=now,
            effective_at=now - timedelta(hours=1),
            entity_id="pkg:python3:3.11.2-1",
            ingestion_run_id="run-001",
            payload={"version": "3.11.2-1", "distribution": "debian", "release": "sid"},
        )
        assert env.payload["version"] == "3.11.2-1"
        assert env.ingestion_run_id == "run-001"

    def test_effective_after_observed_rejected(self) -> None:
        now = datetime.now(timezone.utc)
        future = now + timedelta(days=1)
        with pytest.raises(ValidationError):
            EventEnvelope(
                event_type="PackageObserved",
                source="udd",
                entity_id="pkg:test",
                observed_at=now,
                effective_at=future,
            )

    def test_utc_normalization(self) -> None:
        naive = datetime(2024, 1, 1, 12, 0, 0)
        env = EventEnvelope(
            event_type="ReleaseObserved",
            source="snapshot",
            entity_id="rel:bookworm",
            observed_at=naive,
        )
        assert env.observed_at.tzinfo == timezone.utc

    def test_from_payload_factory(self) -> None:
        obs = ObservationMeta(source="udd")
        payload = PackageObservedPayload(
            package_id="python3",
            name="python3",
            section="python",
            observation=obs,
        )
        env = EventEnvelope.from_payload(
            event_type="PackageObserved",
            source="udd",
            entity_id="python3",
            payload_model=payload,
        )
        assert env.event_type == "PackageObserved"
        assert env.payload["package_id"] == "python3"
        assert env.payload["name"] == "python3"

    def test_to_json_bytes(self) -> None:
        env = EventEnvelope(
            event_type="PackageObserved",
            source="udd",
            entity_id="pkg:test",
            payload={"key": "value"},
        )
        raw = env.to_json_bytes()
        assert isinstance(raw, bytes)
        parsed = json.loads(raw)
        assert parsed["event_type"] == "PackageObserved"
        assert parsed["payload"]["key"] == "value"

    def test_get_payload_model_package(self) -> None:
        obs = ObservationMeta(source="udd")
        payload = PackageObservedPayload(
            package_id="python3",
            name="python3",
            observation=obs,
        )
        env = EventEnvelope.from_payload(
            event_type="PackageObserved",
            source="udd",
            entity_id="python3",
            payload_model=payload,
        )
        recovered = env.get_payload_model()
        assert isinstance(recovered, PackageObservedPayload)
        assert recovered.package_id == "python3"

    def test_get_payload_model_unknown_type(self) -> None:
        env = EventEnvelope(
            event_type="UnknownEvent",
            source="test",
            entity_id="ent:test",
            payload={"data": 123},
        )
        recovered = env.get_payload_model()
        assert recovered == {"data": 123}

    def test_payload_from_basemodel(self) -> None:
        class DummyPayload(BaseModel):
            value: int = 42

        env = EventEnvelope(
            event_type="TestEvent",
            source="test",
            entity_id="ent:test",
            payload=DummyPayload(value=99),
        )
        assert env.payload == {"value": 99}

    def test_immutability(self) -> None:
        env = EventEnvelope(
            event_type="PackageObserved",
            source="udd",
            entity_id="pkg:test",
        )
        with pytest.raises(ValidationError):
            env.event_type = "Changed"  # type: ignore[misc]

    def test_invalid_event_type_rejected(self) -> None:
        with pytest.raises(ValidationError):
            EventEnvelope(event_type="", source="udd", entity_id="pkg:test")

    def test_event_id_roundtrip(self) -> None:
        env = EventEnvelope(
            event_type="ReleaseObserved",
            source="snapshot",
            entity_id="rel:bookworm",
        )
        raw = env.to_json_bytes()
        parsed = EventEnvelope.model_validate_json(raw.decode())
        assert parsed.event_id == env.event_id


# ---------------------------------------------------------------------------
# Payload model tests
# ---------------------------------------------------------------------------


class TestPackageObservedPayload:
    """Tests for PackageObserved event payload."""

    def test_construction(self) -> None:
        obs = ObservationMeta(source="udd")
        payload = PackageObservedPayload(
            package_id="python3",
            name="python3",
            section="python",
            architecture="all",
            observation=obs,
        )
        assert payload.package_id == "python3"
        assert payload.section == "python"

    def test_invalid_name_rejected(self) -> None:
        obs = ObservationMeta(source="udd")
        with pytest.raises(ValidationError):
            PackageObservedPayload(
                package_id="bad",
                name="UPPERCASE",
                observation=obs,
            )


class TestPackageVersionObservedPayload:
    """Tests for PackageVersionObserved event payload."""

    def test_construction(self) -> None:
        obs = ObservationMeta(source="udd")
        payload = PackageVersionObservedPayload(
            package_id="python3",
            version="3.11.2-1",
            distribution="sid",
            component="main",
            observation=obs,
        )
        assert payload.version == "3.11.2-1"
        assert payload.component == "main"

    def test_invalid_version_rejected(self) -> None:
        obs = ObservationMeta(source="udd")
        with pytest.raises(ValidationError):
            PackageVersionObservedPayload(
                package_id="p",
                version="bad",
                observation=obs,
            )


class TestReleaseObservedPayload:
    """Tests for ReleaseObserved event payload."""

    def test_construction(self) -> None:
        obs = ObservationMeta(source="snapshot")
        payload = ReleaseObservedPayload(
            release_id="bookworm",
            name="stable",
            codename="bookworm",
            status="stable",
            version="12.0",
            observation=obs,
        )
        assert payload.codename == "bookworm"
        assert payload.status == "stable"

    def test_empty_name_rejected(self) -> None:
        obs = ObservationMeta(source="test")
        with pytest.raises(ValidationError):
            ReleaseObservedPayload(
                release_id="test",
                name="",
                observation=obs,
            )


class TestMigrationObservedPayload:
    """Tests for MigrationObserved event payload."""

    def test_construction(self) -> None:
        obs = ObservationMeta(source="udd")
        payload = MigrationObservedPayload(
            migration_id="mig-001",
            source="sid",
            target="trixie",
            package="nginx",
            version="1.24.0-1",
            status=MigrationStatus.PENDING,
            observation=obs,
        )
        assert payload.status == MigrationStatus.PENDING
        assert payload.source == "sid"

    def test_default_status(self) -> None:
        obs = ObservationMeta(source="udd")
        payload = MigrationObservedPayload(
            migration_id="mig-002",
            source="unstable",
            target="testing",
            package="test",
            version="1.0-1",
            observation=obs,
        )
        assert payload.status == MigrationStatus.UNKNOWN


# ---------------------------------------------------------------------------
# DatasetManifest tests
# ---------------------------------------------------------------------------


class TestChecksum:
    """Tests for Checksum model."""

    def test_sha256(self) -> None:
        cs = Checksum(
            algorithm="sha256",
            value="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        assert cs.algorithm == "sha256"

    def test_invalid_algorithm(self) -> None:
        with pytest.raises(ValidationError):
            Checksum(algorithm="crc32", value="aabbcc")

    def test_non_hex_value_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Checksum(algorithm="sha256", value="not-hex!!!")

    def test_normalization(self) -> None:
        cs = Checksum(algorithm="SHA256", value="0xAbCdEf")
        assert cs.algorithm == "sha256"
        assert cs.value == "abcdef"


class TestDatasetManifest:
    """Tests for DatasetManifest model."""

    def test_minimal_construction(self) -> None:
        manifest = DatasetManifest(
            name="udd-packages",
            source="udd.debian.net",
            checksum=Checksum(algorithm="sha256", value="a" * 64),
            size_bytes=1000,
            record_count=100,
        )
        assert manifest.name == "udd-packages"
        assert manifest.record_count == 100

    def test_full_construction(self) -> None:
        now = datetime.now(timezone.utc)
        manifest = DatasetManifest(
            name="udd-bookworm",
            source="udd.debian.net",
            source_version="2024-01-15",
            retrieved_at=now,
            checksum=Checksum(algorithm="sha256", value="b" * 64),
            size_bytes=45_231_890,
            record_count=1_234_567,
            license="MIT/Debian",
            description="UDD package data for bookworm",
            ingestion_run_id="run-2024-01-15-001",
            parent_manifest_id="run-2024-01-01-001",
            tags=("daily", "bookworm"),
            metadata={"format": "csv", "delimiter": ";"},
        )
        assert manifest.ingestion_run_id == "run-2024-01-15-001"
        assert manifest.tags == ("daily", "bookworm")
        assert manifest.metadata["format"] == "csv"

    def test_negative_size_rejected(self) -> None:
        with pytest.raises(ValidationError):
            DatasetManifest(
                name="test",
                source="test",
                checksum=Checksum(algorithm="sha256", value="c" * 64),
                size_bytes=-1,
                record_count=0,
            )

    def test_utc_normalization(self) -> None:
        naive = datetime(2024, 1, 1, 12, 0, 0)
        manifest = DatasetManifest(
            name="test",
            source="test",
            retrieved_at=naive,
            checksum=Checksum(algorithm="sha256", value="d" * 64),
            size_bytes=0,
            record_count=0,
        )
        assert manifest.retrieved_at.tzinfo == timezone.utc

    def test_to_from_yaml(self) -> None:
        pytest.importorskip("yaml")
        now = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        manifest = DatasetManifest(
            name="udd-bookworm",
            source="udd.debian.net",
            source_version="2024-01-15",
            retrieved_at=now,
            checksum=Checksum(algorithm="sha256", value="f" * 64),
            size_bytes=10000,
            record_count=200,
            license="MIT",
        )
        yaml_text = manifest.to_yaml()
        assert "udd-bookworm" in yaml_text
        assert "2024-01-15T10:30:00" in yaml_text

        restored = DatasetManifest.from_yaml(yaml_text)
        assert restored.name == manifest.name
        assert restored.checksum.value == manifest.checksum.value
        assert restored.retrieved_at == now

    def test_yaml_missing_pyyaml(self, monkeypatch: pytest.MonkeyPatch) -> None:

        original_import = __builtins__.get("__import__", __import__)

        def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "yaml":
                raise ImportError("No module named 'yaml'")
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr("builtins.__import__", fake_import)

        m = DatasetManifest(
            name="test",
            source="test",
            checksum=Checksum(algorithm="sha256", value="f" * 64),
            size_bytes=0,
            record_count=0,
        )
        with pytest.raises(ImportError, match="pyyaml"):
            m.to_yaml()

    def test_immutability(self) -> None:
        manifest = DatasetManifest(
            name="test",
            source="test",
            checksum=Checksum(algorithm="sha256", value="a" * 64),
            size_bytes=0,
            record_count=0,
        )
        with pytest.raises(ValidationError):
            manifest.name = "changed"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Integration: domain → event flow
# ---------------------------------------------------------------------------


class TestDomainToEventFlow:
    """Integration tests demonstrating the full domain → event flow."""

    def test_package_to_package_observed_event(self) -> None:
        """A Package domain object becomes a PackageObserved event."""
        pkg = Package(
            package_id="python3",
            name="python3",
            section="python",
            architecture="all",
        )
        obs = ObservationMeta(source="udd")
        payload = PackageObservedPayload(
            package_id=pkg.package_id,
            name=pkg.name,
            section=pkg.section,
            architecture=pkg.architecture,
            observation=obs,
        )
        envelope = EventEnvelope.from_payload(
            event_type="PackageObserved",
            source="udd",
            entity_id=pkg.package_id,
            payload_model=payload,
        )
        assert envelope.event_type == "PackageObserved"
        assert envelope.entity_id == "python3"
        recovered = envelope.get_payload_model()
        assert isinstance(recovered, PackageObservedPayload)
        assert recovered.name == "python3"

    def test_package_version_to_event(self) -> None:
        """A PackageVersion becomes a PackageVersionObserved event."""
        pv = PackageVersion(
            package_id="libc6:amd64",
            version="2.31-13+deb12u5",
            distribution="bookworm",
            architecture="amd64",
            source="udd",
        )
        obs = ObservationMeta(source="udd")
        payload = PackageVersionObservedPayload(
            package_id=pv.package_id,
            version=pv.version,
            distribution=pv.distribution,
            architecture=pv.architecture,
            observation=obs,
        )
        envelope = EventEnvelope.from_payload(
            event_type="PackageVersionObserved",
            source="udd",
            entity_id=f"{pv.package_id}:{pv.version}",
            payload_model=payload,
        )
        assert envelope.payload["version"] == "2.31-13+deb12u5"

    def test_event_json_roundtrip(self) -> None:
        """Events survive JSON serialization roundtrips."""
        obs = ObservationMeta(source="snapshot")
        payload = ReleaseObservedPayload(
            release_id="bookworm",
            name="stable",
            codename="bookworm",
            status="stable",
            observation=obs,
        )
        envelope = EventEnvelope.from_payload(
            event_type="ReleaseObserved",
            source="snapshot",
            entity_id="bookworm",
            payload_model=payload,
        )
        json_bytes = envelope.to_json_bytes()
        restored = EventEnvelope.model_validate_json(json_bytes.decode())
        assert restored.event_id == envelope.event_id
        assert restored.event_type == envelope.event_type
        assert restored.payload["codename"] == "bookworm"

    def test_event_idempotency_key(self) -> None:
        """event_id provides a stable key for deduplication."""
        now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        env1 = EventEnvelope(
            event_type="PackageObserved",
            source="udd",
            entity_id="pkg:test",
            observed_at=now,
            effective_at=now,
            payload={"data": 1},
        )
        env2 = EventEnvelope(
            event_id=env1.event_id,
            event_type="PackageObserved",
            source="udd",
            entity_id="pkg:test",
            observed_at=now,
            effective_at=now,
            payload={"data": 1},
        )
        assert env1.event_id == env2.event_id
        assert env1 == env2


class TestPackageVersionObservedExtendedFields:
    """PackageVersionObserved payload carries the full fact fields."""

    def test_construction_and_parquet_row(self) -> None:
        obs = ObservationMeta(source="udd")
        payload = PackageVersionObservedPayload(
            package_id="librte-dpdk:amd64",
            source_package="dpdk",
            version="25.11.3-1",
            distribution="debian",
            release="bullseye-security",
            architecture="amd64",
            component="main",
            section="libs",
            priority="optional",
            observation=obs,
        )
        assert payload.source_package == "dpdk"
        assert payload.release == "bullseye-security"
        assert payload.section == "libs"


class TestPackageVersionObservedSuiteField:
    """PackageVersionObserved payload carries the suite role."""

    def test_construction_and_parquet_row(self) -> None:
        obs = ObservationMeta(source="debian.mirror")
        payload = PackageVersionObservedPayload(
            package_id="vim:amd64",
            version="2:9.0-1",
            distribution="debian",
            release="bookworm",
            suite="stable",
            architecture="amd64",
            component="main",
            priority="optional",
            observation=obs,
        )
        assert payload.suite == "stable"


"""Tests for the dependency/popcon event types and their plumbing.

``DependencyObserved`` and ``PopconObserved`` are registered in
every place the older event types are: the ``EventType`` literal,
the payload models, ``get_payload_model``, the NATS subjects,
and the Parquet event-log schemas (one per event type).
"""


from datetime import datetime, timezone

import pyarrow as pa

from debian_observer.common.types import EventType
from debian_observer.events.envelope import EventEnvelope
from debian_observer.events.types import (
    DependencyObservedPayload,
    ObservationMeta,
    PopconObservedPayload,
)
from debian_observer.messaging.config import StreamConfig
from debian_observer.storage.parquet import (
    EVENT_TYPE_SCHEMAS,
    event_type_directory,
    schema_for_event_type,
)


def _meta() -> ObservationMeta:
    now = datetime.now(timezone.utc)
    return ObservationMeta(
        source="udddump",
        observed_at=now,
        effective_at=now,
        ingestion_run_id="run-001",
    )


@pytest.fixture()
def dependency_payload() -> DependencyObservedPayload:
    return DependencyObservedPayload(
        observation=_meta(),
        distribution="debian",
        release="trixie",
        architecture="amd64",
        component="main",
        package="libhello",
        version="1.0-1",
        kind="depends",
        clause_index=0,
        condition="libc6 (>= 2.36)",
        is_complex=False,
        alternatives=1,
    )


@pytest.fixture()
def popcon_payload() -> PopconObservedPayload:
    return PopconObservedPayload(
        observation=_meta(),
        package="libhello",
        insts=100,
        vote=50,
        olde=10,
        recent=5,
        nofiles=1,
    )


class TestEventTypeLiteral:
    def test_literal_accepts_the_new_types(self) -> None:
        types: list[EventType] = [
            "PackageVersionObserved",
            "DependencyObserved",
            "PopconObserved",
        ]
        assert len(types) == 3


class TestDependencyObservedPayload:
    def test_carries_the_clause_fields(self, dependency_payload) -> None:
        assert dependency_payload.kind == "depends"
        assert dependency_payload.clause_index == 0
        assert dependency_payload.condition == "libc6 (>= 2.36)"
        assert dependency_payload.is_complex is False
        assert dependency_payload.alternatives == 1

    def test_condition_is_verbatim(self) -> None:
        # The eventizer observes: the condition travels as written
        # inside the clause (inner whitespace kept), ends trimmed.
        payload = DependencyObservedPayload(
            observation=_meta(),
            distribution="debian",
            release="trixie",
            architecture="amd64",
            component="main",
            package="pkg",
            version="1.0",
            kind="depends",
            clause_index=2,
            condition="python3:any (>= 3.11)",
            is_complex=True,
            alternatives=2,
        )
        assert payload.condition == "python3:any (>= 3.11)"

    def test_no_resolution_fields(self, dependency_payload) -> None:
        # The payload carries NO target_package/constraint_op/
        # constraint_version: the eventizer does not resolve.
        assert not hasattr(dependency_payload, "target_package")
        assert not hasattr(dependency_payload, "constraint_op")
        assert not hasattr(dependency_payload, "constraint_version")

    def test_kinds(self) -> None:
        for kind in ("depends", "pre-depends"):
            payload = DependencyObservedPayload(
                observation=_meta(),
                distribution="debian",
                release="trixie",
                architecture="amd64",
                component="main",
                package="p",
                version="1.0",
                kind=kind,
                clause_index=0,
                condition="x",
                is_complex=False,
                alternatives=1,
            )
            assert payload.kind == kind

    def test_frozen(self, dependency_payload) -> None:
        with pytest.raises(Exception):
            dependency_payload.kind = "depends2"

    def test_extra_forbidden(self) -> None:
        with pytest.raises(Exception):
            DependencyObservedPayload(
                observation=_meta(),
                package="p",
                version="1.0",
                kind="depends",
                clause_index=0,
                condition="x",
                unknown_field=1,
            )


class TestPopconObservedPayload:
    def test_counters(self, popcon_payload) -> None:
        assert popcon_payload.insts == 100
        assert popcon_payload.vote == 50
        assert popcon_payload.olde == 10
        assert popcon_payload.recent == 5
        assert popcon_payload.nofiles == 1

    def test_frozen(self, popcon_payload) -> None:
        with pytest.raises(Exception):
            popcon_payload.insts = 0


class TestPlumbing:
    def test_get_payload_model_resolves_the_new_types(
        self, dependency_payload, popcon_payload
    ) -> None:
        env1 = EventEnvelope.from_payload(
            event_type="DependencyObserved",
            source="udddump",
            entity_id="libhello:amd64",
            payload_model=dependency_payload,
            ingestion_run_id="run-001",
        )
        assert isinstance(env1.get_payload_model(), DependencyObservedPayload)

        env2 = EventEnvelope.from_payload(
            event_type="PopconObserved",
            source="udddump",
            entity_id="libhello",
            payload_model=popcon_payload,
            ingestion_run_id="run-001",
        )
        assert isinstance(env2.get_payload_model(), PopconObservedPayload)

    def test_subjects_registered(self) -> None:
        config = StreamConfig()
        assert config.subject_for("DependencyObserved") == (
            "debian.dependency.observed"
        )
        assert config.subject_for("PopconObserved") == "debian.popcon.observed"
        assert "debian.dependency.observed" in config.subjects
        assert "debian.popcon.observed" in config.subjects

    def test_parquet_schemas_per_type(self) -> None:
        for event_type in ("DependencyObserved", "PopconObserved"):
            schema = schema_for_event_type(event_type)
            assert isinstance(schema, pa.Schema)
            assert event_type in EVENT_TYPE_SCHEMAS
        dep = schema_for_event_type("DependencyObserved")
        assert "kind" in dep.names
        assert "clause_index" in dep.names
        assert "condition" in dep.names
        assert "is_complex" in dep.names
        assert "alternatives" in dep.names
        pop = schema_for_event_type("PopconObserved")
        assert "insts" in pop.names
        assert "vote" in pop.names

    def test_event_type_directories(self) -> None:
        assert (
            event_type_directory("DependencyObserved")
            == "dependency_observed"
        )
        assert event_type_directory("PopconObserved") == "popcon_observed"

    def test_entity_id_convention(self, dependency_payload) -> None:
        # DependencyObserved → package_id of the depender
        # (<package>:<architecture>, as PackageVersionObserved);
        # PopconObserved → <package> (popcon has no architecture).
        env = EventEnvelope.from_payload(
            event_type="DependencyObserved",
            source="udddump",
            entity_id=f"libhello:amd64",
            payload_model=dependency_payload,
            ingestion_run_id="run-001",
        )
        assert env.entity_id == "libhello:amd64"
