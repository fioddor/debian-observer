"""Unit tests for Debian Observatory API service.

Tests verify that the FastAPI endpoints correctly:
- Return health status
- Serve package metadata and versions
- List releases and migrations
- Return dataset manifests
- Look up events by ID

All tests use an in-memory store and FastAPI TestClient.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from debian_observer.api.app import create_app
from debian_observer.api.store import InMemoryStore
from debian_observer.domain.models import Migration, Package, PackageVersion, Release
from debian_observer.events import EventEnvelope
from debian_observer.events.types import (
    ObservationMeta,
    PackageObservedPayload,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def test_store() -> InMemoryStore:
    """Return a populated in-memory store for testing."""
    store = InMemoryStore()

    # Add packages
    store.add_package(
        Package(
            package_id="package:libc6:amd64",
            name="libc6",
            source_package="glibc",
            section="libs",
            architecture="amd64",
        )
    )
    store.add_package(
        Package(
            package_id="package:python3:amd64",
            name="python3",
            source_package="python3-defaults",
            section="python",
            architecture="amd64",
        )
    )

    # Add package versions
    store.add_package_version(
        PackageVersion(
            package_id="package:libc6:amd64",
            version="2.37-12",
            distribution="trixie",
            architecture="amd64",
            component="main",
            priority="required",
            source="udd",
        )
    )
    store.add_package_version(
        PackageVersion(
            package_id="package:libc6:amd64",
            version="2.37-13",
            distribution="sid",
            architecture="amd64",
            component="main",
            priority="required",
            source="udd",
        )
    )
    store.add_package_version(
        PackageVersion(
            package_id="package:python3:amd64",
            version="3.11.2-1",
            distribution="trixie",
            architecture="amd64",
            component="main",
            priority="important",
            source="udd",
        )
    )

    # Add releases
    store.add_release(
        Release(
            release_id="release:trixie",
            name="Debian 13",
            codename="trixie",
            status="testing",
        )
    )
    store.add_release(
        Release(
            release_id="release:bookworm",
            name="Debian 12",
            codename="bookworm",
            status="stable",
        )
    )

    # Add migrations
    store.add_migration(
        Migration(
            migration_id="mig:1",
            source="testing",
            target="stable",
            package="libc6",
            version="2.37-12",
            architecture="amd64",
            status="completed",
        )
    )

    # Add an event
    obs = ObservationMeta(source="udd")
    payload = PackageObservedPayload(
        package_id="package:libc6:amd64",
        name="libc6",
        source_package="glibc",
        architecture="amd64",
        observation=obs,
    )
    event = EventEnvelope.from_payload(
        event_type="PackageObserved",
        source="udd",
        entity_id="package:libc6:amd64",
        payload_model=payload,
    )
    store.add_event(event)

    # Add dataset manifest
    store.add_dataset(
        "udd-snapshot",
        {
            "dataset_name": "udd-snapshot",
            "source": "udd",
            "retrieved_at": "2026-01-01T12:00:00Z",
            "checksum": "abc123",
            "record_count": 100,
        },
    )

    return store


@pytest.fixture
def test_client(test_store: InMemoryStore) -> TestClient:
    """Return a configured TestClient with the in-memory store."""
    app = create_app(store=test_store)
    return TestClient(app)


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------


class TestHealth:
    """Tests for the health endpoint."""

    def test_health_returns_200(self, test_client: TestClient) -> None:
        response = test_client.get("/health")
        assert response.status_code == 200

    def test_health_returns_status(self, test_client: TestClient) -> None:
        response = test_client.get("/health")
        data = response.json()
        assert data["status"] == "ok"
        assert "timestamp" in data


# ---------------------------------------------------------------------------
# Packages endpoint
# ---------------------------------------------------------------------------


class TestPackages:
    """Tests for package endpoints."""

    def test_get_package_exists(self, test_client: TestClient) -> None:
        response = test_client.get("/packages/libc6")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "libc6"
        assert data["source_package"] == "glibc"
        assert data["section"] == "libs"

    def test_get_package_not_found(self, test_client: TestClient) -> None:
        response = test_client.get("/packages/nonexistent")
        assert response.status_code == 404

    def test_get_package_versions(self, test_client: TestClient) -> None:
        response = test_client.get("/packages/libc6/versions")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        versions = [v["version"] for v in data]
        assert "2.37-12" in versions
        assert "2.37-13" in versions

    def test_get_package_versions_empty(self, test_client: TestClient) -> None:
        response = test_client.get("/packages/python3/versions")
        # python3 DOES have a version in test_store
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["version"] == "3.11.2-1"

    def test_get_package_versions_not_found(self, test_client: TestClient) -> None:
        response = test_client.get("/packages/nonexistent/versions")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Releases endpoint
# ---------------------------------------------------------------------------


class TestReleases:
    """Tests for releases endpoints."""

    def test_list_releases(self, test_client: TestClient) -> None:
        response = test_client.get("/releases")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        codenames = [r["codename"] for r in data]
        assert "trixie" in codenames
        assert "bookworm" in codenames


# ---------------------------------------------------------------------------
# Migrations endpoint
# ---------------------------------------------------------------------------


class TestMigrations:
    """Tests for migrations endpoints."""

    def test_list_migrations(self, test_client: TestClient) -> None:
        response = test_client.get("/migrations")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["migration_id"] == "mig:1"
        assert data[0]["package"] == "libc6"


# ---------------------------------------------------------------------------
# Datasets endpoint
# ---------------------------------------------------------------------------


class TestDatasets:
    """Tests for dataset endpoints."""

    def test_get_dataset_exists(self, test_client: TestClient) -> None:
        response = test_client.get("/datasets/udd-snapshot")
        assert response.status_code == 200
        data = response.json()
        assert data["dataset_name"] == "udd-snapshot"
        assert data["source"] == "udd"
        assert data["record_count"] == 100

    def test_get_dataset_not_found(self, test_client: TestClient) -> None:
        response = test_client.get("/datasets/nonexistent")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Events endpoint
# ---------------------------------------------------------------------------


@pytest.fixture
def _test_store_with_event() -> InMemoryStore:
    """Return a store with one event for event endpoint tests."""
    store = InMemoryStore()
    obs = ObservationMeta(source="udd")
    payload = PackageObservedPayload(
        package_id="package:libc6:amd64",
        name="libc6",
        source_package="glibc",
        architecture="amd64",
        observation=obs,
    )
    event = EventEnvelope.from_payload(
        event_type="PackageObserved",
        source="udd",
        entity_id="package:libc6:amd64",
        payload_model=payload,
    )
    store.add_event(event)
    return store


@pytest.fixture
def test_client_for_events(_test_store_with_event: InMemoryStore) -> TestClient:
    """Return a TestClient with a store that has one event."""
    app = create_app(store=_test_store_with_event)
    return TestClient(app)


class TestEvents:
    """Tests for event endpoints."""

    def test_get_event_by_id(
        self, test_client_for_events: TestClient, _test_store_with_event: InMemoryStore
    ) -> None:
        event_id = list(_test_store_with_event.events.keys())[0]
        response = test_client_for_events.get(f"/events/{event_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["event_type"] == "PackageObserved"
        assert data["entity_id"] == "package:libc6:amd64"
        assert data["source"] == "udd"

    def test_get_event_not_found(self, test_client_for_events: TestClient) -> None:
        response = test_client_for_events.get("/events/nonexistent-id")
        assert response.status_code == 404


class TestEventLoaderExtendedFields:
    """EventLoader restores the full fact fields from Parquet."""

    def test_load_new_schema_restores_extended_fields(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq

        from debian_observer.api.loader import EventLoader
        from debian_observer.api.store import InMemoryStore
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()
        columns = [
            "event_id", "event_type", "source", "entity_id", "observed_at",
            "effective_at", "ingestion_run_id", "package_id", "source_package",
            "version", "distribution", "release", "architecture", "component",
            "section", "priority",
        ]
        row = (
            "550e8400-e29b-41d4-a716-446655440000", "PackageVersionObserved", "udd", "librte-dpdk:amd64",
            now, now, "run-001", "librte-dpdk:amd64", "dpdk", "25.11.3-1",
            "debian", "bullseye-security", "amd64", "main", "libs", "optional",
        )
        table = pa.Table.from_pylist(
            [dict(zip(columns, row))],
            schema=pa.schema([(c, pa.string()) for c in columns]),
        )
        pq.write_table(table, tmp_path / "run-001.parquet")

        store = InMemoryStore()
        loader = EventLoader(store)
        count = loader.load_from_parquet(tmp_path)
        assert count == 1
        versions = store.list_package_versions()
        assert len(versions) == 1
        assert versions[0].source_package == "dpdk"
        assert versions[0].release == "bullseye-security"
        assert versions[0].section == "libs"
        assert versions[0].priority == "optional"

    def test_load_legacy_schema_maps_old_columns(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq

        from debian_observer.api.loader import EventLoader
        from debian_observer.api.store import InMemoryStore
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()
        columns = [
            "event_id", "event_type", "source", "entity_id", "observed_at",
            "effective_at", "ingestion_run_id", "package_id", "version",
            "distribution", "architecture", "component", "priority",
        ]
        row = (
            "550e8400-e29b-41d4-a716-446655440001", "PackageVersionObserved", "udd", "vim:amd64",
            now, now, "run-002", "vim:amd64", "2:9.0-1", "debian",
            "amd64", "main", "optional",
        )
        table = pa.Table.from_pylist(
            [dict(zip(columns, row))],
            schema=pa.schema([(c, pa.string()) for c in columns]),
        )
        pq.write_table(table, tmp_path / "run-002.parquet")

        store = InMemoryStore()
        loader = EventLoader(store)
        count = loader.load_from_parquet(tmp_path)
        assert count == 1
        version = store.list_package_versions()[0]
        assert version.version == "2:9.0-1"
        assert version.release is None
        assert version.section is None
        assert version.source_package is None


class TestEventLoaderSuiteField:
    """EventLoader restores the suite role; legacy files load suite None."""

    def test_load_new_schema_restores_suite(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq

        from debian_observer.api.loader import EventLoader
        from debian_observer.api.store import InMemoryStore
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()
        columns = [
            "event_id", "event_type", "source", "entity_id", "observed_at",
            "effective_at", "ingestion_run_id", "package_id", "source_package",
            "version", "distribution", "release", "architecture", "component",
            "section", "priority", "suite",
        ]
        row = (
            "550e8400-e29b-41d4-a716-446655440002", "PackageVersionObserved",
            "debian.mirror", "vim:amd64",
            now, now, "run-003", "vim:amd64", "vim", "2:9.0-1",
            "debian", "bookworm", "amd64", "main", "editors", "optional",
            "stable",
        )
        table = pa.Table.from_pylist(
            [dict(zip(columns, row))],
            schema=pa.schema([(c, pa.string()) for c in columns]),
        )
        pq.write_table(table, tmp_path / "run-003.parquet")

        store = InMemoryStore()
        loader = EventLoader(store)
        count = loader.load_from_parquet(tmp_path)
        assert count == 1
        version = store.list_package_versions()[0]
        assert version.suite == "stable"
        assert version.release == "bookworm"

    def test_load_legacy_16_schema_suite_defaults_none(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq

        from debian_observer.api.loader import EventLoader
        from debian_observer.api.store import InMemoryStore
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()
        columns = [
            "event_id", "event_type", "source", "entity_id", "observed_at",
            "effective_at", "ingestion_run_id", "package_id", "source_package",
            "version", "distribution", "release", "architecture", "component",
            "section", "priority",
        ]
        row = (
            "550e8400-e29b-41d4-a716-446655440003", "PackageVersionObserved",
            "udd", "librte-dpdk:amd64",
            now, now, "run-004", "librte-dpdk:amd64", "dpdk", "25.11.3-1",
            "debian", "bullseye-security", "amd64", "main", "libs", "optional",
        )
        table = pa.Table.from_pylist(
            [dict(zip(columns, row))],
            schema=pa.schema([(c, pa.string()) for c in columns]),
        )
        pq.write_table(table, tmp_path / "run-004.parquet")

        store = InMemoryStore()
        loader = EventLoader(store)
        count = loader.load_from_parquet(tmp_path)
        assert count == 1
        version = store.list_package_versions()[0]
        assert version.release == "bullseye-security"
        assert version.suite is None
