"""Shared pytest fixtures for the Debian Observatory test suite."""

from datetime import datetime, timezone
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Domain model fixtures (TDD — will fail until models are implemented)
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_package_id() -> str:
    """Return a canonical package identifier."""
    return "package:libc6"


@pytest.fixture
def sample_package_data() -> dict[str, Any]:
    """Return valid kwargs for creating a Package instance."""
    return {
        "package_id": "package:libc6",
        "name": "libc6",
        "source_package": "glibc",
        "section": "libs",
        "architecture": "amd64",
    }


@pytest.fixture
def sample_package_version_data() -> dict[str, Any]:
    """Return valid kwargs for creating a PackageVersion instance."""
    return {
        "package_id": "package:libc6",
        "version": "2.37-12",
        "distribution": "debian",
        "release": "trixie",
        "architecture": "amd64",
        "observed_at": datetime(2025, 12, 31, 0, 0, 0, tzinfo=timezone.utc),
        "source": "udd",
    }


@pytest.fixture
def sample_release_data() -> dict[str, Any]:
    """Return valid kwargs for creating a Release instance."""
    return {
        "release_id": "release:trixie",
        "name": "Debian 13",
        "codename": "trixie",
        "status": "testing",
    }


# ---------------------------------------------------------------------------
# Event fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_event_id() -> str:
    """Return a deterministic event UUID."""
    return "evt-550e8400-e29b-41d4-a716-446655440000"


@pytest.fixture
def sample_event_payload() -> dict[str, Any]:
    """Return a minimal valid event payload."""
    return {
        "name": "libc6",
        "version": "2.37-12",
        "distribution": "debian",
        "release": "trixie",
    }


@pytest.fixture
def sample_event_envelope_data(
    sample_event_id: str,
    sample_event_payload: dict[str, Any],
) -> dict[str, Any]:
    """Return valid kwargs for creating an EventEnvelope instance."""
    return {
        "event_id": sample_event_id,
        "event_type": "PackageVersionObserved",
        "schema_version": 1,
        "source": "udd",
        "observed_at": datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        "effective_at": datetime(2025, 12, 31, 0, 0, 0, tzinfo=timezone.utc),
        "entity_id": "package:libc6",
        "payload": sample_event_payload,
    }


@pytest.fixture
def sample_idempotency_key() -> str:
    """Return a deterministic idempotency key."""
    return "idmp-550e8400-e29b-41d4-a716-446655440000"


# ---------------------------------------------------------------------------
# Async / I/O fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def event_loop():
    """Provide a fresh event loop for async tests."""
    import asyncio

    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# ---------------------------------------------------------------------------
# Invalid / boundary data fixtures (for validation tests)
# ---------------------------------------------------------------------------


@pytest.fixture
def invalid_package_name_too_long() -> dict[str, Any]:
    """Package name exceeding reasonable length limit."""
    return {
        "package_id": "package:x" * 200,
        "name": "x" * 300,
        "source_package": "glibc",
        "section": "libs",
        "architecture": "amd64",
    }


@pytest.fixture
def invalid_empty_package_name() -> dict[str, Any]:
    """Package with empty required name field."""
    return {
        "package_id": "package:",
        "name": "",
        "source_package": "glibc",
        "section": "libs",
        "architecture": "amd64",
    }


@pytest.fixture
def invalid_version_empty() -> dict[str, Any]:
    """PackageVersion with empty version string."""
    return {
        "package_id": "package:libc6",
        "version": "",
        "distribution": "debian",
        "release": "trixie",
        "architecture": "amd64",
        "observed_at": datetime(2025, 12, 31, 0, 0, 0, tzinfo=timezone.utc),
        "source": "udd",
    }


@pytest.fixture
def invalid_release_status() -> dict[str, Any]:
    """Release with an unrecognised status value."""
    return {
        "release_id": "release:trixie",
        "name": "Debian 13",
        "codename": "trixie",
        "status": "frozen-banana",
    }


@pytest.fixture
def malformed_event_missing_type() -> dict[str, Any]:
    """Event envelope missing the mandatory event_type field."""
    return {
        "event_id": "evt-550e8400-e29b-41d4-a716-446655440000",
        "schema_version": 1,
        "source": "udd",
        "observed_at": "2026-01-01T12:00:00Z",
        "effective_at": "2025-12-31T00:00:00Z",
        "entity_id": "package:libc6",
        "payload": {},
    }
