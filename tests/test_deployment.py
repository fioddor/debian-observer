"""Tests for deployment configurations.

Validates deployment artifacts without requiring Docker runtime.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml


class TestDockerCompose:
    """Validate docker-compose.yml structure and references."""

    @pytest.fixture
    def compose_path(self) -> Path:
        return (
            Path(__file__).parent.parent / "deploy" / "compose" / "docker-compose.yml"
        )

    @pytest.fixture
    def compose(self, compose_path: Path) -> dict:
        assert compose_path.exists(), f"docker-compose.yml not found at {compose_path}"
        with open(compose_path) as f:
            return yaml.safe_load(f)

    def test_services_defined(self, compose: dict):
        assert "services" in compose
        assert "nats" in compose["services"]
        assert "api" in compose["services"]

    def test_nats_service(self, compose: dict):
        nats = compose["services"]["nats"]
        assert nats["image"] == "nats:2.10-alpine"
        assert "-js" in nats["command"]
        assert "ports" in nats
        assert "volumes" in nats
        assert "healthcheck" in nats

    def test_api_service(self, compose: dict):
        api = compose["services"]["api"]
        assert "build" in api
        assert api["build"]["dockerfile"] == "deploy/compose/Dockerfile"
        assert "depends_on" in api
        assert "nats" in api["depends_on"]
        assert api["depends_on"]["nats"]["condition"] == "service_healthy"
        assert "environment" in api
        env = {e.split("=")[0]: e.split("=")[1] for e in api["environment"]}
        assert "NATS_URL" in env
        assert "OTEL_EXPORTER" in env

    def test_api_healthcheck(self, compose: dict):
        api = compose["services"]["api"]
        assert "healthcheck" in api
        hc = api["healthcheck"]
        assert "test" in hc
        assert "localhost:8000/health" in " ".join(hc["test"])

    def test_volumes_defined(self, compose: dict):
        assert "volumes" in compose
        assert "nats-data" in compose["volumes"]
        assert "api-data" in compose["volumes"]

    def test_network_defined(self, compose: dict):
        assert "networks" in compose
        assert "debian-observer" in compose["networks"]


class TestDockerfile:
    """Validate Dockerfile structure."""

    @pytest.fixture
    def dockerfile_path(self) -> Path:
        return Path(__file__).parent.parent / "deploy" / "compose" / "Dockerfile"

    @pytest.fixture
    def dockerfile(self, dockerfile_path: Path) -> str:
        assert dockerfile_path.exists()
        with open(dockerfile_path) as f:
            return f.read()

    def test_from_python311(self, dockerfile: str):
        assert re.search(r"FROM\s+python:3\.11", dockerfile)

    def test_workdir_app(self, dockerfile: str):
        assert "WORKDIR /app" in dockerfile

    def test_expose_port(self, dockerfile: str):
        assert "EXPOSE 8000" in dockerfile

    def test_healthcheck_defined(self, dockerfile: str):
        assert "HEALTHCHECK" in dockerfile
        assert "localhost:8000/health" in dockerfile

    def test_serve_cmd(self, dockerfile: str):
        assert "debian-observer" in dockerfile
        assert "serve" in dockerfile
        assert "--host" in dockerfile
        assert "0.0.0.0" in dockerfile
        assert "--port" in dockerfile

    def test_parquet_env_handled(self, dockerfile: str):
        assert "PARQUET_DIR" in dockerfile


class TestDeploymentReadme:
    """Validate deployment documentation exists."""

    def test_readme_exists(self):
        readme = Path(__file__).parent.parent / "deploy" / "README.md"
        assert readme.exists()
        content = readme.read_text()
        assert "Docker Compose" in content
        assert "NATS" in content
        assert "Parquet" in content
