"""Tests for deployment configurations.

Validates deployment artifacts without requiring Docker runtime.
"""

from __future__ import annotations

import json
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
        assert "localhost:8088/health" in " ".join(hc["test"])

    def test_cli_build_context_is_repo_root(self, compose: dict):
        # regression 2026-10-26: the CLI service built with context "."
        # (deploy/compose/) while its Dockerfile expects the repo root
        # -> COPY src/ failed. The api service already had it right.
        cli = compose["services"]["debian-observer"]
        assert cli["build"]["context"] == "../.."
        assert cli["build"]["dockerfile"] == "deploy/compose/Dockerfile"

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
        assert "EXPOSE 8088" in dockerfile

    def test_healthcheck_defined(self, dockerfile: str):
        assert "HEALTHCHECK" in dockerfile
        assert "localhost:8088/health" in dockerfile

    def test_serve_cmd(self, dockerfile: str):
        assert "debian-observer" in dockerfile
        assert "serve" in dockerfile
        assert "--host" in dockerfile
        assert "0.0.0.0" in dockerfile
        assert "--port" in dockerfile

    def test_events_env_handled(self, dockerfile: str):
        assert "EVENTS_DIR" in dockerfile


class TestDeploymentReadme:
    """Validate deployment documentation exists."""

    def test_readme_exists(self):
        readme = Path(__file__).parent.parent / "deploy" / "README.md"
        assert readme.exists()
        content = readme.read_text()
        assert "Docker Compose" in content
        assert "NATS" in content
        assert "Parquet" in content

class TestDevcontainerImageTag:
    """The dev image tag is optional, latest by default.

    The compose service pins the image to this repository's devcontainer
    image and defaults its tag to ``latest``, so the plain
    ``docker compose up -d`` keeps working; whoever wants to keep a
    concrete image passes ``DEVCONTAINER_TAG`` on the lift, with no
    versioned file to edit.
    """

    #: The compose variable that names the image tag.
    TAG_VAR = "DEVCONTAINER_TAG"
    #: The tag compose falls back to when the variable is unset.
    DEFAULT_TAG = "latest"
    #: The repository of the development image.
    IMAGE_REPO = "deb-obs-devcontainer"
    DEVCONTAINER = Path(__file__).parent.parent / ".devcontainer"
    CONTRIBUTING = Path(__file__).parent.parent / "docs" / "CONTRIBUTING.md"

    def test_compose_defaults_the_tag_to_latest(self) -> None:
        """The service image is the devcontainer image, tag optional.

        ``${DEVCONTAINER_TAG:-latest}`` keeps the variable optional: no
        command has to run before compose, and unset means latest.
        """
        compose = yaml.safe_load(
            (self.DEVCONTAINER / "docker-compose.yml").read_text(encoding="utf-8")
        )
        image = compose["services"]["debian-observer-development"]["image"]
        repository, _, tag = image.partition(":")
        assert repository == self.IMAGE_REPO
        assert tag == "${%s:-%s}" % (self.TAG_VAR, self.DEFAULT_TAG)

    def test_devcontainer_starts_compose_without_a_setup_command(self) -> None:
        """No VS Code command writes the tag before compose starts.

        The compose default is what keeps the tag optional: a mandatory
        variable would need a command that writes it before
        ``docker compose up -d``, and the default removes that need.
        """
        config = json.loads(
            (self.DEVCONTAINER / "devcontainer.json").read_text(encoding="utf-8")
        )
        assert "initializeCommand" not in config

    def test_contributing_documents_the_optional_tag(self) -> None:
        """Docker (Compose) documents the optional tagged lift.

        The plain commands stay intact: the optional tag is a paragraph
        beside them, and the destroy step removes the image compose
        builds instead of a name no tool ever produced.
        """
        content = self.CONTRIBUTING.read_text(encoding="utf-8")
        flat = " ".join(content.split())
        assert f"{self.TAG_VAR}=<tag> docker compose up -d" in flat
        assert (
            f"`{self.IMAGE_REPO}:${{{self.TAG_VAR}:-{self.DEFAULT_TAG}}}`" in flat
        )
        assert f"docker rmi {self.IMAGE_REPO}:{self.DEFAULT_TAG}" in flat
        assert "devcontainer-debian-observer-development" not in flat
