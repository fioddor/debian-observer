"""CI pipeline contract: .woodpecker/test.yaml runs the test suite.

Pins the parts that must not drift:
- the pinned image (same Python minor as the devcontainer),
- the test command (pytest with the repository's pytest.ini —
  coverage and the 80% threshold come from there),
- the absence of branch/event filters: every branch tip must be
  buildable (CONTRIBUTING, "Buildable state").
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
PIPELINE = REPO_ROOT / ".woodpecker" / "test.yaml"


class TestPipelineContract:
    def test_pipeline_exists_and_parses(self) -> None:
        assert PIPELINE.is_file(), ".woodpecker/test.yaml is missing"
        doc = yaml.safe_load(PIPELINE.read_text(encoding="utf-8"))
        assert isinstance(doc, dict)

    def test_test_step_pins_python_and_runs_pytest(self) -> None:
        doc = yaml.safe_load(PIPELINE.read_text(encoding="utf-8"))
        steps = doc["steps"]
        assert isinstance(steps, list) and len(steps) == 1
        test = steps[0]
        assert test["name"] == "test"
        # image pinned by tag: same Python minor as the devcontainer
        assert test["image"] == "python:3.13-slim"
        commands = " ".join(test["commands"])
        # the suite runs with the repository's pytest.ini (coverage 80)
        assert 'pip install -e ".[dev]"' in commands
        assert "pytest" in commands

    def test_no_branch_filter_runs_on_every_push(self) -> None:
        doc = yaml.safe_load(PIPELINE.read_text(encoding="utf-8"))
        # tips must be buildable: no branch/event filtering here
        assert "when" not in doc
        for step in doc["steps"]:
            assert "when" not in step