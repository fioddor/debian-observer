"""Tests for docs/CONTRIBUTING.md — contribution rules as docs.

Pins wrap-tolerantly: line wrapping must not break a rule.
"""

from __future__ import annotations

from pathlib import Path


CONTRIBUTING = Path(__file__).parent.parent.parent / "docs" / "CONTRIBUTING.md"


def _normalized(text: str) -> str:
    return " ".join(text.split())




class TestCIRule:
    """CI runs the suite on Woodpecker; every tip must be buildable."""

    def test_ci_section_documents_the_pipeline(self) -> None:
        content = CONTRIBUTING.read_text(encoding="utf-8")
        flat = _normalized(content)
        assert "# CI (Woodpecker)" in content
        assert ".woodpecker/test.yaml" in flat
        assert "python:3.13-slim" in flat
        # coverage threshold comes from pytest.ini, not the pipeline
        assert "--cov-fail-under=80" in flat

    def test_ci_section_documents_local_pipeline_lint(self) -> None:
        flat = _normalized(CONTRIBUTING.read_text(encoding="utf-8"))
        assert "woodpecker-cli lint" in flat
        assert "WOODPECKER_CLI_VERSION" in flat

    def test_ci_keeps_the_buildable_state_mapping(self) -> None:
        flat = _normalized(CONTRIBUTING.read_text(encoding="utf-8"))
        # R-commits are red by design: CI does not gate them
        assert "red by design" in flat
