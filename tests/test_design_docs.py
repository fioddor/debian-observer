"""High-level design documentation tests (``docs/design`` layer).

Pins the structure of the design layer that sits *between* the
architecture overview (``docs/architecture/``) and the code:

- ``docs/design/data-model.md`` — entity intents and invariants
- ``docs/design/module-contracts.md`` — what each module promises
- ``docs/design/key-flows.md`` — end-to-end flows across modules

The design layer documents only what neither the architecture nor the
code reveals (intent, contracts, cross-module behaviour).
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DESIGN = REPO_ROOT / "docs" / "design"
SRC = REPO_ROOT / "src" / "debian_observer"


def _top_level_modules() -> set[str]:
    """Every top-level package or module under ``src/debian_observer``."""
    names: set[str] = set()
    for entry in SRC.iterdir():
        if entry.name == "__pycache__" or entry.name == "__init__.py":
            continue
        if entry.is_dir():
            names.add(entry.name)
        elif entry.suffix == ".py":
            names.add(entry.stem)
    return names


def _headings(text: str, level: int = 2) -> list[str]:
    pattern = rf"^{'#' * level}\s+(.+)$"
    return [m.strip() for m in re.findall(pattern, text, re.MULTILINE)]


def test_data_model_docs_exist_with_minimum_structure():
    path = DESIGN / "data-model.md"
    text = path.read_text(encoding="utf-8")
    headings = _headings(text)

    assert "Entities" in headings, "data-model.md needs an '## Entities' section"
    assert "Invariants" in headings, "data-model.md needs an '## Invariants' section"

    entities_table = text.split("## Entities", 1)[1].split("## ", 1)[0]
    for entity in ("Package", "PackageVersion", "Release", "Migration"):
        assert re.search(rf"\b{entity}\b", entities_table), (
            f"data-model.md entity table must mention {entity}"
        )


def test_module_contracts_cover_every_top_level_module():
    path = DESIGN / "module-contracts.md"
    text = path.read_text(encoding="utf-8")

    documented = set()
    for heading in _headings(text):
        match = re.match(r"`(\w+)`", heading)
        if match:
            documented.add(match.group(1))

    missing = _top_level_modules() - documented
    assert not missing, (
        f"module-contracts.md is missing sections for: {sorted(missing)}"
    )


def test_key_flows_document_two_to_four_flows_with_steps():
    path = DESIGN / "key-flows.md"
    text = path.read_text(encoding="utf-8")

    flow_headings = [h for h in _headings(text) if h.startswith("Flow ")]
    assert 2 <= len(flow_headings) <= 4, (
        "key-flows.md must document between 2 and 4 flows"
    )

    sections = text.split("## Flow ")[1:]
    for section in sections:
        section_body = section.split("## ", 1)[0]
        steps = re.findall(r"^\d+\.\s", section_body, re.MULTILINE)
        assert len(steps) >= 3, (
            "each flow needs at least 3 numbered steps"
        )


def test_design_readme_states_dry_rules():
    path = DESIGN / "README.md"
    text = path.read_text(encoding="utf-8")

    assert "Single source of truth" in text
    assert "same commit" in text


def test_repo_readme_links_the_design_layer():
    text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    for doc in ("README.md", "data-model.md", "module-contracts.md", "key-flows.md"):
        assert f"docs/design/{doc}" in text, (
            f"README.md must link docs/design/{doc}"
        )
