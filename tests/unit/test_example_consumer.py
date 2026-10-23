"""Smoke tests for examples/consume_packages.py — the external recipe.

The example is NOT part of the installed package: it is a recipe the
researcher copies and runs as a process (the CLI has no ``consume``
command). These tests pin that the recipe exists, imports, and that
its handler → merge.feed → durable write flow works — the flow the
crash test exercises for real against NATS.
"""

from __future__ import annotations

import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = REPO_ROOT / "examples" / "consume_packages.py"

NOW = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc)


def _load_example(dataset_dir: Path):
    """Import the example with a dataset dir under tmp_path (the
    DATASET_DIR constant is read from the environment at import)."""
    import os

    os.environ["DEBIAN_OBSERVER_DATASET_DIR"] = str(dataset_dir)
    spec = importlib.util.spec_from_file_location(
        f"consume_packages_{id(dataset_dir)}", EXAMPLE
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _pvo_envelope() -> "object":
    from debian_observer.events import EventEnvelope

    return EventEnvelope(
        event_id=uuid4(),
        event_type="PackageVersionObserved",
        source="udd",
        entity_id="vim:amd64",
        payload={
            "package_id": "vim:amd64",
            "source_package": "vim",
            "version": "2:9.0-1",
            "distribution": "debian",
            "release": "sid",
            "suite": None,
            "architecture": "amd64",
            "component": "main",
            "section": "editors",
            "priority": "optional",
        },
        observed_at=NOW,
        effective_at=NOW,
    )


def test_example_file_exists() -> None:
    """The recipe ships in the repo (not installed — a copy target)."""
    assert EXAMPLE.is_file()
    assert EXAMPLE.read_text(encoding="utf-8").startswith("#!/usr/bin/env python3")


def test_example_registers_its_handler(tmp_path: Path) -> None:
    module = _load_example(tmp_path / "dataset")

    assert "PackageVersionObserved" in module.app.handlers
    assert module.app.handlers["PackageVersionObserved"] is module.handle


def test_handler_feeds_and_writes_the_durable_table(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    module = _load_example(dataset_dir)

    module.handle(_pvo_envelope())

    out = dataset_dir / "package_versions.parquet"
    assert out.exists()
    rows = pq.read_table(out).to_pylist()
    assert len(rows) == 1
    assert rows[0]["package_id"] == "vim:amd64"
    # Atomic rewrite: no tmp residue.
    assert [p.name for p in dataset_dir.iterdir()] == ["package_versions.parquet"]


def test_handler_via_consumer_app_process(tmp_path: Path) -> None:
    """The ack flow at unit level: ConsumerApp.process → handler →
    durable write → the next duplicate process is absorbed (one row,
    not two)."""
    dataset_dir = tmp_path / "dataset"
    module = _load_example(dataset_dir)

    envelope = _pvo_envelope()
    assert module.app.process(envelope) is True
    # Redelivery of the same event: idempotency absorbs it.
    assert module.app.process(envelope) is True

    rows = pq.read_table(
        dataset_dir / "package_versions.parquet"
    ).to_pylist()
    assert len(rows) == 1