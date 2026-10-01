"""Ingestion layer for Debian Observatory.

Retrieves or receives source data, validates acquisition,
records provenance, stores raw artifacts, creates dataset manifests,
and triggers downstream processing.

This module provides:

- DataSource abstractions for acquiring raw Debian data
- AcquisitionService for orchestrating retrieval and validation
- ManifestBuilder for constructing reproducible dataset manifests
- ProvenanceRecorder for tracking raw artifact lineage
- IngestionPipeline for end-to-end ingestion workflows

Usage::

    from debian_observer.ingestion import IngestionPipeline, MockDebianDataSource

    source = MockDebianDataSource(package_count=100)
    pipeline = IngestionPipeline(raw_dir=Path("data/raw"))
    result = pipeline.run(source, ingestion_run_id="run-001")
    print(f"Ingested {result.record_count} records into {result.artifact_path}")
"""

from __future__ import annotations

from debian_observer.ingestion.acquisition import (
    AcquisitionService,
    DataSource,
    InMemoryDataSource,
    MockDebianDataSource,
)
from debian_observer.ingestion.manifest import DatasetManifest, ManifestBuilder
from debian_observer.ingestion.pipeline import IngestionPipeline, IngestionResult
from debian_observer.ingestion.provenance import ProvenanceRecorder, RawArtifact

__all__ = [
    "AcquisitionService",
    "DataSource",
    "DatasetManifest",
    "InMemoryDataSource",
    "IngestionPipeline",
    "IngestionResult",
    "ManifestBuilder",
    "MockDebianDataSource",
    "ProvenanceRecorder",
    "RawArtifact",
]
