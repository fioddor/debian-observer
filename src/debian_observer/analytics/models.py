"""Analytics models.

ExecutionRecord captures the metadata of a single pipeline or query run
for later reporting and trend analysis.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

ExecutionType = Literal["ingestion", "query"]


class ExecutionRecord(BaseModel):
    """A single execution run with performance and provenance metadata.

    Fields map 1:1 to the ``execution_runs`` DuckDB table created by
    :class:`~debian_observer.analytics.store.ExecutionStore`.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,
        str_strip_whitespace=True,
    )

    run_id: str = Field(..., description="Unique identifier for this execution run")
    run_at: datetime = Field(
        ..., description="When the run completed (UTC)"
    )
    execution_type: ExecutionType = Field(
        ..., description="Category: ingestion, normalization, replay-stream, query, etc."
    )
    source_name: str = Field(
        ..., description="Data source: udd, mirror, benchmark, snapshot-YYYYMMDD, etc."
    )
    records_requested: int | None = Field(
        default=None,
        description="Records the caller asked to process (NULL if not applicable)",
    )
    records_processed: int | None = Field(
        default=None, description="Records actually processed"
    )
    events_produced: int | None = Field(
        default=None, description="Domain events generated"
    )
    duration_seconds: float | None = Field(
        default=None,
        description="Wall-clock time from start to completion",
    )
    throughput_rps: float | None = Field(
        default=None,
        description="records_processed / duration_seconds (NULL if duration <= 0)",
    )
    raw_bytes: int | None = Field(
        default=None, description="Size of the raw artifact in bytes"
    )
    parquet_bytes: int | None = Field(
        default=None, description="Size of the Parquet output in bytes"
    )
    raw_path: str | None = Field(
        default=None, description="Filesystem path to the raw artifact"
    )
    parquet_path: str | None = Field(
        default=None, description="Filesystem path to the Parquet output"
    )
    checksum: str | None = Field(
        default=None, description="SHA-256 of the raw artifact"
    )
    error_count: int = Field(
        default=0, description="Number of errors captured during execution"
    )
    error_message: str | None = Field(
        default=None,
        description="First error message if error_count > 0",
    )
    host_info: str | None = Field(
        default=None,
        description="JSON: hostname, cpu_count, mem_gb, python_version",
    )
    metadata: str | None = Field(
        default=None,
        description="JSON: type-specific fields, freely extensible",
    )

    def to_dict(self) -> dict[str, object]:
        """Serialize to a plain dict with ISO-8601 timestamps."""
        d = self.model_dump()
        if isinstance(d.get("run_at"), datetime):
            d["run_at"] = d["run_at"].isoformat()
        return d
