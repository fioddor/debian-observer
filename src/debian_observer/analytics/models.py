"""Analytics models."""
from __future__ import annotations
from datetime import datetime
from typing import Literal
from pydantic import BaseModel, Field

ExecutionType = Literal["ingestion", "query"]

class ExecutionRecord(BaseModel):
    run_id: str
    run_at: datetime
    execution_type: ExecutionType
    source_name: str
    records_requested: int | None = None
    records_processed: int | None = None
    events_produced: int | None = None
    duration_seconds: float | None = None
    throughput_rps: float | None = None
    raw_bytes: int | None = None
    parquet_bytes: int | None = None
    raw_path: str | None = None
    parquet_path: str | None = None
    checksum: str | None = None
    error_count: int = 0
    error_message: str | None = None
    host_info: str | None = None
    metadata: str | None = None

    def to_dict(self):
        raise NotImplementedError
