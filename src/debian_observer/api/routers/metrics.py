"""API router: Metrics.

Exposes a simple text-based metrics endpoint for R1.
In R2/R3 this will be replaced by a Prometheus-compatible endpoint.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter

from debian_observer.observability.metrics import get_meter

router = APIRouter()


@router.get("/", response_model=dict[str, Any])
async def metrics_endpoint() -> dict[str, Any]:
    """Return a snapshot of registered metric instruments.

    This is a lightweight diagnostic endpoint for R1.
    Prometheus exposition format is deferred to R2.

    Returns:
        A dict with metric names and a system timestamp.
    """
    meter = get_meter("debian_observer.api")
    return {
        "service": "debian-observer",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "note": "ConsoleMetricExporter is active; metrics are also written to stderr",
        "instruments": [
            "ingestion.duration_seconds",
            "ingestion.records_total",
            "ingestion.bytes_total",
            "ingestion.errors_total",
            "messaging.events_published_total",
            "messaging.events_consumed_total",
            "messaging.events_failed_total",
            "messaging.consumer_duration_seconds",
            "messaging.consumer_lag",
            "api.query_duration_seconds",
        ],
    }
