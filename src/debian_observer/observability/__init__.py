"""Observability: OpenTelemetry instrumentation for Debian Observer.

Provides unified tracing, metrics, and structured-logging entry points.
All components use a lightweight Console exporter suitable for R1;
upgrade to OTLP/Prometheus/Grafana is deferred to R2/R3.

Usage::

    from debian_observer.observability import get_tracer, get_meter, span
    from debian_observer.observability.metrics import IngestionMetrics
"""

from __future__ import annotations

from debian_observer.observability.metrics import (
    IngestionMetrics,
    MessagingMetrics,
    get_meter,
)
from debian_observer.observability.tracing import get_tracer, span

__all__ = [
    "get_tracer",
    "span",
    "get_meter",
    "IngestionMetrics",
    "MessagingMetrics",
]
