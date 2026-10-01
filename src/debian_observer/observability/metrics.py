"""Observability: OpenTelemetry metrics.

Configures a ``MeterProvider`` with a ``ConsoleMetricExporter`` for R1.
Provides metric helpers for ingestion, messaging, and API operations.

Upgrade path:
- R2: add OTLPMetricExporter + Prometheus pull endpoint
- R3: Grafana dashboards + alerting rules
"""

from __future__ import annotations

import logging
from typing import Any

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global provider setup
# ---------------------------------------------------------------------------

_Provider: MeterProvider | None = None


def _init_provider() -> MeterProvider:
    """Idempotently initialise the global MeterProvider."""
    global _Provider
    if _Provider is not None:
        return _Provider

    exporter = ConsoleMetricExporter()
    reader = PeriodicExportingMetricReader(exporter, export_interval_millis=5_000)
    provider = MeterProvider(metric_readers=[reader])
    metrics.set_meter_provider(provider)
    _Provider = provider
    logger.debug("MeterProvider initialised with ConsoleMetricExporter")
    return provider


def get_meter(name: str = "debian_observer") -> metrics.Meter:
    """Return a named meter, ensuring the provider is alive."""
    _init_provider()
    return metrics.get_meter(name)


def shutdown_meter() -> None:
    """Flush and shut down the global provider.

    Call this on application exit to avoid dropped metrics.
    """
    global _Provider
    if _Provider is not None:
        _Provider.shutdown()
        _Provider = None
        logger.debug("MeterProvider shut down")


# ---------------------------------------------------------------------------
# Metric helpers
# ---------------------------------------------------------------------------

class IngestionMetrics:
    """Counters and histograms for the ingestion pipeline."""

    def __init__(self, meter: metrics.Meter | None = None) -> None:
        m = meter or get_meter("debian_observer.ingestion")
        self._duration = m.create_histogram(
            name="ingestion.duration_seconds",
            description="Time spent in ingestion pipeline",
            unit="s",
        )
        self._records = m.create_counter(
            name="ingestion.records_total",
            description="Total records ingested",
            unit="1",
        )
        self._bytes = m.create_counter(
            name="ingestion.bytes_total",
            description="Total bytes ingested",
            unit="By",
        )
        self._errors = m.create_counter(
            name="ingestion.errors_total",
            description="Total ingestion failures",
            unit="1",
        )

    def record_duration(self, seconds: float, **attrs: Any) -> None:
        self._duration.record(seconds, attrs)

    def add_records(self, count: int, **attrs: Any) -> None:
        self._records.add(count, attrs)

    def add_bytes(self, size: int, **attrs: Any) -> None:
        self._bytes.add(size, attrs)

    def add_error(self, **attrs: Any) -> None:
        self._errors.add(1, attrs)


class MessagingMetrics:
    """Counters and histograms for NATS messaging."""

    def __init__(self, meter: metrics.Meter | None = None) -> None:
        m = meter or get_meter("debian_observer.messaging")
        self._published = m.create_counter(
            name="messaging.events_published_total",
            description="Total events published",
            unit="1",
        )
        self._consumed = m.create_counter(
            name="messaging.events_consumed_total",
            description="Total events consumed",
            unit="1",
        )
        self._failed = m.create_counter(
            name="messaging.events_failed_total",
            description="Total message processing failures",
            unit="1",
        )
        self._consumer_duration = m.create_histogram(
            name="messaging.consumer_duration_seconds",
            description="Time spent processing a consumed event",
            unit="s",
        )
        self._lag = m.create_up_down_counter(
            name="messaging.consumer_lag",
            description="Approximate consumer lag (messages)",
            unit="1",
        )

    def add_published(self, count: int = 1, **attrs: Any) -> None:
        self._published.add(count, attrs)

    def add_consumed(self, count: int = 1, **attrs: Any) -> None:
        self._consumed.add(count, attrs)

    def add_failed(self, count: int = 1, **attrs: Any) -> None:
        self._failed.add(count, attrs)

    def record_consumer_duration(self, seconds: float, **attrs: Any) -> None:
        self._consumer_duration.record(seconds, attrs)

    def set_lag(self, delta: int, **attrs: Any) -> None:
        self._lag.add(delta, attrs)


class APIMetrics:
    """Histograms for API query latency."""

    def __init__(self, meter: metrics.Meter | None = None) -> None:
        m = meter or get_meter("debian_observer.api")
        self._duration = m.create_histogram(
            name="api.query_duration_seconds",
            description="API endpoint latency",
            unit="s",
        )

    def record_duration(self, seconds: float, **attrs: Any) -> None:
        self._duration.record(seconds, attrs)
