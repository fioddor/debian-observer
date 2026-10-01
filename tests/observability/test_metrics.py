"""Tests for observability/metrics module."""

from __future__ import annotations

import pytest
from opentelemetry import metrics

from debian_observer.observability.metrics import (
    _init_provider,
    get_meter,
    shutdown_meter,
    IngestionMetrics,
    MessagingMetrics,
    APIMetrics,
)


@pytest.fixture(autouse=True)
def _reset_provider():
    """Ensure a clean provider for every test."""
    shutdown_meter()
    yield
    shutdown_meter()


class TestInitProvider:
    def test_idempotent(self):
        p1 = _init_provider()
        p2 = _init_provider()
        assert p1 is p2

    def test_sets_global_meter_provider(self):
        _init_provider()
        assert metrics.get_meter_provider() is not None


class TestGetMeter:
    def test_returns_meter(self):
        meter = get_meter("test_meter")
        assert isinstance(meter, metrics.Meter)


class TestIngestionMetrics:
    def test_all_methods_callable(self):
        im = IngestionMetrics()
        im.record_duration(1.23, dataset="test")
        im.add_records(100, dataset="test")
        im.add_bytes(1024, dataset="test")
        im.add_error(dataset="test")

    def test_accepts_custom_meter(self):
        meter = get_meter("custom")
        im = IngestionMetrics(meter)
        im.add_records(1)


class TestMessagingMetrics:
    def test_all_methods_callable(self):
        mm = MessagingMetrics()
        mm.add_published(5, subject="events.ingest")
        mm.add_consumed(5, subject="events.ingest")
        mm.add_failed(1, subject="events.ingest")
        mm.record_consumer_duration(0.45, subject="events.ingest")
        mm.set_lag(3, consumer="worker-1")

    def test_accepts_custom_meter(self):
        meter = get_meter("custom")
        mm = MessagingMetrics(meter)
        mm.add_published(1)


class TestAPIMetrics:
    def test_record_duration(self):
        am = APIMetrics()
        am.record_duration(0.12, endpoint="/health")

    def test_accepts_custom_meter(self):
        meter = get_meter("custom")
        am = APIMetrics(meter)
        am.record_duration(0.12)


class TestShutdownMeter:
    def test_cleans_up(self):
        _init_provider()
        shutdown_meter()
        shutdown_meter()
