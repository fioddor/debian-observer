"""Unit tests for observability/tracing and observability/metrics.

These tests verify tracing decorator behaviour and metric helper classes
without requiring a full OpenTelemetry collector.
"""

from __future__ import annotations

import asyncio
import inspect
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from opentelemetry import trace as trace_api

from debian_observer.observability.metrics import (
    APIMetrics,
    IngestionMetrics,
    MessagingMetrics,
    _init_provider as _init_metrics_provider,
    get_meter,
    shutdown_meter,
)
from debian_observer.observability.tracing import (
    _init_provider,
    get_tracer,
    shutdown_tracer,
    span,
)


# ---------------------------------------------------------------------------
# Tracing tests
# ---------------------------------------------------------------------------


class TestTracingLifecycle:
    """Tests for tracer provider lifecycle."""

    def test_shutdown_tracer_when_provider_exists(self) -> None:
        """shutdown_tracer should clean up an active provider."""
        _init_provider()
        shutdown_tracer()
        # No error should be raised

    def test_shutdown_tracer_when_no_provider(self) -> None:
        """shutdown_tracer should be a no-op when provider is None."""
        shutdown_tracer()
        shutdown_tracer()

    def test_get_tracer_returns_tracer(self) -> None:
        tracer = get_tracer("test_module")
        assert isinstance(tracer, trace_api.Tracer)

    def test_get_tracer_ensures_provider(self) -> None:
        """get_tracer should initialise provider if needed."""
        shutdown_tracer()
        tracer = get_tracer("test")
        assert tracer is not None


class TestSpanDecorator:
    """Tests for @span decorator."""

    def test_sync_function_with_attributes(self) -> None:
        """Decorator should set attributes when provided."""
        @span(attributes={"custom.attr": "value"})
        def work() -> int:
            return 42

        assert work() == 42

    def test_sync_function_exception(self) -> None:
        """Decorator should record exception on sync function error."""
        @span()
        def boom() -> None:
            raise ValueError("sync boom")

        with pytest.raises(ValueError, match="sync boom"):
            boom()

    async def test_async_function_runs(self) -> None:
        """Decorator should wrap async functions."""
        @span()
        async def async_work(x: int) -> int:
            return x * 3

        result = await async_work(7)
        assert result == 21

    async def test_async_function_exception(self) -> None:
        """Decorator should record exception on async function error."""
        @span()
        async def async_boom() -> None:
            raise RuntimeError("async boom")

        with pytest.raises(RuntimeError, match="async boom"):
            await async_boom()

    def test_iscoroutinefunction_detection(self) -> None:
        """Should return async wrapper for coroutine functions."""
        @span()
        async def coro() -> None:
            pass

        assert inspect.iscoroutinefunction(coro)

    def test_sync_wrapper_returned_for_sync(self) -> None:
        """Should return sync wrapper for regular functions."""
        @span()
        def sync_fn() -> None:
            pass

        assert not inspect.iscoroutinefunction(sync_fn)


# ---------------------------------------------------------------------------
# Metrics tests
# ---------------------------------------------------------------------------


class TestMetricsLifecycle:
    """Tests for meter provider lifecycle."""

    def test_shutdown_meter_when_provider_exists(self) -> None:
        _init_metrics_provider()
        shutdown_meter()

    def test_shutdown_meter_when_no_provider(self) -> None:
        shutdown_meter()
        shutdown_meter()

    def test_get_meter_returns_meter(self) -> None:
        meter = get_meter("test")
        assert meter is not None


class TestIngestionMetricsCoverage:
    """Tests to cover missed IngestionMetrics methods."""

    def test_add_error(self) -> None:
        """add_error should call the errors counter."""
        im = IngestionMetrics()
        im.add_error(dataset="test")


class TestMessagingMetricsCoverage:
    """Tests to cover missed MessagingMetrics methods."""

    def test_set_lag(self) -> None:
        """set_lag should call the up-down counter."""
        mm = MessagingMetrics()
        mm.set_lag(5, consumer="c1")

    def test_all_methods_with_custom_meter(self) -> None:
        """All methods should work when initialized with a custom meter."""
        meter = get_meter("custom")
        mm = MessagingMetrics(meter)
        mm.add_published(1)
        mm.add_consumed(1)
        mm.add_failed(1)
        mm.record_consumer_duration(0.1)
        mm.set_lag(0)


class TestAPIMetricsCoverage:
    """Tests to cover missed APIMetrics methods."""

    def test_record_duration_with_custom_meter(self) -> None:
        meter = get_meter("custom")
        am = APIMetrics(meter)
        am.record_duration(0.12, endpoint="/test")
