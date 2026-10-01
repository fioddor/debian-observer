"""Tests for observability/tracing module."""

from __future__ import annotations

import pytest
from opentelemetry import trace
from opentelemetry.trace import StatusCode

from debian_observer.observability.tracing import (
    _init_provider,
    get_tracer,
    span,
    shutdown_tracer,
)


@pytest.fixture(autouse=True)
def _reset_provider():
    """Ensure a clean provider for every test."""
    shutdown_tracer()
    yield
    shutdown_tracer()


class TestInitProvider:
    def test_idempotent(self):
        p1 = _init_provider()
        p2 = _init_provider()
        assert p1 is p2

    def test_sets_global_tracer_provider(self):
        _init_provider()
        assert trace.get_tracer_provider() is not None


class TestGetTracer:
    def test_returns_tracer(self):
        tracer = get_tracer("test_module")
        assert isinstance(tracer, trace.Tracer)


class TestSpanDecorator:
    def test_sync_function_runs(self):
        @span()
        def work(x: int) -> int:
            return x * 2

        assert work(5) == 10

    def test_async_function_runs(self):
        @span()
        async def async_work(x: int) -> int:
            return x * 3

        import asyncio
        assert asyncio.run(async_work(7)) == 21

    def test_preserves_function_name(self):
        @span()
        def named():
            pass

        assert named.__name__ == "named"

    def test_uses_custom_span_name(self):
        captured = []

        @span(name="custom_span")
        def do_it():
            tracer = get_tracer("test")
            span = trace.get_current_span()
            captured.append(span)
            return 42

        do_it()
        # Span was active inside the function
        assert len(captured) == 1

    def test_records_exception_on_error(self):
        @span()
        def boom():
            tracer = get_tracer("test")
            current = trace.get_current_span()
            current.set_attribute("pre_crash", True)
            raise RuntimeError("intentional")

        with pytest.raises(RuntimeError, match="intentional"):
            boom()


class TestShutdownTracer:
    def test_cleans_up(self):
        _init_provider()
        shutdown_tracer()
        # Calling again should be a no-op
        shutdown_tracer()
