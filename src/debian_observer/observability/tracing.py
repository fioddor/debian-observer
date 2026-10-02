"""Observability: OpenTelemetry tracing.

Configures a ``TracerProvider`` with a ``ConsoleSpanExporter`` for R1.
Provides a ``@span`` decorator and ``get_tracer()`` helper.

Upgrade path:
- R2: swap ConsoleSpanExporter for OTLPSpanExporter
- R3: add Jaeger/Tempo collector
"""

from __future__ import annotations

import functools
import inspect
import logging
from typing import Any, Callable, TypeVar

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import Status, StatusCode

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])

# ---------------------------------------------------------------------------
# Global provider setup
# ---------------------------------------------------------------------------

_Provider: TracerProvider | None = None


def _init_provider() -> TracerProvider:
    """Idempotently initialise the global TracerProvider."""
    global _Provider
    if _Provider is not None:
        return _Provider

    exporter = ConsoleSpanExporter()
    processor = BatchSpanProcessor(exporter)
    provider = TracerProvider()
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    _Provider = provider
    logger.debug("TracerProvider initialised with ConsoleSpanExporter")
    return provider


def get_tracer(name: str = "debian_observer") -> trace.Tracer:
    """Return a named tracer, ensuring the provider is alive."""
    _init_provider()
    return trace.get_tracer(name)


def shutdown_tracer() -> None:
    """Flush and shut down the global provider.

    Call this on application exit to avoid dropped spans.
    """
    global _Provider
    if _Provider is not None:
        _Provider.shutdown()
        _Provider = None
        logger.debug("TracerProvider shut down")


# ---------------------------------------------------------------------------
# @span decorator
# ---------------------------------------------------------------------------


def span(
    name: str | None = None,
    kind: trace.SpanKind = trace.SpanKind.INTERNAL,
    attributes: dict[str, Any] | None = None,
) -> Callable[[F], F]:
    """Decorator that wraps a function in an OpenTelemetry span.

    Args:
        name: Span name. Defaults to ``function.__qualname__``.
        kind: Span kind (INTERNAL, SERVER, PRODUCER, CONSUMER, etc.).
        attributes: Static attributes merged into the span.

    Usage::

        @span()
        def ingest_dataset(source: str) -> list[Event]:
            ...

        @span(name="nats_publish", kind=SpanKind.PRODUCER)
        async def publish(event: EventEnvelope) -> int:
            ...
    """

    def decorator(func: F) -> F:
        span_name = name or func.__qualname__
        tracer = get_tracer(func.__module__)

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with tracer.start_as_current_span(span_name, kind=kind) as s:
                if attributes:
                    s.set_attributes(attributes)
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    s.set_status(Status(StatusCode.ERROR, str(exc)))
                    s.record_exception(exc)
                    raise

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            with tracer.start_as_current_span(span_name, kind=kind) as s:
                if attributes:
                    s.set_attributes(attributes)
                try:
                    return await func(*args, **kwargs)
                except Exception as exc:
                    s.set_status(Status(StatusCode.ERROR, str(exc)))
                    s.record_exception(exc)
                    raise

        # Return the appropriate wrapper without executing either
        if inspect.iscoroutinefunction(func):
            return async_wrapper  # type: ignore[return-value]
        return wrapper  # type: ignore[return-value]

    return decorator
