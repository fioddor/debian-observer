"""API: FastAPI application factory.

Creates and configures the FastAPI application with routers and
an in-memory data store.

Usage::

    from debian_observer.api.app import create_app
    from debian_observer.api.store import InMemoryStore

    store = InMemoryStore()
    # ... populate store ...
    app = create_app(store=store)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Request

from debian_observer.api.store import InMemoryStore
from debian_observer.api.routers import (
    datasets,
    events,
    health,
    metrics,
    migrations,
    packages,
    releases,
)
from debian_observer.observability.tracing import get_tracer
from debian_observer.observability.metrics import APIMetrics


def create_app(
    *,
    store: InMemoryStore | None = None,
    **fastapi_kwargs: Any,
) -> FastAPI:
    """Factory that creates and configures the FastAPI application.

    Args:
        store: Data store for the API. Defaults to an empty InMemoryStore.
        **fastapi_kwargs: Extra arguments forwarded to FastAPI constructor.

    Returns:
        A configured FastAPI application instance.
    """
    app = FastAPI(
        title="Debian Observer",
        description="Event-driven observatory for Debian releases.",
        version="0.1.0",
        **fastapi_kwargs,
    )

    # Observability middleware: trace + time every request
    tracer = get_tracer("debian_observer.api")
    api_metrics = APIMetrics()

    @app.middleware("http")
    async def observability_middleware(request: Request, call_next):
        import time
        start = time.perf_counter()
        method = request.method
        path = request.url.path

        with tracer.start_as_current_span("api.request") as span:
            span.set_attribute("http.method", method)
            span.set_attribute("http.route", path)
            response = await call_next(request)
            duration = time.perf_counter() - start
            span.set_attribute("http.status_code", response.status_code)
            api_metrics.record_duration(duration, endpoint=path, method=method)

        return response

    # Use an empty store if none provided
    data_store = store if store is not None else InMemoryStore()

    # Attach store to app state for router access
    app.state.store = data_store

    # Include routers
    app.include_router(health.router, tags=["Health"])
    app.include_router(packages.router, prefix="/packages", tags=["Packages"])
    app.include_router(releases.router, prefix="/releases", tags=["Releases"])
    app.include_router(migrations.router, prefix="/migrations", tags=["Migrations"])
    app.include_router(datasets.router, prefix="/datasets", tags=["Datasets"])
    app.include_router(events.router, prefix="/events", tags=["Events"])
    app.include_router(metrics.router, prefix="/metrics", tags=["Metrics"])

    return app
