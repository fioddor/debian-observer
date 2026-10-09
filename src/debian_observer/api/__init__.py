"""API layer for Debian Observatory.

Small FastAPI service exposing analytical endpoints for packages,
releases, migrations, datasets, and events.

Usage::

    from debian_observer.api.app import create_app
    from debian_observer.api.store import InMemoryStore

    store = InMemoryStore()
    store.add_package(...)
    app = create_app(store=store)
"""

from __future__ import annotations

from debian_observer.api.app import create_app
from debian_observer.api.store import InMemoryStore

__all__ = ["create_app", "InMemoryStore"]
