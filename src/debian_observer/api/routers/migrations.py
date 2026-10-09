"""API router: Migrations.

Migration observation endpoints.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/", response_model=list[dict[str, Any]])
async def list_migrations(request: Request) -> list[dict[str, Any]]:
    """Return all observed migrations.

    Returns:
        List of migration records.
    """
    store = request.app.state.store
    migrations = store.list_migrations()
    return [m.model_dump(mode="json") for m in migrations]
