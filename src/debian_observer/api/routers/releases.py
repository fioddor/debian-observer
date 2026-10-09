"""API router: Releases.

Release and suite endpoints.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/", response_model=list[dict[str, Any]])
async def list_releases(request: Request) -> list[dict[str, Any]]:
    """Return all known Debian releases.

    Returns:
        List of release records.
    """
    store = request.app.state.store
    releases = store.list_releases()
    return [r.model_dump(mode="json") for r in releases]
