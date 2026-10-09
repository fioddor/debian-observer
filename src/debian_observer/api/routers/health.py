"""API router: Health.

Health check endpoint.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

router = APIRouter()


@router.get("/health", response_model=dict[str, str])
async def health_check() -> dict[str, str]:
    """Return service health status.

    Returns:
        A dict with status "ok" and the current UTC timestamp.
    """
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
