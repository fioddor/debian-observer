"""API router: Events.

Event lookup by event_id.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

router = APIRouter()


@router.get("/{event_id}", response_model=dict[str, Any])
async def get_event(event_id: str, request: Request) -> dict[str, Any]:
    """Return a single event by its ID.

    Args:
        event_id: The UUID event identifier.

    Returns:
        Event envelope as a dict.

    Raises:
        HTTPException: 404 if the event is not found.
    """
    store = request.app.state.store
    event = store.get_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail=f"Event '{event_id}' not found")
    return event.model_dump(mode="json")
