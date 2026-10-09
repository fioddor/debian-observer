"""API router: Datasets.

Dataset manifest and provenance endpoints.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

router = APIRouter()


@router.get("/{dataset_name}", response_model=dict[str, Any])
async def get_dataset(dataset_name: str, request: Request) -> dict[str, Any]:
    """Return manifest for a single dataset.

    Args:
        dataset_name: The dataset name.

    Returns:
        Dataset manifest dict.

    Raises:
        HTTPException: 404 if the dataset is not found.
    """
    store = request.app.state.store
    manifest = store.get_dataset(dataset_name)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found")
    return manifest
