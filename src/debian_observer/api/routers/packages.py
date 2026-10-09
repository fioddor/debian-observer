"""API router: Packages.

Package metadata and version endpoints.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

router = APIRouter()


@router.get("/{package_name}", response_model=dict[str, Any])
async def get_package(package_name: str, request: Request) -> dict[str, Any]:
    """Return metadata for a single package.

    Args:
        package_name: The human-readable package name (e.g. "libc6").

    Returns:
        Package metadata as a dict.

    Raises:
        HTTPException: 404 if the package is not found.
    """
    store = request.app.state.store
    package = store.get_package(package_name)
    if package is None:
        raise HTTPException(status_code=404, detail=f"Package '{package_name}' not found")
    return package.model_dump(mode="json")


@router.get("/{package_name}/versions", response_model=list[dict[str, Any]])
async def get_package_versions(package_name: str, request: Request) -> list[dict[str, Any]]:
    """Return all observed versions for a package.

    Args:
        package_name: The human-readable package name.

    Returns:
        List of version records.

    Raises:
        HTTPException: 404 if the package is not found.
    """
    store = request.app.state.store
    versions = store.get_package_versions(package_name)
    if versions is None:
        raise HTTPException(status_code=404, detail=f"Package '{package_name}' not found")
    return [v.model_dump(mode="json") for v in versions]
