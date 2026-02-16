"""Utilities for consistent deprecation headers on compatibility endpoints."""

from __future__ import annotations

from fastapi import Response

# Keep a >=12 month migration window for v1 compatibility routes.
_SUNSET_HTTP_DATE = "Sun, 28 Feb 2027 23:59:59 GMT"
_MIGRATION_GUIDE_PATH = "/docs/FOUNDRY_V1_TO_V2_MIGRATION.md"


def apply_v1_to_v2_deprecation_headers(
    response: Response,
    *,
    successor_path: str,
) -> None:
    """
    Attach RFC8594-style deprecation metadata for v1 compatibility routes.
    """
    response.headers["Deprecation"] = "true"
    response.headers["Sunset"] = _SUNSET_HTTP_DATE
    response.headers["Warning"] = (
        '299 - "This v1 endpoint is deprecated and will be removed after sunset; migrate to the Foundry v2 successor."'
    )
    response.headers["Link"] = (
        f"<{successor_path}>; rel=\"successor-version\", "
        f"<{_MIGRATION_GUIDE_PATH}>; rel=\"deprecation\"; type=\"text/markdown\""
    )
