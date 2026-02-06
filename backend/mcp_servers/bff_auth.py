from __future__ import annotations

from typing import Optional

from shared.config.settings import get_settings


def bff_api_base_url() -> str:
    base = get_settings().services.bff_base_url.rstrip("/")
    return f"{base}/api/v1"


def bff_admin_token() -> Optional[str]:
    return (get_settings().clients.bff_admin_token or "").strip() or None

