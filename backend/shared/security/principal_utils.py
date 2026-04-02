from __future__ import annotations

from typing import Any, Mapping, Optional, Sequence, Set, Tuple


DEFAULT_PRINCIPAL_ID_HEADERS: Tuple[str, ...] = (
    "X-Principal-Id",
    "X-Principal-ID",
    "X-User",
    "X-Actor",
    "X-User-Id",
    "X-User-ID",
)

DEFAULT_PRINCIPAL_TYPE_HEADERS: Tuple[str, ...] = (
    "X-Principal-Type",
    "X-Principal-TYPE",
    "X-Actor-Type",
    "X-User-Type",
)


def resolve_principal_from_headers(
    headers: Optional[Mapping[str, Any]],
    *,
    principal_id_headers: Sequence[str] = DEFAULT_PRINCIPAL_ID_HEADERS,
    principal_type_headers: Sequence[str] = DEFAULT_PRINCIPAL_TYPE_HEADERS,
    default_principal_type: str = "user",
    default_principal_id: str = "system",
    allowed_principal_types: Optional[Set[str]] = None,
) -> tuple[str, str]:
    header_map = headers or {}

    principal_id = ""
    for key in principal_id_headers:
        candidate = _header_value(header_map, key)
        if candidate:
            principal_id = candidate
            break
    if not principal_id:
        principal_id = str(default_principal_id or "system").strip() or "system"

    principal_type = ""
    for key in principal_type_headers:
        candidate = _header_value(header_map, key)
        if candidate:
            principal_type = candidate
            break
    normalized_type = (principal_type or default_principal_type or "user").strip().lower() or "user"
    if allowed_principal_types and normalized_type not in allowed_principal_types:
        normalized_type = (default_principal_type or "user").strip().lower() or "user"
    return normalized_type, principal_id


def actor_label(principal_type: str, principal_id: str) -> str:
    return f"{(principal_type or 'user')}:{(principal_id or 'unknown')}"


def _header_value(headers: Mapping[str, Any], key: str) -> str:
    for candidate in (key, key.lower(), key.upper()):
        value = headers.get(candidate)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""
