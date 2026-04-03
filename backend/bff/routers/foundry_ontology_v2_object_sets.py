from __future__ import annotations

from typing import Any

from bff.routers.foundry_ontology_v2_errors import ObjectSetNotFoundError
from shared.config.settings import get_settings
from shared.errors.infra_errors import RegistryUnavailableError
from shared.foundry.temporary_object_set_store import FoundryTemporaryObjectSetStore
from shared.services.storage.redis_service import RedisService


def _temporary_object_set_store(redis_service: RedisService) -> FoundryTemporaryObjectSetStore:
    settings = get_settings()
    return FoundryTemporaryObjectSetStore(
        redis_service=redis_service,
        ttl_seconds=int(settings.cache.foundry_temp_object_set_ttl_seconds),
    )


async def _store_temporary_object_set(
    object_set: dict[str, Any],
    *,
    redis_service: RedisService,
) -> str:
    store = _temporary_object_set_store(redis_service)
    return await store.create(object_set)


async def _load_temporary_object_set(
    rid: str,
    *,
    redis_service: RedisService,
) -> dict[str, Any]:
    store = _temporary_object_set_store(redis_service)
    try:
        return await store.get(rid)
    except LookupError as exc:
        raise ObjectSetNotFoundError(str(exc)) from exc
    except RegistryUnavailableError:
        raise


def _collect_object_set_object_types(object_set: Any) -> list[str]:
    stack: list[Any] = [object_set]
    visited: set[int] = set()
    out: list[str] = []
    seen: set[str] = set()

    while stack:
        current = stack.pop()
        if not isinstance(current, dict):
            continue
        marker = id(current)
        if marker in visited:
            continue
        visited.add(marker)

        for key in ("objectType", "objectTypeApiName"):
            candidate = str(current.get(key) or "").strip()
            if candidate and candidate not in seen:
                seen.add(candidate)
                out.append(candidate)

        for value in current.values():
            if isinstance(value, dict):
                stack.append(value)
            elif isinstance(value, list):
                stack.extend(item for item in value if isinstance(item, dict))
    return out


def _resolve_object_set_object_type(object_set: Any) -> str | None:
    object_types = _collect_object_set_object_types(object_set)
    return object_types[0] if object_types else None


async def _resolve_object_set_definition(object_set: Any) -> dict[str, Any]:
    if isinstance(object_set, dict):
        return dict(object_set)
    if isinstance(object_set, str):
        raise ValueError("Temporary objectSet RID resolution requires redis_service")
    raise ValueError("objectSet is required")


async def _resolve_object_set_definition_with_store(
    object_set: Any,
    *,
    redis_service: RedisService,
) -> dict[str, Any]:
    if isinstance(object_set, dict):
        return dict(object_set)
    if isinstance(object_set, str):
        return await _load_temporary_object_set(object_set, redis_service=redis_service)
    raise ValueError("objectSet is required")


def _is_search_around_object_set(object_set: Any) -> bool:
    if not isinstance(object_set, dict):
        return False
    object_set_type = str(object_set.get("type") or "").strip().lower()
    return object_set_type == "searcharound"


def _extract_search_around_link_type(object_set: dict[str, Any]) -> str:
    link = object_set.get("link")
    if isinstance(link, str):
        link_type = link.strip()
    elif isinstance(link, dict):
        link_type = str(link.get("apiName") or link.get("linkType") or "").strip()
    else:
        link_type = ""
    if not link_type:
        raise ValueError("searchAround objectSet requires link")
    return link_type


def _dedupe_rows_by_identity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        object_type = str(row.get("objectType") or row.get("objectTypeApiName") or "").strip()
        primary_key = str(row.get("primaryKey") or "").strip()
        key = (object_type, primary_key)
        if object_type and primary_key:
            if key in seen:
                continue
            seen.add(key)
        deduped.append(row)
    return deduped
