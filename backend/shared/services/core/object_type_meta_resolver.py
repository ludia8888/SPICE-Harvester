from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, Optional, TypedDict

from shared.utils.resource_rid import parse_metadata_rev
from shared.utils.writeback_conflicts import parse_conflict_policy


class ObjectTypeMeta(TypedDict):
    conflict_policy: Optional[str]
    rev: int


def build_object_type_meta_resolver(
    *,
    resources: Any,
    db_name: str,
    branch: str,
) -> Callable[[str], Awaitable[ObjectTypeMeta]]:
    cache: Dict[str, ObjectTypeMeta] = {}

    async def resolve(class_id: str) -> ObjectTypeMeta:
        key = str(class_id or "").strip()
        if not key:
            return {"conflict_policy": None, "rev": 1}

        cached = cache.get(key)
        if cached is not None:
            return cached

        meta: ObjectTypeMeta = {"conflict_policy": None, "rev": 1}
        try:
            object_resource = await resources.get_resource(
                db_name,
                branch=branch,
                resource_type="object_type",
                resource_id=key,
            )
            obj_spec = object_resource.get("spec") if isinstance(object_resource, dict) else None
            if isinstance(obj_spec, dict):
                meta["conflict_policy"] = parse_conflict_policy(obj_spec.get("conflict_policy"))
            obj_metadata = object_resource.get("metadata") if isinstance(object_resource, dict) else None
            meta["rev"] = parse_metadata_rev(obj_metadata)
        except Exception:
            pass

        cache[key] = meta
        return meta

    return resolve
