from __future__ import annotations

from typing import Any, Dict, List, Tuple

from shared.utils.canonical_json import sha256_canonical_json_prefixed


_ALLOWED_CONFLICT_POLICIES = {"WRITEBACK_WINS", "BASE_WINS", "FAIL", "MANUAL_REVIEW"}


def parse_conflict_policy(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    if not text:
        return None
    return text if text in _ALLOWED_CONFLICT_POLICIES else None


def normalize_conflict_policy(value: Any) -> str:
    return parse_conflict_policy(value) or "FAIL"


def extract_action_targets(input_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Minimal, code-aligned target format (P0):
    - Either {class_id, instance_id, changes:{set,unset,delete}}
    - Or {targets:[...]} with the same per-target shape
    """
    if not isinstance(input_payload, dict):
        raise ValueError("action input must be an object")

    targets = input_payload.get("targets")
    if isinstance(targets, list) and targets:
        out = [t for t in targets if isinstance(t, dict)]
        if not out:
            raise ValueError("action input.targets must contain objects")
        return out

    if input_payload.get("instance_id") and input_payload.get("class_id"):
        return [input_payload]

    raise ValueError("action input must include targets[] or (class_id, instance_id)")


def normalize_changes(target: Dict[str, Any]) -> Dict[str, Any]:
    raw = target.get("changes")
    if raw is None:
        raw = {k: target.get(k) for k in ("set", "unset", "link_add", "link_remove", "delete") if k in target}
    if not isinstance(raw, dict):
        raise ValueError("target.changes must be an object")
    set_ops = raw.get("set") or {}
    unset_ops = raw.get("unset") or []
    delete_flag = bool(raw.get("delete") or False)

    if set_ops is not None and not isinstance(set_ops, dict):
        raise ValueError("changes.set must be an object")
    if unset_ops is not None and not isinstance(unset_ops, list):
        raise ValueError("changes.unset must be a list")
    if unset_ops and not all(isinstance(item, str) and item.strip() for item in unset_ops):
        raise ValueError("changes.unset must be a list of non-empty strings")

    return {
        "set": dict(set_ops or {}),
        "unset": [str(k).strip() for k in (unset_ops or []) if str(k).strip()],
        "link_add": list(raw.get("link_add") or []),
        "link_remove": list(raw.get("link_remove") or []),
        "delete": delete_flag,
    }


def _normalize_link_ops(raw: Any) -> List[Tuple[str, str]]:
    ops: List[Tuple[str, str]] = []
    if not isinstance(raw, list):
        return ops
    for item in raw:
        field = None
        target = None
        if isinstance(item, dict):
            field = item.get("field") or item.get("predicate") or item.get("name")
            target = item.get("value") or item.get("to") or item.get("target")
            if (field is None or target is None) and len(item) == 1:
                k, v = next(iter(item.items()))
                field = k
                target = v
        elif isinstance(item, str):
            raw_str = item.strip()
            if ":" in raw_str:
                field, target = raw_str.split(":", 1)
        field_str = str(field or "").strip()
        target_str = str(target or "").strip()
        if not field_str or not target_str:
            continue
        ops.append((field_str, target_str))
    return ops


def compute_observed_base(*, base: Dict[str, Any], changes: Dict[str, Any]) -> Dict[str, Any]:
    fields: Dict[str, Any] = {}
    for key in set(changes.get("set", {}).keys()) | set(changes.get("unset", []) or []):
        if key in base:
            fields[key] = base.get(key)
        else:
            fields[key] = None

    links: Dict[str, Any] = {}
    touched = {
        field
        for field, _t in _normalize_link_ops(changes.get("link_add")) + _normalize_link_ops(changes.get("link_remove"))
    }
    for field in touched:
        current = base.get(field)
        if isinstance(current, list):
            links[field] = [str(v) for v in current if v is not None]
        elif current is None:
            links[field] = []
        else:
            links[field] = [str(current)]

    return {"fields": fields, "links": links}


def compute_base_token(
    *,
    db_name: str,
    class_id: str,
    instance_id: str,
    lifecycle_id: str,
    base_doc: Dict[str, Any],
    object_type_version_id: str | None = None,
) -> Dict[str, Any]:
    base_state_hash = sha256_canonical_json_prefixed(base_doc)
    base_dataset_version_id = None
    meta = base_doc.get("_metadata") if isinstance(base_doc.get("_metadata"), dict) else {}
    for k in ("dataset_version_id", "backing_datasource_version_id", "base_dataset_version_id"):
        value = meta.get(k)
        if isinstance(value, str) and value.strip():
            base_dataset_version_id = value.strip()
            break
    base_dataset_version_id = base_dataset_version_id or "unknown"
    return {
        "base_dataset_version_id": base_dataset_version_id,
        "object_type_version_id": object_type_version_id or f"object_type:{class_id}",
        "instance_id": instance_id,
        "lifecycle_id": lifecycle_id,
        "base_state_hash": base_state_hash,
        "db_name": db_name,
    }


def detect_overlap_fields(*, observed_base: Dict[str, Any], current_base: Dict[str, Any]) -> List[str]:
    """Return field names whose current value differs from the observed_base snapshot."""
    conflict_fields: List[str] = []
    if not isinstance(observed_base, dict) or not isinstance(current_base, dict):
        return conflict_fields
    observed_fields = observed_base.get("fields") if isinstance(observed_base.get("fields"), dict) else {}
    for field, expected_value in observed_fields.items():
        field_name = str(field or "").strip()
        if not field_name:
            continue
        current_value = current_base.get(field_name) if field_name in current_base else None
        if current_value != expected_value:
            conflict_fields.append(field_name)
    return conflict_fields


def detect_overlap_links(
    *,
    observed_base: Dict[str, Any],
    current_base: Dict[str, Any],
    changes: Dict[str, Any],
) -> List[Dict[str, str]]:
    """
    Element-level link conflict detection (P0, best-effort).

    Conflict rule (design):
    - base removed vs patch adds => conflict
    - base added vs patch removes => conflict
    """
    conflicts: List[Dict[str, str]] = []
    if not isinstance(observed_base, dict) or not isinstance(current_base, dict) or not isinstance(changes, dict):
        return conflicts

    observed_links = observed_base.get("links") if isinstance(observed_base.get("links"), dict) else {}

    def _link_set(value: Any) -> set[str]:
        if value is None:
            return set()
        if isinstance(value, list):
            return {str(v) for v in value if v is not None}
        return {str(value)}

    add_ops = _normalize_link_ops(changes.get("link_add"))
    remove_ops = _normalize_link_ops(changes.get("link_remove"))

    for field, target in add_ops:
        observed_set = _link_set(observed_links.get(field))
        current_set = _link_set(current_base.get(field))
        if target in observed_set and target not in current_set:
            conflicts.append({"field": field, "value": target, "direction": "BASE_REMOVED_PATCH_ADDS"})

    for field, target in remove_ops:
        observed_set = _link_set(observed_links.get(field))
        current_set = _link_set(current_base.get(field))
        if target not in observed_set and target in current_set:
            conflicts.append({"field": field, "value": target, "direction": "BASE_ADDED_PATCH_REMOVES"})

    return conflicts


def resolve_applied_changes(
    *,
    conflict_policy: str,
    changes: Dict[str, Any],
    conflict_fields: List[str],
    conflict_links: List[Dict[str, str]] | None = None,
) -> tuple[Dict[str, Any], str]:
    """
    Resolve an Action target's applied changes based on conflict_policy.

    Resolution values:
    - APPLIED: applied_changes are applied as-is
    - SKIPPED: no-op applied_changes (BASE_WINS)
    - REJECTED: signal caller to reject at the action level (FAIL / MANUAL_REVIEW)
    """
    policy = normalize_conflict_policy(conflict_policy)
    has_conflict = bool(conflict_fields) or bool(conflict_links or [])
    if not has_conflict:
        return changes, "APPLIED"
    if policy == "WRITEBACK_WINS":
        return changes, "APPLIED"
    if policy == "BASE_WINS":
        return {"set": {}, "unset": [], "link_add": [], "link_remove": [], "delete": False}, "SKIPPED"
    return changes, "REJECTED"
