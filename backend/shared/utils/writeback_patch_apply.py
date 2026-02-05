from __future__ import annotations

from typing import Any, Dict


def apply_changes_to_payload(payload: Dict[str, Any], changes: Dict[str, Any]) -> bool:
    """
    Apply a writeback patchset `changes` object to a payload dict.

    Patchset shape (normalized):
    - {"delete": true} marks the payload as tombstoned
    - {"set": {"field": value, ...}} applies field updates
    - {"unset": ["field", ...]} removes fields
    - {"link_add": [...], "link_remove": [...]} best-effort link list updates

    Returns:
        True if the patch indicates deletion (tombstone), else False.
    """

    if not isinstance(payload, dict):
        raise TypeError("payload must be a dict")
    if not isinstance(changes, dict):
        raise TypeError("changes must be a dict")

    if bool(changes.get("delete")):
        return True

    set_ops = changes.get("set") if isinstance(changes.get("set"), dict) else {}
    unset_ops = changes.get("unset") if isinstance(changes.get("unset"), list) else []

    for key, value in (set_ops or {}).items():
        if isinstance(key, str) and key:
            payload[key] = value
    for key in unset_ops or []:
        if isinstance(key, str) and key:
            payload.pop(key, None)

    # Link ops are supported in the patchset shape but currently best-effort.
    # Accept either {"field": "...", "value": "..."} or "field:value".
    for op_key, add in (("link_add", True), ("link_remove", False)):
        ops = changes.get(op_key)
        if not isinstance(ops, list):
            continue
        for item in ops:
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
                raw = item.strip()
                if ":" in raw:
                    field, target = raw.split(":", 1)
            field_str = str(field or "").strip()
            target_str = str(target or "").strip()
            if not field_str or not target_str:
                continue
            existing = payload.get(field_str)
            if isinstance(existing, list):
                values = [str(v) for v in existing if v is not None]
            elif existing is None:
                values = []
            else:
                values = [str(existing)]
            if add:
                if target_str not in values:
                    values.append(target_str)
            else:
                values = [v for v in values if v != target_str]
            payload[field_str] = values

    return False

