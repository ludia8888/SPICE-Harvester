from __future__ import annotations

from typing import Any, Dict

DEFAULT_LIFECYCLE_ID = "lc-0"


def derive_lifecycle_id(instance_state: Any) -> str:
    """
    Derive a stable lifecycle/epoch identifier for an instance.

    This is a P0 requirement for delete/recreate semantics in ACTION_WRITEBACK_DESIGN.md.
    """
    if not isinstance(instance_state, dict):
        return DEFAULT_LIFECYCLE_ID

    value = instance_state.get("lifecycle_id")
    if isinstance(value, str) and value.strip():
        return value.strip()

    meta: Dict[str, Any] = instance_state.get("_metadata") if isinstance(instance_state.get("_metadata"), dict) else {}
    value = meta.get("lifecycle_id")
    if isinstance(value, str) and value.strip():
        return value.strip()

    for key in ("created_command_id", "create_command_id", "creation_command_id", "epoch_id", "epoch"):
        candidate = meta.get(key)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()

    history = meta.get("command_history")
    if isinstance(history, list) and history:
        for item in reversed(history):
            if not isinstance(item, dict):
                continue
            command_type = str(item.get("command_type") or "").strip()
            if command_type in ("CREATE_INSTANCE", "BULK_CREATE_INSTANCES"):
                command_id = item.get("command_id")
                if command_id is None:
                    continue
                command_id_str = str(command_id).strip()
                if command_id_str:
                    return command_id_str

        for item in reversed(history):
            if not isinstance(item, dict):
                continue
            command_id = item.get("command_id")
            if command_id is None:
                continue
            command_id_str = str(command_id).strip()
            if command_id_str:
                return command_id_str

    return DEFAULT_LIFECYCLE_ID


def overlay_doc_id(*, instance_id: str, lifecycle_id: str) -> str:
    """
    Build the ES `_id` for overlay documents as (instance_id, lifecycle_id).

    Delimiter is `|` because instance_id validation forbids it, making it unambiguous.
    """
    iid = str(instance_id or "").strip()
    lc = str(lifecycle_id or "").strip() or DEFAULT_LIFECYCLE_ID
    if not iid:
        raise ValueError("instance_id is required")
    if "|" in iid or "|" in lc:
        raise ValueError("overlay_doc_id delimiter collision")
    return f"{iid}|{lc}"

