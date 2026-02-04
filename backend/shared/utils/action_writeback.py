from __future__ import annotations

from typing import Any, Dict
from uuid import UUID

from shared.utils.deterministic_ids import deterministic_uuid5


def action_applied_event_id(action_log_id: str) -> UUID:
    return deterministic_uuid5(f"action-applied:{action_log_id}")


def safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def is_noop_changes(changes: Dict[str, Any]) -> bool:
    if not isinstance(changes, dict):
        return True
    if bool(changes.get("delete")):
        return False
    return not (
        (changes.get("set") or {})
        or (changes.get("unset") or [])
        or (changes.get("link_add") or [])
        or (changes.get("link_remove") or [])
    )
