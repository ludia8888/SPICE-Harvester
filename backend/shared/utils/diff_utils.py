from __future__ import annotations

import json
from typing import Any, Dict, List, Optional


def _looks_like_change(value: Dict[str, Any]) -> bool:
    for key in ("op", "@op", "type", "path", "id", "@id"):
        if key in value:
            return True
    return False


def normalize_diff_changes(raw: Any) -> List[Dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [dict(change) for change in raw if isinstance(change, dict)]
    if isinstance(raw, dict):
        for key in ("changes", "patch", "diff"):
            if key in raw:
                return normalize_diff_changes(raw.get(key))
        if _looks_like_change(raw):
            return [dict(raw)]
        return []
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        if text.lower() in {"matches", "match"}:
            return []
        try:
            parsed = json.loads(text)
        except Exception:
            return []
        return normalize_diff_changes(parsed)
    return []


def _classify_change(change: Dict[str, Any]) -> Optional[str]:
    op = change.get("type") or change.get("op") or change.get("@op")
    if op is None:
        return None
    op_text = str(op).strip().lower()
    if not op_text:
        return None
    if op_text in {"add", "insert", "create", "new"}:
        return "added"
    if op_text in {"remove", "delete", "drop"} or op_text.startswith("remove"):
        return "deleted"
    if op_text in {"modify", "update", "replace", "swapvalue", "swap"} or "swap" in op_text:
        return "modified"
    return "modified"


def summarize_diff_changes(changes: List[Dict[str, Any]]) -> Dict[str, int]:
    summary = {"added": 0, "modified": 0, "deleted": 0}
    for change in changes:
        if not isinstance(change, dict):
            continue
        kind = _classify_change(change)
        if kind in summary:
            summary[kind] += 1
    return summary


def normalize_diff_response(
    from_ref: Optional[str],
    to_ref: Optional[str],
    raw: Any,
) -> Dict[str, Any]:
    payload = raw
    error: Optional[str] = None
    if isinstance(raw, dict):
        from_ref = raw.get("from", from_ref)
        to_ref = raw.get("to", to_ref)
        if isinstance(raw.get("error"), str):
            error = raw.get("error")
        payload = raw.get("changes", raw)
    changes = normalize_diff_changes(payload)
    summary = summarize_diff_changes(changes)
    response = {
        "from": from_ref,
        "to": to_ref,
        "changes": changes,
        "summary": summary,
    }
    if error:
        response["error"] = error
    return response
