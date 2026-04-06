from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

OBJECTIFY_VISIBILITY_FIELD = "objectify_visibility_state"
OBJECTIFY_VISIBILITY_STAGED = "staged"
OBJECTIFY_VISIBILITY_COMMITTED = "committed"


def is_instances_index(index_name: Any) -> bool:
    token = str(index_name or "").strip().lower()
    return "_instances" in token


def staged_visibility_clause() -> Dict[str, Any]:
    return {"term": {OBJECTIFY_VISIBILITY_FIELD: OBJECTIFY_VISIBILITY_STAGED}}


def apply_visible_instances_filter(query: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    hidden_clause = staged_visibility_clause()
    if not isinstance(query, Mapping) or not query:
        return {"bool": {"must_not": [hidden_clause]}}

    if isinstance(query.get("bool"), Mapping):
        bool_query = dict(query.get("bool") or {})
        must_not = list(bool_query.get("must_not") or [])
        must_not.append(hidden_clause)
        bool_query["must_not"] = must_not
        merged = dict(query)
        merged["bool"] = bool_query
        return merged

    return {
        "bool": {
            "must": [dict(query)],
            "must_not": [hidden_clause],
        }
    }


def is_visible_instance_document(document: Mapping[str, Any] | None) -> bool:
    if not isinstance(document, Mapping):
        return False
    visibility = str(document.get(OBJECTIFY_VISIBILITY_FIELD) or "").strip().lower()
    return visibility != OBJECTIFY_VISIBILITY_STAGED
