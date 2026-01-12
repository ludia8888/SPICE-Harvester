from __future__ import annotations

from typing import Any, Dict, Optional


def extract_backing_dataset_id(object_type_spec: Any) -> Optional[str]:
    if not isinstance(object_type_spec, dict):
        return None
    backing_source = object_type_spec.get("backing_source")
    if not isinstance(backing_source, dict):
        return None
    dataset_id = backing_source.get("dataset_id")
    if not isinstance(dataset_id, str):
        return None
    dataset_id = dataset_id.strip()
    return dataset_id or None


def policies_aligned(backing_policy: Any, writeback_policy: Any) -> bool:
    if not isinstance(backing_policy, dict) or not isinstance(writeback_policy, dict):
        return False
    return backing_policy == writeback_policy


def format_acl_alignment_result(
    *,
    scope: str,
    writeback_dataset_id: str,
    backing_dataset_id: str,
    backing_policy: Dict[str, Any],
    writeback_policy: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "scope": scope,
        "writeback_dataset_id": writeback_dataset_id,
        "backing_dataset_id": backing_dataset_id,
        "aligned": backing_policy == writeback_policy,
    }
