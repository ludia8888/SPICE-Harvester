from __future__ import annotations

from typing import Any


def normalize_dependency_entries(raw: Any, *, strict: bool = False) -> tuple[list[dict[str, str]], int]:
    if raw is None:
        return [], 0
    if not isinstance(raw, list):
        if strict:
            raise ValueError("dependencies must be a list")
        return [], 1

    output: list[dict[str, str]] = []
    invalid = 0
    for item in raw:
        if not isinstance(item, dict):
            if strict:
                raise ValueError("dependencies items must be objects")
            invalid += 1
            continue
        pipeline_id = str(
            item.get("pipeline_id") or item.get("pipelineId") or item.get("pipelineID") or ""
        ).strip()
        if not pipeline_id:
            if strict:
                raise ValueError("dependencies pipeline_id is required")
            invalid += 1
            continue
        status = str(item.get("status") or item.get("required_status") or "DEPLOYED").strip().upper()
        output.append({"pipeline_id": pipeline_id, "status": status})
    return output, invalid
