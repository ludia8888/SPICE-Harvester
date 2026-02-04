"""Shared helpers for objectify artifact outputs."""

from __future__ import annotations

from typing import Any, Dict


def match_output_name(output: Dict[str, Any], name: str) -> bool:
    target = (name or "").strip()
    if not target:
        return False
    for key in ("output_name", "dataset_name", "node_id"):
        candidate = str(output.get(key) or "").strip()
        if candidate and candidate == target:
            return True
    return False
