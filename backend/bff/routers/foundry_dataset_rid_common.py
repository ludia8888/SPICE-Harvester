"""Shared Foundry dataset RID helpers."""

from __future__ import annotations

from shared.foundry.rids import build_rid, parse_rid


def _dataset_id_from_rid(dataset_rid: str) -> str | None:
    text = str(dataset_rid or "").strip()
    if not text:
        return None
    try:
        kind, parsed = parse_rid(text)
    except ValueError:
        return None
    if kind != "dataset":
        return None
    return parsed


def _dataset_rid(dataset_id: str) -> str:
    return build_rid("dataset", dataset_id)
