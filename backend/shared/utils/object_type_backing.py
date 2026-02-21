from __future__ import annotations

from typing import Any, Dict, List


def list_backing_sources(spec: Any) -> List[Dict[str, Any]]:
    if not isinstance(spec, dict):
        return []

    sources: List[Dict[str, Any]] = []
    raw_sources = spec.get("backing_sources")
    if isinstance(raw_sources, list):
        sources.extend(item for item in raw_sources if isinstance(item, dict))

    raw_source = spec.get("backing_source")
    if isinstance(raw_source, dict) and raw_source and not sources:
        sources.append(raw_source)
    return sources


def select_primary_backing_source(spec: Any) -> Dict[str, Any]:
    sources = list_backing_sources(spec)
    if sources:
        return dict(sources[0])
    return {}

