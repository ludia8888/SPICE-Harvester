"""Helpers for reading nested instance ``properties[]`` payloads."""

from __future__ import annotations

from typing import Any, Dict, Iterator, Tuple


def instance_property_name(prop: Any) -> str:
    if not isinstance(prop, dict):
        return ""
    return str(prop.get("name") or prop.get("id") or "").strip()


def iter_instance_properties(properties: Any) -> Iterator[Tuple[str, Dict[str, Any]]]:
    if not isinstance(properties, list):
        return
    for prop in properties:
        if not isinstance(prop, dict):
            continue
        name = instance_property_name(prop)
        if name:
            yield name, prop


def flatten_instance_properties(properties: Any) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    for name, prop in iter_instance_properties(properties):
        flat[name] = prop.get("value")
    return flat
