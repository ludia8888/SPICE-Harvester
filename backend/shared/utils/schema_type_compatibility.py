"""Schema type compatibility helpers.

This centralizes simple XSD type compatibility rules used across services.
"""

from __future__ import annotations

from typing import Set

TYPE_COMPATIBILITY: dict[str, Set[str]] = {
    "xsd:string": {"xsd:string"},
    "xsd:integer": {"xsd:integer"},
    "xsd:decimal": {"xsd:decimal", "xsd:integer"},
    "xsd:boolean": {"xsd:boolean"},
    "xsd:date": {"xsd:date", "xsd:dateTime"},
    "xsd:dateTime": {"xsd:dateTime", "xsd:date"},
    "xsd:time": {"xsd:time"},
}


def is_type_compatible(source_type: str, target_type: str) -> bool:
    allowed = TYPE_COMPATIBILITY.get(target_type)
    if allowed is None:
        return source_type == target_type
    return source_type in allowed

