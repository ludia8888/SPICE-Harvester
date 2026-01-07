"""
Ontology type normalization helpers.
"""

from __future__ import annotations

from typing import Any, Optional


def normalize_ontology_base_type(value: Any) -> Optional[str]:
    """
    Normalize ontology/base type identifiers to a canonical token.

    This keeps semantic families (array/struct/vector/geopoint/geoshape) distinct,
    while collapsing aliases (int -> integer, url -> string, timestamp -> datetime).
    """
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None

    lowered = raw.lower()
    if lowered.startswith("xsd:"):
        lowered = lowered[4:]
    elif lowered.startswith("sys:"):
        lowered = lowered[4:]

    # Canonicalize aliases
    alias_map = {
        # String-like
        "string": "string",
        "text": "string",
        "anyuri": "string",
        "uri": "string",
        "url": "string",
        "email": "string",
        "uuid": "string",
        "phone": "string",
        "ip": "string",
        "cipher": "cipher",
        "ciphertext": "cipher",
        "marking": "marking",
        "media": "media",
        "media_reference": "media",
        "attachment": "attachment",
        "time_series": "time_series",
        "timeseries": "time_series",
        # Integer-like
        "int": "integer",
        "integer": "integer",
        "long": "integer",
        "short": "integer",
        "byte": "integer",
        "unsignedint": "integer",
        "unsignedlong": "integer",
        "unsignedshort": "integer",
        "unsignedbyte": "integer",
        # Decimal-like
        "number": "decimal",
        "decimal": "decimal",
        "money": "decimal",
        # Floating-point
        "float": "float",
        "double": "double",
        # Boolean
        "bool": "boolean",
        "boolean": "boolean",
        # Temporal
        "date": "date",
        "datetime": "datetime",
        "timestamp": "datetime",
        "time": "time",
        # Complex
        "array": "array",
        "list": "array",
        "set": "array",
        "struct": "struct",
        "object": "struct",
        "json": "struct",
        "vector": "vector",
        "geopoint": "geopoint",
        "geo_point": "geopoint",
        "geoshape": "geoshape",
        "geo_shape": "geoshape",
    }

    return alias_map.get(lowered, lowered)
