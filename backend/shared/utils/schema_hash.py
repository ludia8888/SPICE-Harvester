"""
Shared helpers for computing stable schema hashes.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, List, Mapping, Optional

from shared.utils.schema_columns import extract_schema_columns


def compute_schema_hash(columns: List[Mapping[str, Any]]) -> str:
    """
    Produce a stable hash for a list of column definitions.

    Sorting keys ensures field order within each column dict does not change the hash,
    but the column order in the list is still significant (matches pipeline output order).
    """
    payload = json.dumps(columns or [], sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_schema_hash_from_sample(sample_json: Any) -> Optional[str]:
    if not isinstance(sample_json, dict):
        return None
    columns = sample_json.get("columns")
    if not isinstance(columns, list) or not columns:
        return None
    return compute_schema_hash(columns)


def compute_schema_hash_from_payload(payload: Any) -> Optional[str]:
    columns = extract_schema_columns(payload)
    if not columns:
        return None
    return compute_schema_hash(columns)
