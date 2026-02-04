"""Spark-specific schema and file helpers for the pipeline worker.

These are intentionally kept as pure helpers (no worker instance required) so
they can be imported by tests and reused across orchestration code.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

try:
    from pyspark.sql import DataFrame  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    DataFrame = Any  # type: ignore[assignment,misc]

from shared.services.pipeline.pipeline_type_utils import spark_type_to_xsd
from shared.utils.schema_hash import compute_schema_hash


def _is_data_object(key: str) -> bool:
    if not key:
        return False
    base = os.path.basename(key)
    if base.startswith("_"):
        return False
    if base.startswith("."):
        return False
    return bool(os.path.splitext(base)[1])


def _schema_from_dataframe(frame: DataFrame) -> List[Dict[str, str]]:
    columns: List[Dict[str, str]] = []
    schema = frame.schema
    for field in schema.fields:
        columns.append({"name": field.name, "type": spark_type_to_xsd(field.dataType)})
    return columns


def _hash_schema_columns(columns: List[Dict[str, Any]]) -> str:
    return compute_schema_hash(columns)


def _schema_columns_map(columns: List[Dict[str, Any]]) -> Dict[str, str]:
    output: Dict[str, str] = {}
    for col in columns or []:
        if not isinstance(col, dict):
            continue
        name = str(col.get("name") or col.get("column") or "").strip()
        raw_type = col.get("type") or col.get("data_type") or col.get("datatype")
        if not name:
            continue
        output[name] = str(raw_type).strip().lower() if raw_type is not None else ""
    return output


def _schema_diff(
    *,
    current_columns: List[Dict[str, Any]],
    expected_columns: List[Dict[str, Any]],
) -> Dict[str, Any]:
    current_map = _schema_columns_map(current_columns)
    expected_map = _schema_columns_map(expected_columns)
    current_keys = set(current_map.keys())
    expected_keys = set(expected_map.keys())
    removed = sorted(expected_keys - current_keys)
    added = sorted(current_keys - expected_keys)
    type_changed = []
    for key in sorted(current_keys & expected_keys):
        cur = current_map.get(key)
        exp = expected_map.get(key)
        if exp and cur and exp != cur:
            type_changed.append({"column": key, "expected": exp, "observed": cur})
    return {
        "removed_columns": removed,
        "added_columns": added,
        "type_changes": type_changed,
    }


def _list_part_files(path: str, *, extensions: Optional[set[str]] = None) -> List[str]:
    if extensions is None:
        extensions = {".json", ".parquet"}
    normalized = {ext.lower() for ext in extensions}
    part_files: List[str] = []
    for root, _, files in os.walk(path):
        for name in files:
            if not name.startswith("part-"):
                continue
            ext = os.path.splitext(name)[1].lower()
            if ext in normalized:
                part_files.append(os.path.join(root, name))
    return part_files

