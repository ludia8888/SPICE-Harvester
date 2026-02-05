from __future__ import annotations

from typing import Any, Dict, List

from shared.services.pipeline.pipeline_schema_utils import normalize_schema_type


def extract_schema_casts(schema_json: Any) -> List[Dict[str, str]]:
    if not isinstance(schema_json, dict):
        return []
    columns = schema_json.get("columns")
    if not isinstance(columns, list):
        columns = schema_json.get("fields")
    if isinstance(columns, list):
        casts: List[Dict[str, str]] = []
        for col in columns:
            if isinstance(col, dict):
                name = str(col.get("name") or "").strip()
                if not name:
                    continue
                cast_type = normalize_schema_type(col.get("type") or col.get("data_type"))
                casts.append({"column": name, "type": cast_type or "xsd:string"})
            elif isinstance(col, str):
                casts.append({"column": col, "type": "xsd:string"})
        return casts
    properties = schema_json.get("properties")
    if isinstance(properties, dict):
        return [{"column": name, "type": "xsd:string"} for name in properties.keys() if name]
    return []

