"""Pipeline dataset funnel/schema helpers.

Small, stable helpers extracted from `bff.routers.pipeline_datasets_ops`.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import HTTPException, status

from shared.config.settings import get_settings

logger = logging.getLogger(__name__)


def _normalize_inferred_type(type_value: Any) -> str:
    if not type_value:
        return "xsd:string"
    raw = str(type_value).strip()
    if not raw:
        return "xsd:string"
    if raw.startswith("xsd:"):
        return raw
    lowered = raw.lower()
    if lowered == "money":
        return "xsd:decimal"
    if lowered in {"integer", "int"}:
        return "xsd:integer"
    if lowered in {"decimal", "number", "float", "double"}:
        return "xsd:decimal"
    if lowered in {"boolean", "bool"}:
        return "xsd:boolean"
    if lowered in {"datetime", "timestamp"}:
        return "xsd:dateTime"
    if lowered == "date":
        return "xsd:dateTime"
    return "xsd:string"


def _build_schema_columns(columns: list[str], inferred_schema: list[Dict[str, Any]]) -> list[Dict[str, str]]:
    inferred_map: Dict[str, str] = {}
    for item in inferred_schema or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or item.get("column_name") or "").strip()
        if not name:
            continue
        inferred_type = item.get("type") or item.get("data_type")
        if not inferred_type:
            inferred_payload = item.get("inferred_type")
            if isinstance(inferred_payload, dict):
                inferred_type = inferred_payload.get("type")
        inferred_map[name] = _normalize_inferred_type(inferred_type)

    schema_columns: list[Dict[str, str]] = []
    for col in columns:
        col_name = str(col)
        if not col_name:
            continue
        schema_columns.append({"name": col_name, "type": inferred_map.get(col_name, "xsd:string")})
    return schema_columns


def _columns_from_schema(schema_columns: list[Dict[str, str]]) -> list[Dict[str, str]]:
    return [
        {"name": str(col.get("name") or ""), "type": str(col.get("type") or "String")}
        for col in schema_columns
        if str(col.get("name") or "")
    ]


def _rows_from_preview(columns: list[str], sample_rows: list[list[Any]]) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    for row in sample_rows or []:
        if not isinstance(row, list):
            continue
        payload: Dict[str, Any] = {}
        for index, col in enumerate(columns):
            payload[str(col)] = row[index] if index < len(row) else None
        rows.append(payload)
    return rows


FUNNEL_RISK_POLICY = {"stage": "funnel", "suggestion_only": True, "hard_gate": False}


def _build_funnel_analysis_payload(
    analysis: Optional[Dict[str, Any]], inferred_schema: list[Dict[str, Any]]
) -> Dict[str, Any]:
    payload = dict(analysis or {})
    if "columns" not in payload and inferred_schema:
        payload["columns"] = inferred_schema
    if "risk_summary" not in payload:
        payload["risk_summary"] = []
    if "risk_policy" not in payload:
        payload["risk_policy"] = FUNNEL_RISK_POLICY
    return payload


def _extract_sample_columns(sample_json: Any) -> list[str]:
    if not isinstance(sample_json, dict):
        return []
    raw_columns = sample_json.get("columns")
    if isinstance(raw_columns, list):
        columns: list[str] = []
        for col in raw_columns:
            if isinstance(col, dict):
                name = str(col.get("name") or col.get("column") or "").strip()
                if name:
                    columns.append(name)
            else:
                name = str(col).strip()
                if name:
                    columns.append(name)
        if columns:
            return columns
    raw_fields = sample_json.get("fields")
    if isinstance(raw_fields, list):
        columns = []
        for col in raw_fields:
            if isinstance(col, dict) and col.get("name"):
                columns.append(str(col.get("name")))
        if columns:
            return columns
    raw_props = sample_json.get("properties")
    if isinstance(raw_props, dict):
        return [str(key) for key in raw_props.keys() if str(key)]
    return []


def _extract_sample_rows(sample_json: Any, columns: list[str]) -> list[list[Any]]:
    if not isinstance(sample_json, dict):
        return []
    rows = sample_json.get("rows")
    if rows is None:
        rows = sample_json.get("data")
    if not isinstance(rows, list):
        return []
    if rows and isinstance(rows[0], dict):
        if not columns:
            seen: set[str] = set()
            ordered: list[str] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                for key in row.keys():
                    key_str = str(key)
                    if key_str and key_str not in seen:
                        seen.add(key_str)
                        ordered.append(key_str)
            columns[:] = ordered
        return [[row.get(col) for col in columns] for row in rows if isinstance(row, dict)]
    if rows and isinstance(rows[0], list):
        list_rows = [row for row in rows if isinstance(row, list)]
        if not columns and list_rows:
            columns[:] = [f"col_{idx}" for idx in range(len(list_rows[0]))]
        return list_rows
    return []


async def _compute_funnel_analysis_from_sample(sample_json: Any) -> Dict[str, Any]:
    columns = _extract_sample_columns(sample_json)
    rows = _extract_sample_rows(sample_json, columns)
    if not columns and not rows:
        return _build_funnel_analysis_payload(None, [])
    try:
        from bff.services.funnel_client import FunnelClient

        settings = get_settings()
        async with FunnelClient() as funnel_client:
            analysis = await funnel_client.analyze_dataset(
                {
                    "data": rows,
                    "columns": columns,
                    "sample_size": min(len(rows), 500) if rows else 0,
                    "include_complex_types": True,
                },
                timeout_seconds=float(settings.services.funnel_infer_timeout_seconds),
            )
        analysis_payload = analysis if isinstance(analysis, dict) else None
        inferred_schema = (analysis_payload or {}).get("columns") or []
        return _build_funnel_analysis_payload(analysis_payload, inferred_schema)
    except Exception as exc:
        logger.warning("Funnel analysis failed: %s", exc)
        from shared.services.pipeline.pipeline_funnel_fallback import build_funnel_analysis_fallback

        fallback = build_funnel_analysis_fallback(
            columns=columns,
            rows=rows,
            include_complex_types=True,
            error=str(exc),
            stage="bff",
        )
        return _build_funnel_analysis_payload(fallback, fallback.get("columns") or [])


def _select_sample_row(
    sample_json: Dict[str, Any],
    *,
    filename: Optional[str],
    file_index: Optional[int],
) -> Optional[Dict[str, Any]]:
    rows = sample_json.get("rows") if isinstance(sample_json, dict) else None
    if not isinstance(rows, list):
        return None
    candidates = [row for row in rows if isinstance(row, dict)]
    if not candidates:
        return None
    if file_index is not None:
        if file_index < 0 or file_index >= len(candidates):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File index out of range")
        return candidates[file_index]
    if filename:
        for row in candidates:
            if str(row.get("filename") or "") == filename:
                return row
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found in dataset")
    return candidates[0]

