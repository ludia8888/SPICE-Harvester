"""Sheet import parsing helpers (BFF).

Small, reusable helpers shared between ontology suggestion/import endpoints.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from fastapi import UploadFile, status

from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.observability.tracing import trace_external_call


@trace_external_call("bff.sheet_import.read_excel_upload")
async def read_excel_upload(file: UploadFile) -> Tuple[str, bytes]:
    filename = file.filename or "upload.xlsx"
    if not filename.lower().endswith((".xlsx", ".xlsm")):
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "Only .xlsx/.xlsm files are supported", code=ErrorCode.REQUEST_VALIDATION_FAILED)

    content = await file.read()
    if not content:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "Empty file", code=ErrorCode.REQUEST_VALIDATION_FAILED)
    return filename, content


def parse_table_bbox(
    *,
    table_top: Optional[int],
    table_left: Optional[int],
    table_bottom: Optional[int],
    table_right: Optional[int],
) -> Optional[Dict[str, int]]:
    bbox_parts = [table_top, table_left, table_bottom, table_right]
    if not any(v is not None for v in bbox_parts):
        return None
    if any(v is None for v in bbox_parts):
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "table_top/table_left/table_bottom/table_right must be provided together", code=ErrorCode.REQUEST_VALIDATION_FAILED)
    return {
        "top": int(table_top),
        "left": int(table_left),
        "bottom": int(table_bottom),
        "right": int(table_right),
    }


def parse_json_array(
    value: Optional[str],
    *,
    field_name: str,
    required_message: Optional[str] = None,
    treat_blank_as_missing: bool = False,
    type_error_message: Optional[str] = None,
) -> List[Any]:
    if value is None:
        if required_message is not None:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, required_message, code=ErrorCode.REQUEST_VALIDATION_FAILED)
        return []
    if treat_blank_as_missing and value.strip() == "":
        if required_message is not None:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, required_message, code=ErrorCode.REQUEST_VALIDATION_FAILED)
        return []
    try:
        raw = json.loads(value)
        if not isinstance(raw, list):
            raise ValueError(type_error_message or f"{field_name} must be a JSON array")
        return raw
    except Exception as exc:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, f"Invalid {field_name}: {exc}", code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc


def parse_json_object(
    value: Optional[str],
    *,
    field_name: str,
    default: Optional[Dict[str, Any]] = None,
    treat_blank_as_missing: bool = True,
    type_error_message: Optional[str] = None,
) -> Dict[str, Any]:
    if value is None:
        return dict(default) if isinstance(default, dict) else {}
    if treat_blank_as_missing and value.strip() == "":
        return dict(default) if isinstance(default, dict) else {}

    try:
        raw = json.loads(value)
        if not isinstance(raw, dict):
            raise ValueError(type_error_message or f"{field_name} must be an object")
        return raw
    except Exception as exc:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, f"Invalid {field_name}: {exc}", code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc


def parse_target_schema_json(value: Optional[str]) -> List[Dict[str, Any]]:
    raw = parse_json_array(
        value,
        field_name="target_schema_json",
        required_message="target_schema_json is required for import (field types)",
        treat_blank_as_missing=True,
        type_error_message="target_schema_json must be a JSON array",
    )
    return [item for item in raw if isinstance(item, dict)]

