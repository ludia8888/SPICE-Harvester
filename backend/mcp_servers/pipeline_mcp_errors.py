from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional, Sequence

from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.observability.request_context import get_correlation_id, get_request_id


def _tool_error_fingerprint(
    *,
    operation: str,
    code: ErrorCode,
    message: str,
    detail: str,
    context: Optional[Dict[str, Any]],
) -> str:
    payload = {
        "operation": str(operation or "unknown"),
        "code": code.value,
        "message": str(message),
        "detail": str(detail),
        "context": context or {},
    }
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str).encode("utf-8", errors="replace")
    return hashlib.sha256(encoded).hexdigest()[:20]


def tool_error(
    message: str,
    *,
    detail: Optional[str] = None,
    status_code: int = 400,
    code: ErrorCode = ErrorCode.REQUEST_VALIDATION_FAILED,
    category: ErrorCategory = ErrorCategory.INPUT,
    external_code: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    operation: Optional[str] = None,
    diagnostics: Optional[Dict[str, Any]] = None,
    fingerprint: Optional[str] = None,
) -> Dict[str, Any]:
    operation_final = str(operation or (context or {}).get("operation") or "tool_operation")
    detail_text = str(detail) if detail is not None else str(message)
    fingerprint_final = fingerprint or _tool_error_fingerprint(
        operation=operation_final,
        code=code,
        message=str(message),
        detail=detail_text,
        context=context,
    )
    diag_payload: Dict[str, Any] = {
        "request_id": get_request_id(),
        "correlation_id": get_correlation_id(),
    }
    if diagnostics:
        diag_payload.update({str(k): v for k, v in diagnostics.items()})

    merged_context: Dict[str, Any] = dict(context or {})
    merged_context.setdefault("operation", operation_final)
    merged_context.setdefault("fingerprint", fingerprint_final)

    payload = build_error_envelope(
        service_name="pipeline_mcp_server",
        message=str(message),
        detail=detail_text,
        code=code,
        category=category,
        status_code=status_code,
        external_code=external_code,
        context=merged_context,
        prefer_status_code=True,
    )
    # Many tool consumers (agent loop) treat `error` as the canonical signal for failure.
    payload["error"] = str(message)
    payload["tool_error"] = {
        "operation": operation_final,
        "fingerprint": fingerprint_final,
        "diagnostics": diag_payload,
    }
    payload["operation"] = operation_final
    payload["fingerprint"] = fingerprint_final
    return payload


def missing_required_params(
    tool_name: str,
    required: Sequence[str],
    arguments: Dict[str, Any],
) -> Dict[str, Any]:
    required_list = [str(item).strip() for item in required if str(item).strip()]
    missing = [key for key in required_list if not arguments.get(key)]
    if not missing:
        missing = required_list

    payload = tool_error(
        f"Missing required parameters for tool '{tool_name}': {', '.join(missing)}",
        status_code=400,
        code=ErrorCode.REQUEST_VALIDATION_FAILED,
        category=ErrorCategory.INPUT,
        context={"tool": tool_name, "missing": missing},
        operation=f"{tool_name}.validate",
    )
    payload["tool"] = tool_name
    payload["missing"] = missing
    payload["hint"] = "Provide all required parameters with non-empty values."
    return payload
