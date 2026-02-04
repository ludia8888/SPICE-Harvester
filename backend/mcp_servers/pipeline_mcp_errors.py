from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode


def tool_error(
    message: str,
    *,
    detail: Optional[str] = None,
    status_code: int = 400,
    code: ErrorCode = ErrorCode.REQUEST_VALIDATION_FAILED,
    category: ErrorCategory = ErrorCategory.INPUT,
    external_code: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload = build_error_envelope(
        service_name="pipeline_mcp_server",
        message=str(message),
        detail=str(detail) if detail is not None else str(message),
        code=code,
        category=category,
        status_code=status_code,
        external_code=external_code,
        context=context,
        prefer_status_code=True,
    )
    # Many tool consumers (agent loop) treat `error` as the canonical signal for failure.
    payload["error"] = str(message)
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
    )
    payload["tool"] = tool_name
    payload["missing"] = missing
    payload["hint"] = "Provide all required parameters with non-empty values."
    return payload

