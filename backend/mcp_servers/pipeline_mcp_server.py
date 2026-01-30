#!/usr/bin/env python3
"""
Pipeline MCP Server

Exposes deterministic "plan builder" + profiling/preview utilities as MCP tools,
so an internal planner can assemble PipelinePlan artifacts via tool calls.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from mcp.server import InitializationOptions, Server
from mcp.server.stdio import stdio_server
from mcp.types import ServerCapabilities, Tool, ToolsCapability

# Import paths depend on whether we run from source (repo layout) or from a container image.
# - repo layout: <repo>/backend/mcp_servers -> add <repo>/backend
# - container layout: /app/backend/mcp_servers -> add /app (bff lives at /app/bff)
_this_file = Path(__file__).resolve()
_backend_root = _this_file.parents[1]
_repo_root = _this_file.parents[2] if len(_this_file.parents) > 2 else _backend_root
for _path in (str(_backend_root), str(_repo_root)):
    if _path and _path not in sys.path:
        sys.path.append(_path)

from bff.services.pipeline_join_evaluator import evaluate_pipeline_joins  # noqa: E402
from bff.services.pipeline_plan_validation import validate_pipeline_plan  # noqa: E402
from shared.models.pipeline_plan import PipelinePlan  # noqa: E402
from shared.config.settings import get_settings  # noqa: E402
from shared.services.registries.dataset_profile_registry import DatasetProfileRegistry  # noqa: E402
from shared.services.registries.dataset_registry import DatasetRegistry  # noqa: E402
from shared.services.registries.pipeline_registry import PipelineRegistry  # noqa: E402
from shared.services.registries.objectify_registry import ObjectifyRegistry  # noqa: E402
from shared.models.objectify_job import ObjectifyJob  # noqa: E402
from shared.services.pipeline.pipeline_executor import PipelineExecutor  # noqa: E402
from shared.services.pipeline.pipeline_preview_inspector import inspect_preview  # noqa: E402
from shared.services.pipeline.pipeline_preview_policy import evaluate_preview_policy  # noqa: E402
from shared.services.pipeline.pipeline_plan_builder import (  # noqa: E402
    PipelinePlanBuilderError,
    add_edge,
    add_cast,
    add_compute,
    add_compute_assignments,
    add_compute_column,
    add_dedupe,
    add_drop,
    add_explode,
    add_filter,
    add_input,
    add_external_input,
    add_join,
    add_group_by_expr,
    add_normalize,
    add_output,
    add_pivot,
    add_rename,
    add_regex_replace,
    add_select,
    add_select_expr,
    add_sort,
    add_transform,
    add_union,
    add_window_expr,
    configure_input_read,
    delete_edge,
    delete_node,
    new_plan,
    reset_plan,
    set_node_inputs,
    update_settings,
    update_node_metadata,
    update_output,
    validate_structure,
)
from shared.services.pipeline.pipeline_claim_refuter import refute_pipeline_plan_claims  # noqa: E402
from shared.services.pipeline.pipeline_type_inference import (  # noqa: E402
    common_join_key_type,
    infer_xsd_type_with_confidence,
    normalize_declared_type,
)
from shared.errors.error_envelope import build_error_envelope  # noqa: E402
from shared.errors.error_types import ErrorCategory, ErrorCode  # noqa: E402
from shared.utils.llm_safety import mask_pii  # noqa: E402

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _tool_error(
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


def _bff_api_base_url() -> str:
    """
    Internal helper for MCP tools that need to call the BFF's REST API.

    In docker-compose, `services.bff_base_url` typically resolves to `http://bff:8002`.
    In local dev, it resolves to `http://127.0.0.1:8002`.
    """
    base = get_settings().services.bff_base_url.rstrip("/")
    return f"{base}/api/v1"


def _bff_admin_token() -> Optional[str]:
    # Reuse the same fallback chain as other internal clients.
    return (get_settings().clients.bff_admin_token or "").strip() or None


def _bff_headers(
    *,
    db_name: str,
    principal_id: Optional[str],
    principal_type: Optional[str],
) -> Dict[str, str]:
    token = _bff_admin_token()
    if not token:
        raise RuntimeError("BFF admin token unavailable (set BFF_ADMIN_TOKEN or ADMIN_TOKEN)")

    headers: Dict[str, str] = {
        "X-Admin-Token": token,
        "Content-Type": "application/json",
    }
    db_name = str(db_name or "").strip()
    if db_name:
        headers["X-DB-Name"] = db_name
        headers["X-Project"] = db_name

    pid = (principal_id or "").strip() or None
    ptype = (principal_type or "").strip().lower() or None
    if pid:
        headers["X-Principal-Id"] = pid
        headers["X-User-ID"] = pid
        headers["X-Actor"] = pid
    if ptype in {"user", "service"}:
        headers["X-Principal-Type"] = ptype
        headers["X-Actor-Type"] = ptype
    return headers


async def _bff_json(
    method: str,
    path: str,
    *,
    db_name: str,
    principal_id: Optional[str],
    principal_type: Optional[str],
    json_body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout_seconds: float = 30.0,
) -> Dict[str, Any]:
    base = _bff_api_base_url()
    url = f"{base}{path}"
    headers = _bff_headers(db_name=db_name, principal_id=principal_id, principal_type=principal_type)
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        resp = await client.request(method, url, headers=headers, json=json_body, params=params)
    try:
        payload = resp.json()
    except Exception:
        payload = {"raw": (resp.text or "").strip()}
    if resp.status_code >= 400:
        detail = payload.get("detail") if isinstance(payload, dict) else None
        message = payload.get("message") if isinstance(payload, dict) else None
        return {
            "error": message or detail or f"BFF {method} {path} failed ({resp.status_code})",
            "status_code": resp.status_code,
            "response": payload,
        }
    return payload if isinstance(payload, dict) else {"response": payload}


def _oms_api_base_url() -> str:
    """Get OMS API base URL from environment."""
    return os.getenv("OMS_BASE_URL", "http://oms:8000").rstrip("/")


async def _oms_json(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout_seconds: float = 30.0,
) -> Dict[str, Any]:
    """Make an HTTP request to OMS API and return JSON response."""
    base = _oms_api_base_url()
    url = f"{base}{path}"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        resp = await client.request(method, url, headers=headers, json=json_body, params=params)
    try:
        payload = resp.json()
    except Exception:
        payload = {"raw": (resp.text or "").strip()}
    if resp.status_code >= 400:
        detail = payload.get("detail") if isinstance(payload, dict) else None
        message = payload.get("message") if isinstance(payload, dict) else None
        return {
            "error": message or detail or f"OMS {method} {path} failed ({resp.status_code})",
            "status_code": resp.status_code,
            "response": payload,
        }
    return payload if isinstance(payload, dict) else {"response": payload}


# ==================== Trimming Constants ====================
# These control how much data is preserved in tool responses.
# Consistent limits help LLMs understand data without context overflow.
TRIM_PREVIEW_ROWS = 10  # Rows in preview results
TRIM_BUILD_OUTPUT_ROWS = 8  # Rows per output in build results
TRIM_BUILD_MAX_OUTPUTS = 10  # Max number of outputs in build results
TRIM_MAX_WARNINGS = 50  # Max warnings to return
TRIM_MAX_ERRORS = 50  # Max errors to return


def _trim_preview_payload(preview: Dict[str, Any], *, max_rows: int = TRIM_PREVIEW_ROWS) -> Dict[str, Any]:
    if not isinstance(preview, dict):
        return {}
    output = dict(preview)
    rows = output.get("rows")
    if isinstance(rows, list):
        output["rows"] = rows[: max(0, int(max_rows))]
    return output


def _trim_build_output(output_json: Dict[str, Any], *, max_rows: int = TRIM_BUILD_OUTPUT_ROWS) -> Dict[str, Any]:
    if not isinstance(output_json, dict):
        return {}
    out: Dict[str, Any] = {k: v for k, v in output_json.items() if k not in {"outputs"}}
    outputs = output_json.get("outputs")
    if not isinstance(outputs, list):
        return out
    trimmed_outputs: List[Dict[str, Any]] = []
    for item in outputs[:TRIM_BUILD_MAX_OUTPUTS]:
        if not isinstance(item, dict):
            continue
        rows = item.get("rows")
        trimmed = {
            "node_id": item.get("node_id"),
            "dataset_name": item.get("dataset_name") or item.get("datasetName"),
            "row_count": item.get("row_count"),
            "delta_row_count": item.get("delta_row_count") or item.get("deltaRowCount"),
            "columns": item.get("columns"),
            "sample_row_count": item.get("sample_row_count") or item.get("sampleRowCount"),
            "artifact_key": item.get("artifact_key") or item.get("artifactKey"),
            "artifact_prefix": item.get("artifact_prefix") or item.get("artifactPrefix"),
        }
        if isinstance(rows, list):
            trimmed["rows"] = rows[: max(0, int(max_rows))]
        trimmed_outputs.append(trimmed)
    out["outputs"] = trimmed_outputs
    return out


# ==============================================================================
# Enterprise Enhancement (2026-01): MCP Tool Safety
# ==============================================================================

class _ToolCallRateLimiter:
    """
    Simple rate limiter for MCP tool calls to prevent runaway Agent loops.
    Tracks calls per tool and total calls within time windows.
    """

    def __init__(
        self,
        *,
        max_calls_per_minute: int = 60,
        max_calls_per_tool_per_minute: int = 30,
    ):
        self._max_total = max_calls_per_minute
        self._max_per_tool = max_calls_per_tool_per_minute
        self._call_times: List[float] = []
        self._tool_call_times: Dict[str, List[float]] = {}
        import time
        self._time = time

    def check_and_record(self, tool_name: str) -> Optional[str]:
        """
        Check if the call is allowed and record it.
        Returns None if allowed, or an error message if rate limited.
        """
        now = self._time.time()
        minute_ago = now - 60

        # Clean old entries
        self._call_times = [t for t in self._call_times if t > minute_ago]

        if tool_name not in self._tool_call_times:
            self._tool_call_times[tool_name] = []
        self._tool_call_times[tool_name] = [
            t for t in self._tool_call_times[tool_name] if t > minute_ago
        ]

        # Check total rate
        if len(self._call_times) >= self._max_total:
            return (
                f"RATE LIMIT: Total MCP tool calls exceeded {self._max_total}/minute. "
                "This may indicate an Agent loop. Wait before retrying or check Agent logic."
            )

        # Check per-tool rate
        if len(self._tool_call_times[tool_name]) >= self._max_per_tool:
            return (
                f"RATE LIMIT: Tool '{tool_name}' called {self._max_per_tool}+ times/minute. "
                "Consider batching operations or using a different approach."
            )

        # Record the call
        self._call_times.append(now)
        self._tool_call_times[tool_name].append(now)
        return None


# Global rate limiter instance
_rate_limiter = _ToolCallRateLimiter()


def _validate_required_params(
    arguments: Dict[str, Any],
    required: List[str],
    tool_name: str,
) -> Optional[Dict[str, Any]]:
    """
    Validate required parameters and return a helpful error response if missing.
    Returns None if all required params are present, or an error dict otherwise.
    """
    missing = []
    for param in required:
        value = arguments.get(param)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing.append(param)

    if not missing:
        return None

    return {
        "error": f"Missing required parameter(s): {', '.join(missing)}",
        "tool": tool_name,
        "required_params": required,
        "received_params": list(arguments.keys()),
        "hint": f"Call {tool_name} with all required parameters: {required}",
    }


def _build_tool_error_response(
    tool_name: str,
    error: Exception,
    *,
    arguments: Optional[Dict[str, Any]] = None,
    hint: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Enterprise Enhancement: Build a helpful error response for MCP tool failures.
    Includes context about what went wrong and how to fix it.
    """
    error_type = type(error).__name__
    error_msg = str(error)

    response: Dict[str, Any] = {
        "error": error_msg,
        "error_type": error_type,
        "tool": tool_name,
    }

    # Add specific hints based on error type
    if "required" in error_msg.lower():
        response["hint"] = "Check that all required parameters are provided with non-empty values"
    elif "not found" in error_msg.lower():
        response["hint"] = "Verify that the referenced ID exists. Use appropriate list/search tools first."
    elif "invalid" in error_msg.lower():
        response["hint"] = "Check parameter format and allowed values"
    elif isinstance(error, PipelinePlanBuilderError):
        response["hint"] = "This is a plan structure error. Review the plan and fix the identified issue."
    elif hint:
        response["hint"] = hint

    # Include relevant arguments for debugging (mask sensitive values)
    if arguments:
        safe_args = {
            k: v if k not in ("token", "password", "secret", "key") else "***"
            for k, v in arguments.items()
        }
        response["arguments_received"] = safe_args

    return response


def _normalize_string_list(value: Any) -> List[str]:
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    out: List[str] = []
    for item in items:
        text = str(item or "").strip()
        if text:
            out.append(text)
    return out


def _normalize_aggregates(value: Any) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Normalize aggregates list and return (aggregates, warnings)."""
    raw = value if isinstance(value, list) else []
    out: List[Dict[str, Any]] = []
    warnings: List[str] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            warnings.append(f"aggregates[{idx}]: skipped non-dict item")
            continue
        column = str(item.get("column") or "").strip()
        op = str(item.get("op") or item.get("function") or item.get("agg") or "").strip().lower()
        if not column:
            warnings.append(f"aggregates[{idx}]: skipped item with missing 'column'")
            continue
        if not op:
            warnings.append(f"aggregates[{idx}]: skipped item with missing 'op' (column={column})")
            continue
        alias = str(item.get("alias") or "").strip() or None
        payload: Dict[str, Any] = {"column": column, "op": op}
        if alias:
            payload["alias"] = alias
        out.append(payload)
    return out, warnings


def _extract_spark_error_details(run: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract error details from a pipeline run's output_json.
    Returns a dict with error_summary, errors list, and optional stack trace.
    """
    output_json = run.get("output_json") if isinstance(run.get("output_json"), dict) else {}

    errors: List[str] = []
    raw_errors = output_json.get("errors")
    if isinstance(raw_errors, list):
        errors = [str(item).strip() for item in raw_errors if str(item).strip()]

    error_summary = output_json.get("error") or output_json.get("error_message") or ""
    if isinstance(error_summary, dict):
        error_summary = error_summary.get("message") or str(error_summary)

    # Extract stack trace if available
    stack_trace = output_json.get("stack_trace") or output_json.get("traceback") or ""

    # Extract exception type if available
    exception_type = output_json.get("exception_type") or output_json.get("error_type") or ""

    result: Dict[str, Any] = {}
    if errors:
        result["errors"] = errors
    if error_summary:
        result["error_summary"] = str(error_summary)[:500]
    if exception_type:
        result["exception_type"] = str(exception_type)
    if stack_trace:
        result["stack_trace"] = str(stack_trace)[:2000]

    # If no errors extracted but status is FAILED, add a generic message
    if not result:
        result["error_summary"] = "Job failed (no detailed error message available)"

    return result


class PipelineMCPServer:
    def __init__(self) -> None:
        self.server = Server("pipeline-mcp-server")
        self._dataset_registry: Optional[DatasetRegistry] = None
        self._profile_registry: Optional[DatasetProfileRegistry] = None
        self._pipeline_registry: Optional[PipelineRegistry] = None
        self._objectify_registry: Optional[ObjectifyRegistry] = None
        self.websocket_service: Optional[Any] = None  # For schema drift broadcasts
        self._setup_handlers()

    async def _ensure_registries(self) -> tuple[DatasetRegistry, DatasetProfileRegistry]:
        if self._dataset_registry is None:
            self._dataset_registry = DatasetRegistry()
            await self._dataset_registry.initialize()
        if self._profile_registry is None:
            self._profile_registry = DatasetProfileRegistry()
            await self._profile_registry.initialize()
        return self._dataset_registry, self._profile_registry

    async def _ensure_pipeline_registry(self) -> PipelineRegistry:
        if self._pipeline_registry is None:
            self._pipeline_registry = PipelineRegistry()
            await self._pipeline_registry.initialize()
        return self._pipeline_registry

    async def _ensure_objectify_registry(self) -> ObjectifyRegistry:
        if self._objectify_registry is None:
            self._objectify_registry = ObjectifyRegistry()
            await self._objectify_registry.initialize()
        return self._objectify_registry

    async def _ensure_websocket_service(self) -> Optional[Any]:
        """Lazy-init WebSocket service for schema drift broadcasts."""
        if self.websocket_service is None:
            try:
                from shared.services.storage.redis_service import create_redis_service
                from shared.services.core.websocket_service import get_notification_service
                redis_service = create_redis_service()
                self.websocket_service = get_notification_service(redis_service)
            except Exception as exc:
                logger.warning("WebSocket service unavailable for MCP: %s", exc)
        return self.websocket_service

    def _setup_handlers(self) -> None:
        @self.server.list_tools()
        async def list_tools() -> List[Tool]:
            tool_specs: List[Dict[str, Any]] = [
                {
                    "name": "plan_new",
                    "description": "Create a new PipelinePlan JSON (empty nodes/edges).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "goal": {"type": "string"},
                            "db_name": {"type": "string"},
                            "branch": {"type": "string"},
                            "dataset_ids": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["goal", "db_name"],
                    },
                },
                {
                    "name": "plan_reset",
                    "description": "Reset an existing plan to empty (preserves goal + data_scope).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                        },
                        "required": ["plan"],
                    },
                },
                {
                    "name": "plan_add_input",
                    "description": "Add an input node for a dataset.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "dataset_id": {"type": "string"},
                            "dataset_name": {"type": "string"},
                            "dataset_branch": {"type": "string"},
                            "read": {"type": "object", "description": "Spark read config for this input (format/options/schema)."},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan"],
                    },
                },
                {
                    "name": "plan_add_external_input",
                    "description": "Add an input node backed by a Spark-native source (jdbc/kafka/file URI) via metadata.read.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "read": {
                                "type": "object",
                                "description": "Spark read config (must include format + options/path).",
                            },
                            "source_name": {"type": "string"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "read"],
                    },
                },
                {
                    "name": "plan_configure_input_read",
                    "description": "Patch an input node's Spark read config (format/options/schema, permissive parsing, corrupt record capture).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "node_id": {"type": "string"},
                            "read": {"type": "object"},
                            "format": {"type": "string"},
                            "options": {"type": "object"},
                            "options_env": {
                                "type": "object",
                                "description": "Map Spark option key -> env var name (avoid embedding secrets in the plan).",
                            },
                            "schema": {"type": "array", "items": {"type": "object"}},
                            "mode": {"type": "string", "description": "Spark reader mode: PERMISSIVE | DROPMALFORMED | FAILFAST"},
                            "corrupt_record_column": {"type": "string", "description": "Sets columnNameOfCorruptRecord to capture malformed rows"},
                            "header": {"type": "boolean"},
                            "infer_schema": {"type": "boolean"},
                            "replace": {"type": "boolean"},
                        },
                        "required": ["plan", "node_id"],
                    },
                },
                {
                    "name": "plan_add_transform",
                    "description": "Add a generic transform node (operation + metadata) with edges from input_node_ids.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "operation": {"type": "string"},
                            "input_node_ids": {"type": "array", "items": {"type": "string"}},
                            "metadata": {"type": "object"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "operation", "input_node_ids"],
                    },
                },
                {
                    "name": "plan_add_sort",
                    "description": "Add a sort transform node. columns supports ['col','-col2'] or [{'column','direction'}].",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "columns": {"type": "array"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "columns"],
                    },
                },
                {
                    "name": "plan_add_explode",
                    "description": "Add an explode transform node for an array/map-like column (replaces column with exploded elements).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "column": {"type": "string"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "column"],
                    },
                },
                {
                    "name": "plan_add_union",
                    "description": "Add a union transform node for two inputs (unionByName). union_mode: strict|common_only|pad_missing_nulls|pad.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "left_node_id": {"type": "string"},
                            "right_node_id": {"type": "string"},
                            "union_mode": {"type": "string"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "left_node_id", "right_node_id"],
                    },
                },
                {
                    "name": "plan_add_pivot",
                    "description": "Add a pivot transform node (groupBy(index...).pivot(columns).agg(values)).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "index": {"type": "array", "items": {"type": "string"}},
                            "columns": {"type": "string"},
                            "values": {"type": "string"},
                            "agg": {"type": "string"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "index", "columns", "values"],
                    },
                },
                {
                    "name": "plan_add_group_by",
                    "description": "Add a groupBy/aggregate transform node (group_by + aggregates). aggregates items: {column,op,alias?}.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "group_by": {"type": "array", "items": {"type": "string"}},
                            "aggregates": {"type": "array", "items": {"type": "object"}},
                            "operation": {"type": "string", "description": "groupBy (default) or aggregate"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "aggregates"],
                    },
                },
                {
                    "name": "plan_add_group_by_expr",
                    "description": "Add a groupBy/aggregate node using Spark SQL aggregate expressions (supports approx_percentile, etc).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "group_by": {"type": "array", "items": {"type": "string"}},
                            "aggregate_expressions": {"type": "array", "items": {}},
                            "operation": {"type": "string", "description": "groupBy (default) or aggregate"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "aggregate_expressions"],
                    },
                },
                {
                    "name": "plan_add_window",
                    "description": "Add a window transform node. order_by supports ['-col'] for DESC or [{'column','direction'}].",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "partition_by": {"type": "array", "items": {"type": "string"}},
                            "order_by": {"type": "array"},
                            "window": {"type": "object"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id"],
                    },
                },
                {
                    "name": "plan_add_window_expr",
                    "description": "Add a window transform node computing one or more Spark SQL window expressions.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "expressions": {"type": "array", "items": {"type": "object"}},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "expressions"],
                    },
                },
                {
                    "name": "plan_add_join",
                    "description": "Add a join transform node (LEFT then RIGHT edge order). Cross joins are rejected. IMPORTANT: join_type is required - specify 'inner', 'left', 'right', 'full', or 'cross'.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "left_node_id": {"type": "string"},
                            "right_node_id": {"type": "string"},
                            "left_keys": {"type": "array", "items": {"type": "string"}},
                            "right_keys": {"type": "array", "items": {"type": "string"}},
                            "join_type": {"type": "string", "enum": ["inner", "left", "right", "full", "cross"], "description": "REQUIRED: Type of join operation"},
                            "join_hints": {"type": "object", "description": "Optional Spark join hints: {left: 'broadcast', right: 'broadcast'}"},
                            "broadcast_left": {"type": "boolean"},
                            "broadcast_right": {"type": "boolean"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "left_node_id", "right_node_id", "left_keys", "right_keys", "join_type"],
                    },
                },
                {
                    "name": "plan_add_filter",
                    "description": "Add a filter transform node.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "expression": {"type": "string"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "expression"],
                    },
                },
                {
                    "name": "plan_add_compute",
                    "description": "Add a compute transform node (expression).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "expression": {"type": "string"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "expression"],
                    },
                },
                {
                    "name": "plan_add_compute_column",
                    "description": "Add a compute transform node that writes target_column = formula (avoids '=' ambiguity).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "target_column": {"type": "string"},
                            "formula": {"type": "string"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "target_column", "formula"],
                    },
                },
                {
                    "name": "plan_add_compute_assignments",
                    "description": "Add a compute transform node that writes multiple columns. assignments=[{column,expression}].",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "assignments": {"type": "array", "items": {"type": "object"}},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "assignments"],
                    },
                },
                {
                    "name": "plan_add_cast",
                    "description": "Add a cast transform node. casts=[{column,type}].",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "casts": {"type": "array", "items": {"type": "object"}},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "casts"],
                    },
                },
                {
                    "name": "plan_add_rename",
                    "description": "Add a rename transform node. Prefer rename={src:dst}, but renames/mappings as [{from,to}] are also accepted.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            # Accept common LLM shapes:
                            # - rename={"old":"new"}
                            # - rename=[{"from":"old","to":"new"}]
                            # - renames/mappings=[{"from":"old","to":"new"}]
                            "rename": {
                                "oneOf": [
                                    {"type": "object"},
                                    {"type": "array", "items": {"type": "object"}},
                                ]
                            },
                            "renames": {"type": "array", "items": {"type": "object"}},
                            "mappings": {"type": "array", "items": {"type": "object"}},
                            "node_id": {"type": "string"},
                        },
                        # Require at least one mapping field; `plan` and `input_node_id` are always required.
                        "required": ["plan", "input_node_id"],
                        "anyOf": [
                            {"required": ["rename"]},
                            {"required": ["renames"]},
                            {"required": ["mappings"]},
                        ],
                    },
                },
                {
                    "name": "plan_add_select",
                    "description": "Add a select transform node.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "columns": {"type": "array", "items": {"type": "string"}},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "columns"],
                    },
                },
                {
                    "name": "plan_add_select_expr",
                    "description": "Add a select transform node using Spark selectExpr expressions.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "expressions": {"type": "array", "items": {"type": "string"}},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "expressions"],
                    },
                },
                {
                    "name": "plan_add_drop",
                    "description": "Add a drop transform node.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "columns": {"type": "array", "items": {"type": "string"}},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "columns"],
                    },
                },
                {
                    "name": "plan_add_dedupe",
                    "description": "Add a dedupe transform node.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "columns": {"type": "array", "items": {"type": "string"}},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "columns"],
                    },
                },
                {
                    "name": "plan_add_normalize",
                    "description": "Add a normalize transform node. WARNING: Default behavior modifies data (trim=true, empty_to_null=true, whitespace_to_null=true). Set these to false explicitly if you want to preserve original values.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "columns": {"type": "array", "items": {"type": "string"}},
                            "trim": {"type": "boolean", "default": True, "description": "Remove leading/trailing whitespace (default: true)"},
                            "empty_to_null": {"type": "boolean", "default": True, "description": "Convert empty strings to null (default: true)"},
                            "whitespace_to_null": {"type": "boolean", "default": True, "description": "Convert whitespace-only strings to null (default: true)"},
                            "lowercase": {"type": "boolean", "default": False, "description": "Convert to lowercase (default: false)"},
                            "uppercase": {"type": "boolean", "default": False, "description": "Convert to uppercase (default: false)"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "columns"],
                    },
                },
                {
                    "name": "plan_add_regex_replace",
                    "description": "Add a regexReplace transform node. rules=[{column,pattern,replacement,flags?}].",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "rules": {"type": "array", "items": {"type": "object"}},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "rules"],
                    },
                },
                {
                    "name": "plan_add_output",
                    "description": "Add an output node + outputs[] entry.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "output_name": {"type": "string"},
                            "output_kind": {"type": "string"},
                            "node_id": {"type": "string"},
                            "output_metadata": {"type": "object"},
                        },
                        "required": ["plan", "input_node_id", "output_name"],
                    },
                },
                {
                    "name": "plan_add_edge",
                    "description": "Add an edge from->to (idempotent). Incoming edge order can matter for joins.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "from_node_id": {"type": "string"},
                            "to_node_id": {"type": "string"},
                        },
                        "required": ["plan", "from_node_id", "to_node_id"],
                    },
                },
                {
                    "name": "plan_delete_edge",
                    "description": "Delete edges from->to (no-op if not found).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "from_node_id": {"type": "string"},
                            "to_node_id": {"type": "string"},
                        },
                        "required": ["plan", "from_node_id", "to_node_id"],
                    },
                },
                {
                    "name": "plan_set_node_inputs",
                    "description": "Replace all incoming edges to node_id with input_node_ids (in order).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "node_id": {"type": "string"},
                            "input_node_ids": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["plan", "node_id", "input_node_ids"],
                    },
                },
                {
                    "name": "plan_update_node_metadata",
                    "description": "Patch node.metadata (merge by default, replace if requested). Use `set` (aliases: `metadata`, `meta`).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "node_id": {"type": "string"},
                            "set": {"type": "object"},
                            "unset": {"type": "array", "items": {"type": "string"}},
                            "replace": {"type": "boolean"},
                        },
                        "required": ["plan", "node_id"],
                    },
                },
                {
                    "name": "plan_update_settings",
                    "description": "Patch plan.definition_json.settings (merge by default, replace if requested). Use `set` (alias: `settings`).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "set": {"type": "object"},
                            "unset": {"type": "array", "items": {"type": "string"}},
                            "replace": {"type": "boolean"},
                        },
                        "required": ["plan"],
                    },
                },
                {
                    "name": "plan_delete_node",
                    "description": "Delete a node and any incident edges; for output nodes also removes outputs[] entry.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "node_id"],
                    },
                },
                {
                    "name": "plan_update_output",
                    "description": "Patch outputs[] entry (by output_name or output node_id); keeps output node metadata.outputName in sync if renamed.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "output_name": {"type": "string"},
                            "node_id": {"type": "string"},
                            "set": {"type": "object"},
                            "unset": {"type": "array", "items": {"type": "string"}},
                            "replace": {"type": "boolean"},
                        },
                        "required": ["plan"],
                    },
                },
                {
                    "name": "plan_validate_structure",
                    "description": "Validate plan.definition_json structure without resolving dataset schemas.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {"plan": {"type": "object"}},
                        "required": ["plan"],
                    },
                },
                {
                    "name": "plan_validate",
                    "description": "Validate a PipelinePlan using dataset-aware preflight rules.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "require_output": {"type": "boolean"},
                        },
                        "required": ["plan"],
                    },
                },
                {
                    "name": "plan_preview",
                    "description": "Preview a plan via the deterministic PipelineExecutor (sample-safe). Note: limit is capped at 200 rows. For larger previews, use pipeline_preview_wait.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "node_id": {"type": "string"},
                            "limit": {"type": "integer", "default": 50, "minimum": 1, "maximum": 200, "description": "Max rows to preview (default: 50, max: 200)"},
                        },
                        "required": ["plan"],
                    },
                },
                {
                    "name": "plan_refute_claims",
                    "description": "Refute plan-embedded claims with concrete counterexamples (witness-based hard gate). Note: Stops after max_hard_failures (default: 5) or max_soft_warnings (default: 20).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "sample_limit": {"type": "integer", "default": 400, "description": "Sample limit for claim checking (default: 400)"},
                            "max_output_rows": {"type": "integer", "default": 20000, "description": "Max output rows (default: 20000)"},
                            "max_hard_failures": {"type": "integer", "default": 5, "description": "Stop after this many hard failures (default: 5)"},
                            "max_soft_warnings": {"type": "integer", "default": 20, "description": "Stop after this many soft warnings (default: 20)"},
                            "run_tables": {"type": "object"},
                        },
                        "required": ["plan"],
                    },
                },
                {
                    "name": "preview_inspect",
                    "description": "Inspect a preview sample and propose cleansing suggestions (deterministic).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "preview": {"type": "object"},
                        },
                        "required": ["preview"],
                    },
                },
                {
                    "name": "plan_evaluate_joins",
                    "description": "Evaluate join nodes in a plan (coverage/explosion) using sample-safe execution.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "node_filter": {"type": "string"},
                            "run_tables": {"type": "object"},
                        },
                        "required": ["plan"],
                    },
                },
                {
                    "name": "pipeline_create_from_plan",
                    "description": "Create a Pipeline (control plane) from a PipelinePlan.definition_json. If the pipeline already exists (same db/name/branch), this tool will update it and return the existing pipeline_id (idempotent upsert). Requires admin token; respects principal headers for permissions.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "name": {"type": "string"},
                            "location": {"type": "string"},
                            "description": {"type": "string"},
                            "pipeline_type": {"type": "string"},
                            "branch": {"type": "string"},
                            "pipeline_id": {"type": "string"},
                            "principal_id": {"type": "string"},
                            "principal_type": {"type": "string"},
                        },
                        "required": ["plan", "name", "location"],
                    },
                },
                {
                    "name": "pipeline_update_from_plan",
                    "description": "Update an existing Pipeline definition from a PipelinePlan.definition_json.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "pipeline_id": {"type": "string"},
                            "plan": {"type": "object"},
                            "branch": {"type": "string"},
                            "principal_id": {"type": "string"},
                            "principal_type": {"type": "string"},
                        },
                        "required": ["pipeline_id", "plan"],
                    },
                },
                {
                    "name": "pipeline_preview_wait",
                    "description": "Queue a Spark preview for a Pipeline and optionally wait (poll) until completion. If job_id is provided, this tool will poll that existing job_id without enqueuing a new preview (prevents runaway QUEUED runs). Note: limit is capped at 500 rows.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "pipeline_id": {"type": "string"},
                            "db_name": {"type": "string"},
                            "node_id": {"type": "string"},
                            "limit": {"type": "integer", "default": 200, "minimum": 1, "maximum": 500, "description": "Max rows to preview (default: 200, max: 500)"},
                            "branch": {"type": "string"},
                            "job_id": {"type": "string", "description": "Existing preview job_id to poll (skip enqueue)."},
                            "force": {"type": "boolean", "description": "Force enqueue a new preview even if a matching job is already running."},
                            "wait": {"type": "boolean"},
                            "timeout_seconds": {"type": "number"},
                            "poll_interval_seconds": {"type": "number"},
                            "principal_id": {"type": "string"},
                            "principal_type": {"type": "string"},
                        },
                        "required": ["pipeline_id"],
                    },
                },
                {
                    "name": "pipeline_build_wait",
                    "description": "Queue a Spark build for a Pipeline and optionally wait (poll) until completion. If job_id is provided, this tool will poll that existing job_id without enqueuing a new build (prevents runaway QUEUED runs). Note: limit is capped at 500 rows.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "pipeline_id": {"type": "string"},
                            "db_name": {"type": "string"},
                            "node_id": {"type": "string"},
                            "limit": {"type": "integer", "default": 200, "minimum": 1, "maximum": 500, "description": "Max rows in output sample (default: 200, max: 500)"},
                            "branch": {"type": "string"},
                            "job_id": {"type": "string", "description": "Existing build job_id to poll (skip enqueue)."},
                            "force": {"type": "boolean", "description": "Force enqueue a new build even if a matching job is already running."},
                            "wait": {"type": "boolean"},
                            "timeout_seconds": {"type": "number"},
                            "poll_interval_seconds": {"type": "number"},
                            "principal_id": {"type": "string"},
                            "principal_type": {"type": "string"},
                        },
                        "required": ["pipeline_id"],
                    },
                },
                {
                    "name": "pipeline_deploy_promote_build",
                    "description": "Promote a successful build to a deployed dataset (requires approve permission). To avoid build↔deploy definition hash mismatches, you may pass definition_json, or pass pipeline_spec_commit_id and this tool will fetch the exact build snapshot from pipeline_versions.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "pipeline_id": {"type": "string"},
                            "build_job_id": {"type": "string"},
                            "artifact_id": {"type": "string"},
                            "node_id": {"type": "string"},
                            "db_name": {"type": "string"},
                            "dataset_name": {"type": "string"},
                            "branch": {"type": "string"},
                            "replay_on_deploy": {"type": "boolean"},
                            "definition_json": {"type": "object", "description": "Optional exact pipeline definition_json to deploy (must match build)."},
                            "pipeline_spec_commit_id": {"type": "string", "description": "Optional pipeline version commit id used by the build; used to fetch definition_json snapshot automatically."},
                            "principal_id": {"type": "string"},
                            "principal_type": {"type": "string"},
                        },
                        "required": ["pipeline_id", "build_job_id", "node_id", "db_name", "dataset_name"],
                    },
                },
                # ── Dataset Lookup Tools ──────────────────────────────────────────
                {
                    "name": "dataset_get_by_name",
                    "description": "Look up a dataset by name and return its dataset_id. Use this when you have a dataset name but need the dataset_id for objectify or other tools.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string", "description": "Database name"},
                            "dataset_name": {"type": "string", "description": "Dataset name to look up"},
                            "branch": {"type": "string", "description": "Branch (default: main)"},
                        },
                        "required": ["db_name", "dataset_name"],
                    },
                },
                {
                    "name": "dataset_get_latest_version",
                    "description": "Get the latest version of a dataset. Returns version_id, artifact_key, and schema info.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "dataset_id": {"type": "string", "description": "Dataset ID"},
                        },
                        "required": ["dataset_id"],
                    },
                },
                {
                    "name": "dataset_validate_columns",
                    "description": "Validate that specified columns exist in a dataset's schema. Returns valid/invalid columns and available columns for suggestions. Use this before adding operations that reference specific columns.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "dataset_id": {"type": "string", "description": "Dataset ID to check"},
                            "columns": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of column names to validate",
                            },
                        },
                        "required": ["dataset_id", "columns"],
                    },
                },
                # ── Debugging Tools ──────────────────────────────────────────
                {
                    "name": "debug_get_errors",
                    "description": "Get accumulated errors and warnings from current pipeline run. Use this to see what went wrong.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "include_warnings": {"type": "boolean", "default": True, "description": "Include warnings in addition to errors"},
                            "limit": {"type": "integer", "default": 50, "description": "Maximum number of errors to return"},
                        },
                    },
                },
                {
                    "name": "debug_get_execution_log",
                    "description": "Get step-by-step execution log of tool calls made during this run. Useful for understanding what happened.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "limit": {"type": "integer", "default": 20, "description": "Maximum number of log entries to return"},
                            "step": {"type": "integer", "description": "Filter by specific step number"},
                        },
                    },
                },
                {
                    "name": "debug_inspect_node",
                    "description": "Inspect a specific node's configuration, inputs, and outputs in the pipeline plan.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object", "description": "The pipeline plan to inspect"},
                            "node_id": {"type": "string", "description": "ID of the node to inspect"},
                            "include_sample": {"type": "boolean", "default": False, "description": "Include sample data if available"},
                        },
                        "required": ["plan", "node_id"],
                    },
                },
                {
                    "name": "debug_explain_failure",
                    "description": "Analyze accumulated errors and provide diagnostic suggestions with potential fixes.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {},
                    },
                },
                {
                    "name": "debug_dry_run",
                    "description": "Validate a pipeline plan without actually executing it. Checks for structural issues, missing references, and join key compatibility.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object", "description": "The pipeline plan to validate"},
                            "check_joins": {"type": "boolean", "default": True, "description": "Validate join key compatibility"},
                        },
                        "required": ["plan"],
                    },
                },
                # ── Objectify Tools (Dataset → Ontology Instance Transformation) ──────────────────
                {
                    "name": "objectify_suggest_mapping",
                    "description": "Suggest field mappings from dataset columns to ontology class properties. Uses schema matching and naming heuristics. Call this before creating a mapping spec.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "dataset_id": {"type": "string", "description": "Dataset ID to map from"},
                            "target_class_id": {"type": "string", "description": "Target ontology class ID (e.g., 'Customer', 'Order')"},
                            "db_name": {"type": "string", "description": "Database name"},
                            "branch": {"type": "string", "description": "Ontology branch (default: main)"},
                        },
                        "required": ["dataset_id", "target_class_id", "db_name"],
                    },
                },
                {
                    "name": "objectify_create_mapping_spec",
                    "description": "Create a mapping specification that defines how dataset columns map to ontology class properties. Required before running objectify.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "dataset_id": {"type": "string", "description": "Dataset ID"},
                            "target_class_id": {"type": "string", "description": "Target ontology class ID"},
                            "mappings": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "source_field": {"type": "string"},
                                        "target_field": {"type": "string"},
                                    },
                                    "required": ["source_field", "target_field"],
                                },
                                "description": "List of column-to-property mappings",
                            },
                            "db_name": {"type": "string", "description": "Database name"},
                            "dataset_branch": {"type": "string", "description": "Dataset branch"},
                            "auto_sync": {"type": "boolean", "default": True, "description": "Auto-sync on new dataset versions"},
                            "options": {"type": "object", "description": "Additional options (ontology_branch, batch_size, max_rows)"},
                        },
                        "required": ["dataset_id", "target_class_id", "mappings", "db_name"],
                    },
                },
                {
                    "name": "objectify_list_mapping_specs",
                    "description": "List existing mapping specifications for a dataset.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "dataset_id": {"type": "string", "description": "Dataset ID"},
                            "db_name": {"type": "string", "description": "Database name"},
                            "limit": {"type": "integer", "default": 50, "description": "Max results (default 50)"},
                        },
                        "required": ["dataset_id"],
                    },
                },
                {
                    "name": "objectify_run",
                    "description": "Execute objectify transformation to convert dataset rows into ontology instances. Requires an active mapping spec.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "dataset_id": {"type": "string", "description": "Dataset ID"},
                            "mapping_spec_id": {"type": "string", "description": "Mapping spec ID (optional if dataset has active mapping)"},
                            "dataset_version_id": {"type": "string", "description": "Specific dataset version (optional, uses latest)"},
                            "db_name": {"type": "string", "description": "Database name"},
                            "max_rows": {"type": "integer", "description": "Max rows to process"},
                            "batch_size": {"type": "integer", "description": "Batch size for processing"},
                        },
                        "required": ["dataset_id", "db_name"],
                    },
                },
                {
                    "name": "objectify_get_status",
                    "description": "Get the status of an objectify job.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "job_id": {"type": "string", "description": "Objectify job ID"},
                        },
                        "required": ["job_id"],
                    },
                },
                {
                    "name": "objectify_wait",
                    "description": "Wait for an objectify job to complete. Polls the job status until completion, failure, or timeout. Returns final job status with results.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "job_id": {"type": "string", "description": "Objectify job ID to wait for"},
                            "timeout_seconds": {"type": "number", "default": 300, "description": "Max seconds to wait (default: 300)"},
                            "poll_interval_seconds": {"type": "number", "default": 2, "description": "Seconds between status checks (default: 2)"},
                        },
                        "required": ["job_id"],
                    },
                },
                {
                    "name": "ontology_register_object_type",
                    "description": "Register an ontology class as an object_type resource for objectify. REQUIRED before running objectify. Creates the object_type contract with pk_spec and backing_source configuration.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string", "description": "Database name"},
                            "class_id": {"type": "string", "description": "Ontology class ID (e.g., 'Customer')"},
                            "dataset_id": {"type": "string", "description": "Backing dataset ID for the object type"},
                            "primary_key": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Primary key field(s) (e.g., ['customer_id'])",
                            },
                            "title_key": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Title key field(s) - displayed as instance title (e.g., ['customer_id'])",
                            },
                            "branch": {"type": "string", "description": "Branch (default: main)"},
                        },
                        "required": ["db_name", "class_id", "dataset_id", "primary_key", "title_key"],
                    },
                },
                {
                    "name": "ontology_query_instances",
                    "description": "Query ontology instances by class type. Use this to verify objectify results by counting instances or retrieving sample data. Returns instance count and sample instances.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string", "description": "Database name"},
                            "class_id": {"type": "string", "description": "Class type to query (e.g., 'Customer', 'Order')"},
                            "limit": {"type": "integer", "default": 10, "description": "Max instances to return (default: 10, max: 100)"},
                            "branch": {"type": "string", "description": "Branch (default: main)"},
                            "filters": {
                                "type": "object",
                                "description": "Optional property filters (e.g., {\"status\": \"active\"})",
                            },
                        },
                        "required": ["db_name", "class_id"],
                    },
                },
                # ============================================================
                # Enterprise Features: FK Detection, Incremental Objectify, Schema Drift
                # ============================================================
                {
                    "name": "detect_foreign_keys",
                    "description": "Detect potential FK relationships in a dataset based on naming conventions and value overlap analysis. Returns detected FK patterns with confidence scores.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string", "description": "Database name"},
                            "dataset_id": {"type": "string", "description": "Dataset ID to analyze"},
                            "confidence_threshold": {"type": "number", "default": 0.6, "description": "Minimum confidence (0-1) to report"},
                            "include_sample_analysis": {"type": "boolean", "default": True, "description": "Include value overlap analysis"},
                            "branch": {"type": "string", "default": "main"},
                        },
                        "required": ["db_name", "dataset_id"],
                    },
                },
                {
                    "name": "create_link_type_from_fk",
                    "description": "Create a link_type from a detected FK pattern. Generates the relationship spec and link_type definition.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string", "description": "Database name"},
                            "fk_pattern": {
                                "type": "object",
                                "description": "FK pattern from detect_foreign_keys result",
                            },
                            "source_class_id": {"type": "string", "description": "Source ontology class ID"},
                            "target_class_id": {"type": "string", "description": "Target ontology class ID"},
                            "predicate": {"type": "string", "description": "Relationship predicate (e.g., 'hasCustomer')"},
                            "cardinality": {
                                "type": "string",
                                "enum": ["1:1", "1:n", "n:1", "n:m"],
                                "default": "n:1",
                            },
                            "branch": {"type": "string", "default": "main"},
                        },
                        "required": ["db_name", "fk_pattern", "source_class_id", "target_class_id"],
                    },
                },
                {
                    "name": "trigger_incremental_objectify",
                    "description": "Trigger objectify in incremental mode (watermark or delta). Processes only changed rows since last run.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string", "description": "Database name"},
                            "mapping_spec_id": {"type": "string", "description": "Mapping spec ID"},
                            "execution_mode": {
                                "type": "string",
                                "enum": ["full", "incremental", "delta"],
                                "default": "incremental",
                                "description": "Execution mode: full (all rows), incremental (watermark-based), delta (LakeFS diff)",
                            },
                            "watermark_column": {"type": "string", "description": "Column for watermark filtering (required for incremental mode)"},
                            "force_full_refresh": {"type": "boolean", "default": False, "description": "Reset watermark and run full refresh"},
                            "branch": {"type": "string", "default": "main"},
                        },
                        "required": ["db_name", "mapping_spec_id"],
                    },
                },
                {
                    "name": "get_objectify_watermark",
                    "description": "Get the current watermark state for a mapping spec. Shows last processed watermark value and timestamp.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "mapping_spec_id": {"type": "string", "description": "Mapping spec ID"},
                            "dataset_branch": {"type": "string", "default": "main"},
                        },
                        "required": ["mapping_spec_id"],
                    },
                },
                {
                    "name": "check_schema_drift",
                    "description": "Check if dataset schema has drifted from mapping spec expectations. Detects column additions, removals, type changes.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string", "description": "Database name"},
                            "mapping_spec_id": {"type": "string", "description": "Mapping spec ID to check"},
                            "dataset_version_id": {"type": "string", "description": "Specific version to check (default: latest)"},
                        },
                        "required": ["db_name", "mapping_spec_id"],
                    },
                },
                {
                    "name": "list_schema_changes",
                    "description": "List recent schema changes for a dataset or mapping spec. Shows drift history with severity and change details.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string", "description": "Database name"},
                            "subject_type": {
                                "type": "string",
                                "enum": ["dataset", "mapping_spec"],
                                "description": "Type of subject to query",
                            },
                            "subject_id": {"type": "string", "description": "Subject ID (dataset_id or mapping_spec_id)"},
                            "severity": {"type": "string", "enum": ["info", "warning", "breaking"], "description": "Filter by severity"},
                            "limit": {"type": "integer", "default": 20, "description": "Max records to return"},
                        },
                        "required": ["db_name", "subject_type", "subject_id"],
                    },
                },
            ]
            return [Tool(**spec) for spec in tool_specs]

        @self.server.call_tool()
        async def call_tool(name: str, arguments: Dict[str, Any]) -> Any:
            logger.info("call_tool invoked: name=%s", name)
            # Enterprise Enhancement (2026-01): Rate limiting to prevent runaway loops
            rate_limit_error = _rate_limiter.check_and_record(name)
            if rate_limit_error:
                logger.warning("MCP tool rate limited: %s", rate_limit_error)
                return {"error": rate_limit_error, "tool": name, "retry_after_seconds": 60}

            try:
                if name == "plan_new":
                    plan = new_plan(
                        goal=str(arguments.get("goal") or ""),
                        db_name=str(arguments.get("db_name") or ""),
                        branch=str(arguments.get("branch") or "").strip() or None,
                        dataset_ids=arguments.get("dataset_ids"),
                    )
                    return {"plan": plan}

                if name == "plan_reset":
                    plan = arguments.get("plan") or {}
                    result = reset_plan(plan)
                    return {"plan": result.plan, "warnings": list(result.warnings)}

                if name == "plan_add_input":
                    plan = arguments.get("plan") or {}
                    result = add_input(
                        plan,
                        dataset_id=arguments.get("dataset_id"),
                        dataset_name=arguments.get("dataset_name"),
                        dataset_branch=arguments.get("dataset_branch"),
                        read=arguments.get("read") if isinstance(arguments.get("read"), dict) else None,
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_external_input":
                    plan = arguments.get("plan") or {}
                    read_obj = arguments.get("read")
                    if not isinstance(read_obj, dict) or not read_obj:
                        return {"status": "invalid", "errors": ["read must be a non-empty object"]}
                    result = add_external_input(
                        plan,
                        read=read_obj,
                        source_name=arguments.get("source_name") or arguments.get("sourceName"),
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_configure_input_read":
                    plan = arguments.get("plan") or {}
                    patch: Dict[str, Any] = {}
                    read_obj = arguments.get("read")
                    if isinstance(read_obj, dict):
                        patch.update(read_obj)
                    if arguments.get("format"):
                        patch["format"] = str(arguments.get("format")).strip()
                    options_obj = arguments.get("options")
                    if isinstance(options_obj, dict) and options_obj:
                        patch["options"] = dict(options_obj)
                    options_env_obj = arguments.get("options_env")
                    if isinstance(options_env_obj, dict) and options_env_obj:
                        patch["options_env"] = dict(options_env_obj)
                    schema_obj = arguments.get("schema")
                    if isinstance(schema_obj, list) and schema_obj:
                        patch["schema"] = schema_obj
                    if arguments.get("mode") is not None:
                        patch["mode"] = str(arguments.get("mode"))
                    if arguments.get("corrupt_record_column") is not None:
                        patch["corrupt_record_column"] = str(arguments.get("corrupt_record_column"))
                    if "header" in arguments:
                        patch["header"] = bool(arguments.get("header"))
                    if "infer_schema" in arguments:
                        patch["infer_schema"] = bool(arguments.get("infer_schema"))
                    result = configure_input_read(
                        plan,
                        node_id=str(arguments.get("node_id") or ""),
                        read=patch,
                        replace=bool(arguments.get("replace") or False),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_join":
                    plan = arguments.get("plan") or {}
                    left_keys = (
                        arguments.get("left_keys")
                        or arguments.get("leftKeys")
                        or arguments.get("left_columns")
                        or arguments.get("leftColumns")
                    )
                    right_keys = (
                        arguments.get("right_keys")
                        or arguments.get("rightKeys")
                        or arguments.get("right_columns")
                        or arguments.get("rightColumns")
                    )
                    if not left_keys:
                        left_keys = (
                            arguments.get("left_column")
                            or arguments.get("leftColumn")
                            or arguments.get("left_key")
                            or arguments.get("leftKey")
                            or arguments.get("join_key")
                            or arguments.get("joinKey")
                        )
                    if not right_keys:
                        right_keys = (
                            arguments.get("right_column")
                            or arguments.get("rightColumn")
                            or arguments.get("right_key")
                            or arguments.get("rightKey")
                            or arguments.get("join_key")
                            or arguments.get("joinKey")
                        )
                    join_type = str(arguments.get("join_type") or arguments.get("joinType") or "").strip().lower()
                    valid_join_types = {"inner", "left", "right", "full", "cross"}
                    if not join_type:
                        return {"status": "invalid", "errors": ["join_type is required. Specify one of: inner, left, right, full, cross"]}
                    if join_type not in valid_join_types:
                        return {"status": "invalid", "errors": [f"Invalid join_type '{join_type}'. Must be one of: {', '.join(sorted(valid_join_types))}"]}
                    # Validate key count match
                    left_keys_list = _normalize_string_list(left_keys)
                    right_keys_list = _normalize_string_list(right_keys)
                    if len(left_keys_list) != len(right_keys_list):
                        return {
                            "status": "invalid",
                            "errors": [
                                f"Join key count mismatch: left_keys has {len(left_keys_list)} keys, "
                                f"right_keys has {len(right_keys_list)} keys. They must be equal."
                            ],
                            "left_keys": left_keys_list,
                            "right_keys": right_keys_list,
                        }
                    result = add_join(
                        plan,
                        left_node_id=str(arguments.get("left_node_id") or ""),
                        right_node_id=str(arguments.get("right_node_id") or ""),
                        left_keys=left_keys_list,
                        right_keys=right_keys_list,
                        join_type=join_type,
                        join_hints=arguments.get("join_hints") if isinstance(arguments.get("join_hints"), dict) else None,
                        broadcast_left=bool(arguments.get("broadcast_left") or False),
                        broadcast_right=bool(arguments.get("broadcast_right") or False),
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_group_by":
                    plan = arguments.get("plan") or {}
                    input_node_id = str(arguments.get("input_node_id") or "")
                    group_by = _normalize_string_list(arguments.get("group_by") or arguments.get("groupBy") or [])
                    aggregates, agg_warnings = _normalize_aggregates(arguments.get("aggregates") or arguments.get("aggregations") or [])
                    if not aggregates:
                        if agg_warnings:
                            raise PipelinePlanBuilderError(f"aggregates is required (items: {{column,op,alias?}}). Parse warnings: {agg_warnings}")
                        raise PipelinePlanBuilderError("aggregates is required (items: {column,op,alias?})")
                    op = str(arguments.get("operation") or "groupBy").strip() or "groupBy"
                    if op not in {"groupBy", "aggregate"}:
                        op = "groupBy"
                    result = add_transform(
                        plan,
                        operation=op,
                        input_node_ids=[input_node_id],
                        metadata={"groupBy": group_by, "aggregates": aggregates},
                        node_id=arguments.get("node_id"),
                    )
                    all_warnings = list(result.warnings) + agg_warnings
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": all_warnings}

                if name == "plan_add_group_by_expr":
                    plan = arguments.get("plan") or {}
                    input_node_id = str(arguments.get("input_node_id") or "")
                    group_by = _normalize_string_list(arguments.get("group_by") or arguments.get("groupBy") or [])
                    exprs = arguments.get("aggregate_expressions") or arguments.get("aggregateExpressions") or []
                    op = str(arguments.get("operation") or "groupBy").strip() or "groupBy"
                    if op not in {"groupBy", "aggregate"}:
                        op = "groupBy"
                    result = add_group_by_expr(
                        plan,
                        input_node_id=input_node_id,
                        group_by=group_by,
                        aggregate_expressions=exprs if isinstance(exprs, list) else [exprs],
                        operation=op,
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_window":
                    plan = arguments.get("plan") or {}
                    input_node_id = str(arguments.get("input_node_id") or "")
                    window = arguments.get("window") if isinstance(arguments.get("window"), dict) else None
                    if window is None:
                        partition_by = _normalize_string_list(arguments.get("partition_by") or arguments.get("partitionBy") or [])
                        order_by_raw = arguments.get("order_by") or arguments.get("orderBy") or []
                        order_by = order_by_raw if isinstance(order_by_raw, list) else [order_by_raw]
                        window = {"partitionBy": partition_by, "orderBy": order_by}
                    result = add_transform(
                        plan,
                        operation="window",
                        input_node_ids=[input_node_id],
                        metadata={"window": window},
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_window_expr":
                    plan = arguments.get("plan") or {}
                    input_node_id = str(arguments.get("input_node_id") or "")
                    expressions = arguments.get("expressions") or []
                    result = add_window_expr(
                        plan,
                        input_node_id=input_node_id,
                        expressions=expressions if isinstance(expressions, list) else [expressions],
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_transform":
                    plan = arguments.get("plan") or {}
                    operation = str(arguments.get("operation") or "").strip()
                    input_node_ids = arguments.get("input_node_ids")
                    if not input_node_ids:
                        single = arguments.get("input_node_id") or arguments.get("inputNodeId")
                        input_node_ids = [single] if single else []
                    metadata = arguments.get("metadata") or {}
                    if not isinstance(metadata, dict):
                        metadata = {}
                    transform_agg_warnings: List[str] = []
                    if not metadata and operation in {"groupBy", "aggregate"}:
                        group_by = _normalize_string_list(arguments.get("group_by") or arguments.get("groupBy") or [])
                        aggregates, transform_agg_warnings = _normalize_aggregates(arguments.get("aggregates") or arguments.get("aggregations") or [])
                        metadata = {"groupBy": group_by, "aggregates": aggregates}
                    if not metadata and operation == "window":
                        window = arguments.get("window") if isinstance(arguments.get("window"), dict) else None
                        if window is None:
                            partition_by = _normalize_string_list(arguments.get("partition_by") or arguments.get("partitionBy") or [])
                            order_by_raw = arguments.get("order_by") or arguments.get("orderBy") or []
                            order_by = order_by_raw if isinstance(order_by_raw, list) else [order_by_raw]
                            window = {"partitionBy": partition_by, "orderBy": order_by}
                        metadata = {"window": window}
                    result = add_transform(
                        plan,
                        operation=operation,
                        input_node_ids=input_node_ids or [],
                        metadata=metadata,
                        node_id=arguments.get("node_id"),
                    )
                    all_warnings = list(result.warnings) + transform_agg_warnings
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": all_warnings}

                if name == "plan_add_sort":
                    plan = arguments.get("plan") or {}
                    input_node_id = str(arguments.get("input_node_id") or "")
                    cols = arguments.get("columns") or []
                    columns = cols if isinstance(cols, list) else [cols]
                    result = add_sort(
                        plan,
                        input_node_id=input_node_id,
                        columns=columns,
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_explode":
                    plan = arguments.get("plan") or {}
                    input_node_id = str(arguments.get("input_node_id") or "")
                    column = str(arguments.get("column") or "").strip()
                    result = add_explode(
                        plan,
                        input_node_id=input_node_id,
                        column=column,
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_union":
                    plan = arguments.get("plan") or {}
                    result = add_union(
                        plan,
                        left_node_id=str(arguments.get("left_node_id") or ""),
                        right_node_id=str(arguments.get("right_node_id") or ""),
                        union_mode=str(arguments.get("union_mode") or "strict"),
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_pivot":
                    plan = arguments.get("plan") or {}
                    input_node_id = str(arguments.get("input_node_id") or "")
                    index = arguments.get("index") or []
                    if not isinstance(index, list):
                        index = [index]
                    columns_val = str(arguments.get("columns") or "").strip()
                    values_val = str(arguments.get("values") or "").strip()
                    # Validate required fields
                    pivot_errors: List[str] = []
                    if not input_node_id:
                        pivot_errors.append("input_node_id is required")
                    if not columns_val:
                        pivot_errors.append("columns is required (column to pivot on)")
                    if not values_val:
                        pivot_errors.append("values is required (column to aggregate)")
                    if pivot_errors:
                        return {"status": "invalid", "errors": pivot_errors}
                    result = add_pivot(
                        plan,
                        input_node_id=input_node_id,
                        index=[str(item) for item in index],
                        columns=columns_val,
                        values=values_val,
                        agg=str(arguments.get("agg") or "sum"),
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_filter":
                    plan = arguments.get("plan") or {}
                    result = add_filter(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        expression=str(arguments.get("expression") or ""),
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_compute":
                    plan = arguments.get("plan") or {}
                    input_node_id = str(arguments.get("input_node_id") or "")
                    expr = str(arguments.get("expression") or "").strip()

                    # Common LLM shape: {"computations":[{"alias":"x","expression":"a*b"}, ...]}
                    if not expr:
                        raw = (
                            arguments.get("computations")
                            or arguments.get("computes")
                            or arguments.get("compute")
                            or arguments.get("expressions")
                            or []
                        )
                        items = raw if isinstance(raw, list) else [raw]
                        assignments: list[dict[str, Any]] = []
                        for item in items:
                            if isinstance(item, str) and item.strip():
                                text = item.strip()
                                if "=" in text:
                                    left, right = [part.strip() for part in text.split("=", 1)]
                                    if left and right:
                                        assignments.append({"column": left, "expression": right})
                                continue
                            if not isinstance(item, dict):
                                continue
                            alias = (
                                item.get("alias")
                                or item.get("new_column")
                                or item.get("newColumn")
                                or item.get("column")
                                or item.get("name")
                            )
                            formula = item.get("expression") or item.get("formula") or item.get("expr")
                            alias_value = str(alias or "").strip()
                            formula_value = str(formula or "").strip()
                            if not alias_value or not formula_value:
                                continue
                            assignments.append({"column": alias_value, "expression": formula_value})

                        if assignments:
                            result = add_compute_assignments(
                                plan,
                                input_node_id=input_node_id,
                                assignments=assignments,
                                node_id=arguments.get("node_id"),
                            )
                            return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                    # Another common shape: expression="a*b", alias="x" (no assignment in expression).
                    alias = str(
                        arguments.get("alias")
                        or arguments.get("new_column")
                        or arguments.get("newColumn")
                        or ""
                    ).strip()
                    if expr and alias and "=" not in expr:
                        result = add_compute_column(
                            plan,
                            input_node_id=input_node_id,
                            target_column=alias,
                            formula=expr,
                            node_id=arguments.get("node_id"),
                        )
                        return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                    result = add_compute(
                        plan,
                        input_node_id=input_node_id,
                        expression=expr,
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_compute_column":
                    plan = arguments.get("plan") or {}
                    result = add_compute_column(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        target_column=str(arguments.get("target_column") or ""),
                        formula=str(arguments.get("formula") or ""),
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_compute_assignments":
                    plan = arguments.get("plan") or {}
                    result = add_compute_assignments(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        assignments=arguments.get("assignments") or [],
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_cast":
                    plan = arguments.get("plan") or {}
                    result = add_cast(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        casts=arguments.get("casts") or [],
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_rename":
                    plan = arguments.get("plan") or {}
                    # Be forgiving: LLMs often pass rename maps as a list of {from,to} objects or
                    # use alternate field names like `mappings` / `renames`.
                    raw_rename = arguments.get("rename")
                    if raw_rename is None:
                        raw_rename = arguments.get("renames")
                    if raw_rename is None:
                        raw_rename = arguments.get("mappings")

                    rename_map: Dict[str, str] = {}
                    if isinstance(raw_rename, dict):
                        # Accept both mapping-form {"old":"new"} and single-pair form {"from":"old","to":"new"}.
                        keys = {str(k) for k in raw_rename.keys() if k is not None}
                        if ({"from", "to"} <= keys) or ({"src", "dst"} <= keys) or ({"source", "target"} <= keys):
                            src = str(
                                raw_rename.get("from")
                                or raw_rename.get("src")
                                or raw_rename.get("source")
                                or ""
                            ).strip()
                            dst = str(
                                raw_rename.get("to")
                                or raw_rename.get("dst")
                                or raw_rename.get("target")
                                or ""
                            ).strip()
                            if src and dst:
                                rename_map[src] = dst
                        else:
                            for k, v in raw_rename.items():
                                src = str(k or "").strip()
                                dst = str(v or "").strip()
                                if src and dst:
                                    rename_map[src] = dst
                    elif isinstance(raw_rename, list):
                        for item in raw_rename:
                            if not isinstance(item, dict):
                                continue
                            src = str(item.get("from") or item.get("src") or item.get("source") or "").strip()
                            dst = str(item.get("to") or item.get("dst") or item.get("target") or "").strip()
                            if src and dst:
                                rename_map[src] = dst

                    result = add_rename(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        rename=rename_map,
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_select":
                    plan = arguments.get("plan") or {}
                    result = add_select(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        columns=arguments.get("columns") or [],
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_select_expr":
                    plan = arguments.get("plan") or {}
                    result = add_select_expr(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        expressions=arguments.get("expressions") or [],
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_drop":
                    plan = arguments.get("plan") or {}
                    result = add_drop(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        columns=arguments.get("columns") or [],
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_dedupe":
                    plan = arguments.get("plan") or {}
                    result = add_dedupe(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        columns=arguments.get("columns") or [],
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_normalize":
                    plan = arguments.get("plan") or {}
                    result = add_normalize(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        columns=arguments.get("columns") or [],
                        trim=bool(arguments.get("trim", True)),
                        empty_to_null=bool(arguments.get("empty_to_null", True)),
                        whitespace_to_null=bool(arguments.get("whitespace_to_null", True)),
                        lowercase=bool(arguments.get("lowercase", False)),
                        uppercase=bool(arguments.get("uppercase", False)),
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_regex_replace":
                    plan = arguments.get("plan") or {}
                    result = add_regex_replace(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        rules=arguments.get("rules") or [],
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_output":
                    plan = arguments.get("plan") or {}
                    result = add_output(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        output_name=str(arguments.get("output_name") or ""),
                        output_kind=str(arguments.get("output_kind") or "unknown"),
                        node_id=arguments.get("node_id"),
                        output_metadata=arguments.get("output_metadata"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_edge":
                    plan = arguments.get("plan") or {}
                    result = add_edge(
                        plan,
                        from_node_id=str(arguments.get("from_node_id") or ""),
                        to_node_id=str(arguments.get("to_node_id") or ""),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_delete_edge":
                    plan = arguments.get("plan") or {}
                    result = delete_edge(
                        plan,
                        from_node_id=str(arguments.get("from_node_id") or ""),
                        to_node_id=str(arguments.get("to_node_id") or ""),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_set_node_inputs":
                    plan = arguments.get("plan") or {}
                    result = set_node_inputs(
                        plan,
                        node_id=str(arguments.get("node_id") or ""),
                        input_node_ids=arguments.get("input_node_ids") or [],
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_update_node_metadata":
                    plan = arguments.get("plan") or {}
                    # Models often guess "metadata"/"meta" instead of the canonical "set" param.
                    set_fields = arguments.get("set")
                    if set_fields is None:
                        candidate = arguments.get("metadata")
                        if isinstance(candidate, dict):
                            set_fields = candidate
                    if set_fields is None:
                        candidate = arguments.get("meta")
                        if isinstance(candidate, dict):
                            set_fields = candidate
                    result = update_node_metadata(
                        plan,
                        node_id=str(arguments.get("node_id") or ""),
                        set_fields=set_fields,
                        unset_fields=arguments.get("unset"),
                        replace=bool(arguments.get("replace", False)),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_update_settings":
                    plan = arguments.get("plan") or {}
                    set_fields = arguments.get("set")
                    if set_fields is None:
                        candidate = arguments.get("settings")
                        if isinstance(candidate, dict):
                            set_fields = candidate
                    result = update_settings(
                        plan,
                        set_fields=set_fields,
                        unset_fields=arguments.get("unset"),
                        replace=bool(arguments.get("replace", False)),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_delete_node":
                    plan = arguments.get("plan") or {}
                    result = delete_node(
                        plan,
                        node_id=str(arguments.get("node_id") or ""),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_update_output":
                    plan = arguments.get("plan") or {}
                    output_name = str(arguments.get("output_name") or "").strip()
                    if not output_name:
                        # Convenience: allow selecting the output entry by output node id, since most other
                        # plan patch tools are node_id-based and LLMs commonly supply node_id.
                        node_id = str(arguments.get("node_id") or "").strip()
                        if node_id:
                            definition = plan.get("definition_json")
                            nodes = definition.get("nodes") if isinstance(definition, dict) else None
                            if isinstance(nodes, list):
                                for node in nodes:
                                    if not isinstance(node, dict):
                                        continue
                                    if str(node.get("id") or "").strip() != node_id:
                                        continue
                                    if str(node.get("type") or "").strip().lower() != "output":
                                        continue
                                    meta = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
                                    output_name = str(meta.get("outputName") or meta.get("output_name") or "").strip()
                                    break
                    if not output_name:
                        return {"error": "output_name is required (or provide node_id of an output node)."}
                    result = update_output(
                        plan,
                        output_name=output_name,
                        set_fields=arguments.get("set"),
                        unset_fields=arguments.get("unset"),
                        replace=bool(arguments.get("replace", False)),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_validate_structure":
                    plan = arguments.get("plan") or {}
                    errors, warnings = validate_structure(plan)
                    return {"errors": errors, "warnings": warnings}

                if name == "plan_validate":
                    plan_obj = arguments.get("plan") or {}
                    try:
                        plan = PipelinePlan.model_validate(plan_obj)
                    except Exception as exc:
                        return {"status": "invalid", "errors": [str(exc)], "warnings": []}

                    db_name = str(plan.data_scope.db_name or "").strip()
                    if not db_name:
                        return {"status": "invalid", "errors": ["plan.data_scope.db_name is required"], "warnings": []}

                    dataset_registry, _ = await self._ensure_registries()
                    validation = await validate_pipeline_plan(
                        plan=plan,
                        dataset_registry=dataset_registry,
                        db_name=db_name,
                        branch=str(plan.data_scope.branch or "") or None,
                        require_output=bool(arguments.get("require_output", False)),
                    )
                    payload = {
                        "status": "success" if not validation.errors else "invalid",
                        "plan": validation.plan.model_dump(mode="json"),
                        "errors": list(validation.errors or []),
                        "warnings": list(validation.warnings or []),
                        "preflight": validation.preflight,
                        "compilation_report": validation.compilation_report.model_dump(mode="json"),
                    }
                    return payload

                if name == "plan_preview":
                    plan_obj = arguments.get("plan") or {}
                    # Validate shape early to avoid opaque executor errors.
                    errors, warnings = validate_structure(plan_obj)
                    if errors:
                        return {"status": "invalid", "errors": errors, "warnings": warnings}

                    try:
                        plan = PipelinePlan.model_validate(plan_obj)
                    except Exception as exc:
                        return {"status": "invalid", "errors": [str(exc)], "warnings": warnings}

                    db_name = str(plan.data_scope.db_name or "").strip()
                    if not db_name:
                        return {"status": "invalid", "errors": ["plan.data_scope.db_name is required"], "warnings": warnings}

                    dataset_registry, _ = await self._ensure_registries()
                    executor = PipelineExecutor(dataset_registry)
                    limit = int(arguments.get("limit") or 50)
                    node_id = str(arguments.get("node_id") or "").strip() or None

                    definition = dict(plan.definition_json or {})
                    preview_meta = dict(definition.get("__preview_meta__") or {})
                    preview_meta.setdefault("branch", str(plan.data_scope.branch or "") or "main")
                    preview_meta["sample_limit"] = max(1, min(limit, 200))
                    definition["__preview_meta__"] = preview_meta

                    preview_policy = evaluate_preview_policy(definition)
                    policy_level = str(preview_policy.get("level") or "allow").strip().lower()
                    if policy_level == "deny":
                        return {
                            "status": "preview_denied",
                            "error": "Plan preview denied by policy.",
                            "preview_policy": preview_policy,
                            "preview": {
                                "row_count": 0,
                                "columns": [],
                                "rows": [],
                            },
                            "hint": "Fix the denied operations (see preview_policy) before preview/build/deploy.",
                            "warnings": list(warnings or []),
                        }
                    if policy_level == "require_spark":
                        return {
                            "status": "requires_spark_preview",
                            "preview_policy": preview_policy,
                            "preview": {
                                "row_count": 0,
                                "columns": [],
                                "rows": [],
                            },
                            "hint": "Plan preview is not reliable for this plan. Use pipeline_preview_wait for Spark-backed execution.",
                            "warnings": list(warnings or []),
                        }

                    try:
                        preview = await executor.preview(definition=definition, db_name=db_name, node_id=node_id, limit=limit)
                    except Exception as exc:
                        # Plan preview runs in a lightweight Python executor that cannot support all Spark SQL
                        # expressions. Return explicit error status instead of hiding as warning.
                        logger.warning("plan_preview failed: %s", exc, exc_info=True)
                        return {
                            "status": "preview_failed",
                            "error": str(exc),
                            "error_type": type(exc).__name__,
                            "preview_policy": preview_policy,
                            "preview": {
                                "row_count": 0,
                                "columns": [],
                                "rows": [],
                            },
                            "hint": "Plan preview failed. Use pipeline_preview_wait for Spark-backed execution.",
                            "warnings": list(warnings or []),
                        }

                    preview_masked = mask_pii(preview)
                    return {"status": "success", "preview": preview_masked, "preview_policy": preview_policy, "warnings": warnings}

                if name == "plan_refute_claims":
                    plan_obj = arguments.get("plan") or {}
                    # Validate shape early to avoid opaque executor errors.
                    errors, warnings = validate_structure(plan_obj)
                    if errors:
                        return {"status": "invalid", "errors": errors, "warnings": warnings}

                    try:
                        plan = PipelinePlan.model_validate(plan_obj)
                    except Exception as exc:
                        return {"status": "invalid", "errors": [str(exc)], "warnings": warnings}

                    db_name = str(plan.data_scope.db_name or "").strip()
                    if not db_name:
                        return {"status": "invalid", "errors": ["plan.data_scope.db_name is required"], "warnings": warnings}

                    dataset_registry, _ = await self._ensure_registries()
                    report = await refute_pipeline_plan_claims(
                        plan=plan,
                        dataset_registry=dataset_registry,
                        run_tables=arguments.get("run_tables"),
                        sample_limit=int(arguments.get("sample_limit") or 400),
                        max_output_rows=int(arguments.get("max_output_rows") or 20000),
                        max_hard_failures=int(arguments.get("max_hard_failures") or 5),
                        max_soft_warnings=int(arguments.get("max_soft_warnings") or 20),
                    )
                    if warnings:
                        merged = list(report.get("warnings") or []) if isinstance(report, dict) else []
                        merged.extend([w for w in warnings if w])
                        if isinstance(report, dict):
                            report["warnings"] = merged[:40]
                    return report

                if name == "preview_inspect":
                    preview = arguments.get("preview") or {}
                    if not isinstance(preview, dict):
                        return {"error": "preview must be an object"}
                    inspection = inspect_preview(preview)
                    return {"status": "success", "inspector": mask_pii(inspection)}

                if name == "plan_evaluate_joins":
                    plan_obj = arguments.get("plan") or {}
                    try:
                        plan = PipelinePlan.model_validate(plan_obj)
                    except Exception as exc:
                        return {"status": "invalid", "errors": [str(exc)], "warnings": []}

                    db_name = str(plan.data_scope.db_name or "").strip()
                    if not db_name:
                        return {"status": "invalid", "errors": ["plan.data_scope.db_name is required"], "warnings": []}

                    dataset_registry, _ = await self._ensure_registries()
                    evaluations, warnings = await evaluate_pipeline_joins(
                        definition_json=dict(plan.definition_json or {}),
                        db_name=db_name,
                        dataset_registry=dataset_registry,
                        node_filter=str(arguments.get("node_filter") or "").strip() or None,
                        run_tables=arguments.get("run_tables"),
                    )
                    return {
                        "status": "success",
                        "evaluations": [asdict(item) for item in evaluations],
                        "warnings": list(warnings or []),
                    }

                if name == "pipeline_create_from_plan":
                    plan_obj = arguments.get("plan") or {}
                    try:
                        plan = PipelinePlan.model_validate(plan_obj)
                    except Exception as exc:
                        return {"status": "invalid", "errors": [str(exc)]}
                    db_name = str(plan.data_scope.db_name or "").strip()
                    if not db_name:
                        return {"status": "invalid", "errors": ["plan.data_scope.db_name is required"]}

                    pipeline_name = str(arguments.get("name") or "").strip()
                    if not pipeline_name:
                        return {"status": "invalid", "errors": ["name is required"]}
                    location = str(arguments.get("location") or "").strip()
                    if not location:
                        return {"status": "invalid", "errors": ["location is required"]}

                    payload: Dict[str, Any] = {
                        "db_name": db_name,
                        "name": pipeline_name,
                        "location": location,
                        "branch": str(arguments.get("branch") or plan.data_scope.branch or "main").strip() or "main",
                        "pipeline_type": str(arguments.get("pipeline_type") or "batch").strip() or "batch",
                        "definition_json": dict(plan.definition_json or {}),
                    }
                    description = str(arguments.get("description") or "").strip() or None
                    if description:
                        payload["description"] = description
                    pipeline_id = str(arguments.get("pipeline_id") or "").strip() or None
                    if pipeline_id:
                        payload["pipeline_id"] = pipeline_id

                    resp = await _bff_json(
                        "POST",
                        "/pipelines",
                        db_name=db_name,
                        principal_id=str(arguments.get("principal_id") or "").strip() or None,
                        principal_type=str(arguments.get("principal_type") or "").strip() or None,
                        json_body=payload,
                        timeout_seconds=30.0,
                    )
                    if resp.get("error"):
                        # Upsert behavior: if the pipeline already exists, update its definition.
                        if int(resp.get("status_code") or 0) == 409 and isinstance(resp.get("response"), dict):
                            resp_body = resp.get("response") or {}
                            detail = resp_body.get("detail") if isinstance(resp_body.get("detail"), dict) else {}
                            detail_code = str(detail.get("code") or "").strip().upper()
                            enterprise = resp_body.get("enterprise") if isinstance(resp_body.get("enterprise"), dict) else {}
                            legacy_code = str(enterprise.get("legacy_code") or "").strip().upper()
                            if detail_code == "PIPELINE_ALREADY_EXISTS" or legacy_code == "PIPELINE_ALREADY_EXISTS":
                                branch = str(payload.get("branch") or "main").strip() or "main"
                                list_resp = await _bff_json(
                                    "GET",
                                    "/pipelines",
                                    db_name=db_name,
                                    principal_id=str(arguments.get("principal_id") or "").strip() or None,
                                    principal_type=str(arguments.get("principal_type") or "").strip() or None,
                                    params={"db_name": db_name, "limit": 200},
                                    timeout_seconds=30.0,
                                )
                                if not list_resp.get("error"):
                                    data = list_resp.get("data") if isinstance(list_resp.get("data"), dict) else {}
                                    items = data.get("pipelines") if isinstance(data.get("pipelines"), list) else []
                                    match = None
                                    for item in items:
                                        if not isinstance(item, dict):
                                            continue
                                        if str(item.get("name") or "").strip() != pipeline_name:
                                            continue
                                        if str(item.get("branch") or "main").strip() != branch:
                                            continue
                                        match = item
                                        break
                                    pipeline_id_existing = (
                                        str((match or {}).get("pipeline_id") or (match or {}).get("pipelineId") or "").strip()
                                        if isinstance(match, dict)
                                        else ""
                                    )
                                    if pipeline_id_existing:
                                        update_resp = await _bff_json(
                                            "PUT",
                                            f"/pipelines/{pipeline_id_existing}",
                                            db_name=db_name,
                                            principal_id=str(arguments.get("principal_id") or "").strip() or None,
                                            principal_type=str(arguments.get("principal_type") or "").strip() or None,
                                            json_body={"definition_json": dict(plan.definition_json or {})},
                                            timeout_seconds=30.0,
                                        )
                                        if update_resp.get("error"):
                                            return update_resp
                                        update_data = (
                                            update_resp.get("data")
                                            if isinstance(update_resp.get("data"), dict)
                                            else {}
                                        )
                                        pipeline = (
                                            update_data.get("pipeline")
                                            if isinstance(update_data.get("pipeline"), dict)
                                            else {}
                                        )
                                        return {
                                            "status": "success",
                                            "reused": True,
                                            "pipeline": {
                                                "pipeline_id": pipeline.get("pipeline_id")
                                                or pipeline.get("pipelineId")
                                                or pipeline_id_existing,
                                                "db_name": pipeline.get("db_name") or db_name,
                                                "name": pipeline.get("name") or pipeline_name,
                                                "branch": pipeline.get("branch") or branch,
                                                "location": pipeline.get("location") or location,
                                                "version": pipeline.get("version")
                                                or pipeline.get("commit_id")
                                                or pipeline.get("commitId"),
                                            },
                                        }
                        return resp
                    data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
                    pipeline = data.get("pipeline") if isinstance(data.get("pipeline"), dict) else {}
                    return {
                        "status": "success",
                        "pipeline": {
                            "pipeline_id": pipeline.get("pipeline_id") or pipeline.get("pipelineId"),
                            "db_name": pipeline.get("db_name"),
                            "name": pipeline.get("name"),
                            "branch": pipeline.get("branch"),
                            "location": pipeline.get("location"),
                            "version": pipeline.get("version") or pipeline.get("commit_id") or pipeline.get("commitId"),
                        },
                    }

                if name == "pipeline_update_from_plan":
                    pipeline_id = str(arguments.get("pipeline_id") or "").strip()
                    if not pipeline_id:
                        return {"status": "invalid", "errors": ["pipeline_id is required"]}
                    plan_obj = arguments.get("plan") or {}
                    try:
                        plan = PipelinePlan.model_validate(plan_obj)
                    except Exception as exc:
                        return {"status": "invalid", "errors": [str(exc)]}
                    db_name = str(plan.data_scope.db_name or "").strip()
                    if not db_name:
                        return {"status": "invalid", "errors": ["plan.data_scope.db_name is required"]}
                    payload: Dict[str, Any] = {
                        "definition_json": dict(plan.definition_json or {}),
                    }
                    branch = str(arguments.get("branch") or "").strip() or None
                    if branch:
                        payload["branch"] = branch
                    resp = await _bff_json(
                        "PUT",
                        f"/pipelines/{pipeline_id}",
                        db_name=db_name,
                        principal_id=str(arguments.get("principal_id") or "").strip() or None,
                        principal_type=str(arguments.get("principal_type") or "").strip() or None,
                        json_body=payload,
                        timeout_seconds=30.0,
                    )
                    if resp.get("error"):
                        return resp
                    data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
                    pipeline = data.get("pipeline") if isinstance(data.get("pipeline"), dict) else {}
                    return {
                        "status": "success",
                        "pipeline": {
                            "pipeline_id": pipeline.get("pipeline_id") or pipeline.get("pipelineId"),
                            "db_name": pipeline.get("db_name"),
                            "name": pipeline.get("name"),
                            "branch": pipeline.get("branch"),
                            "location": pipeline.get("location"),
                            "version": pipeline.get("version") or pipeline.get("commit_id") or pipeline.get("commitId"),
                        },
                    }

                if name == "pipeline_preview_wait":
                    pipeline_id = str(arguments.get("pipeline_id") or "").strip()
                    if not pipeline_id:
                        return {"status": "invalid", "errors": ["pipeline_id is required"]}
                    limit = int(arguments.get("limit") or 200)
                    limit = max(1, min(limit, 500))
                    node_id = str(arguments.get("node_id") or "").strip() or None
                    branch = str(arguments.get("branch") or "").strip() or None
                    job_id_arg = str(arguments.get("job_id") or "").strip() or None
                    force = bool(arguments.get("force") or False)
                    wait = bool(arguments.get("wait", True))
                    timeout_seconds = float(arguments.get("timeout_seconds") or 180.0)
                    poll_s = float(arguments.get("poll_interval_seconds") or 2.0)
                    db_name = str(arguments.get("db_name") or "").strip()
                    principal_id = str(arguments.get("principal_id") or "").strip() or None
                    principal_type = str(arguments.get("principal_type") or "").strip() or None

                    async def _fetch_runs() -> Dict[str, Any]:
                        return await _bff_json(
                            "GET",
                            f"/pipelines/{pipeline_id}/runs",
                            db_name=db_name,
                            principal_id=principal_id,
                            principal_type=principal_type,
                            params={"limit": 50},
                            timeout_seconds=15.0,
                        )

                    job_id = job_id_arg
                    reused_existing = False
                    if not job_id and not force:
                        # If there's already a queued/running preview for this node, reuse it.
                        runs = await _fetch_runs()
                        if not runs.get("error"):
                            runs_data = runs.get("data") if isinstance(runs.get("data"), dict) else {}
                            run_list = runs_data.get("runs") if isinstance(runs_data.get("runs"), list) else []
                            for item in run_list:
                                if not isinstance(item, dict):
                                    continue
                                if str(item.get("mode") or "").strip().lower() != "preview":
                                    continue
                                status_value = str(item.get("status") or "").strip().upper()
                                if status_value not in {"QUEUED", "RUNNING"}:
                                    continue
                                item_node_id = str(item.get("node_id") or "").strip() or None
                                if item_node_id != node_id:
                                    continue
                                candidate = str(item.get("job_id") or "").strip()
                                if candidate:
                                    job_id = candidate
                                    reused_existing = True
                                    break

                    if not job_id:
                        # Enqueue preview on the Spark worker via BFF.
                        enqueue_body: Dict[str, Any] = {"limit": limit}
                        if node_id:
                            enqueue_body["node_id"] = node_id
                        if branch:
                            enqueue_body["branch"] = branch
                        resp = await _bff_json(
                            "POST",
                            f"/pipelines/{pipeline_id}/preview",
                            db_name=db_name,
                            principal_id=principal_id,
                            principal_type=principal_type,
                            json_body=enqueue_body,
                            timeout_seconds=30.0,
                        )
                        if resp.get("error"):
                            return resp
                        data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
                        job_id = str(data.get("job_id") or "").strip()
                        if not job_id and isinstance(data.get("sample"), dict):
                            job_id = str((data.get("sample") or {}).get("job_id") or "").strip()
                        if not job_id:
                            return {"error": "preview enqueue did not return job_id", "response": resp}

                    if not wait:
                        # If the caller provided a job_id, return a one-shot status snapshot.
                        if job_id_arg:
                            runs = await _fetch_runs()
                            if runs.get("error"):
                                return {
                                    "status": "queued",
                                    "job_id": job_id,
                                    "reused_existing_job": reused_existing,
                                }
                            runs_data = runs.get("data") if isinstance(runs.get("data"), dict) else {}
                            run_list = runs_data.get("runs") if isinstance(runs_data.get("runs"), list) else []
                            selected_run: Optional[Dict[str, Any]] = None
                            for item in run_list:
                                if not isinstance(item, dict):
                                    continue
                                if str(item.get("job_id") or "").strip() == job_id:
                                    selected_run = item
                                    break
                            if selected_run:
                                status_value = str(selected_run.get("status") or "").strip().upper() or "QUEUED"
                                if status_value in {"SUCCESS", "FAILED"}:
                                    sample_json = (
                                        selected_run.get("sample_json")
                                        if isinstance(selected_run.get("sample_json"), dict)
                                        else {}
                                    )
                                    masked = mask_pii(sample_json)
                                    result = {
                                        "status": status_value.lower(),
                                        "job_id": job_id,
                                        "reused_existing_job": reused_existing,
                                        "preview": _trim_preview_payload(masked, max_rows=8),
                                    }
                                    # Include error details for failed jobs
                                    if status_value == "FAILED":
                                        result.update(_extract_spark_error_details(selected_run))
                                    return result
                                return {
                                    "status": status_value.lower(),
                                    "job_id": job_id,
                                    "reused_existing_job": reused_existing,
                                }
                        return {
                            "status": "queued",
                            "job_id": job_id,
                            "reused_existing_job": reused_existing,
                            "limit": limit,
                        }

                    deadline = asyncio.get_running_loop().time() + max(1.0, timeout_seconds)
                    last_status: Optional[str] = None
                    while asyncio.get_running_loop().time() < deadline:
                        runs = await _fetch_runs()
                        if runs.get("error"):
                            return runs
                        runs_data = runs.get("data") if isinstance(runs.get("data"), dict) else {}
                        run_list = runs_data.get("runs") if isinstance(runs_data.get("runs"), list) else []
                        selected_run: Optional[Dict[str, Any]] = None
                        for item in run_list:
                            if not isinstance(item, dict):
                                continue
                            if str(item.get("job_id") or "").strip() == job_id:
                                selected_run = item
                                break
                        if not selected_run:
                            await asyncio.sleep(max(0.2, poll_s))
                            continue
                        status_value = str(selected_run.get("status") or "").strip().upper() or "QUEUED"
                        last_status = status_value
                        if status_value in {"SUCCESS", "FAILED"}:
                            sample_json = (
                                selected_run.get("sample_json")
                                if isinstance(selected_run.get("sample_json"), dict)
                                else {}
                            )
                            masked = mask_pii(sample_json)
                            result = {
                                "status": status_value.lower(),
                                "job_id": job_id,
                                "reused_existing_job": reused_existing,
                                "preview": _trim_preview_payload(masked, max_rows=8),
                            }
                            # Include error details for failed jobs
                            if status_value == "FAILED":
                                result.update(_extract_spark_error_details(selected_run))
                            return result
                        await asyncio.sleep(max(0.2, poll_s))

                    return {
                        "status": "timeout",
                        "job_id": job_id,
                        "reused_existing_job": reused_existing,
                        "last_status": last_status,
                        "message": "preview still running",
                    }

                if name == "pipeline_build_wait":
                    pipeline_id = str(arguments.get("pipeline_id") or "").strip()
                    if not pipeline_id:
                        return {"status": "invalid", "errors": ["pipeline_id is required"]}
                    limit = int(arguments.get("limit") or 200)
                    limit = max(1, min(limit, 500))
                    node_id = str(arguments.get("node_id") or "").strip() or None
                    branch = str(arguments.get("branch") or "").strip() or None
                    job_id_arg = str(arguments.get("job_id") or "").strip() or None
                    force = bool(arguments.get("force") or False)
                    wait = bool(arguments.get("wait", True))
                    timeout_seconds = float(arguments.get("timeout_seconds") or 600.0)
                    poll_s = float(arguments.get("poll_interval_seconds") or 2.5)
                    db_name = str(arguments.get("db_name") or "").strip()
                    principal_id = str(arguments.get("principal_id") or "").strip() or None
                    principal_type = str(arguments.get("principal_type") or "").strip() or None

                    async def _fetch_runs() -> Dict[str, Any]:
                        return await _bff_json(
                            "GET",
                            f"/pipelines/{pipeline_id}/runs",
                            db_name=db_name,
                            principal_id=principal_id,
                            principal_type=principal_type,
                            params={"limit": 50},
                            timeout_seconds=15.0,
                        )

                    job_id = job_id_arg
                    reused_existing = False
                    if not job_id and not force:
                        # If there's already a queued/running build for this node, reuse it.
                        runs = await _fetch_runs()
                        if not runs.get("error"):
                            runs_data = runs.get("data") if isinstance(runs.get("data"), dict) else {}
                            run_list = runs_data.get("runs") if isinstance(runs_data.get("runs"), list) else []
                            for item in run_list:
                                if not isinstance(item, dict):
                                    continue
                                if str(item.get("mode") or "").strip().lower() != "build":
                                    continue
                                status_value = str(item.get("status") or "").strip().upper()
                                if status_value not in {"QUEUED", "RUNNING"}:
                                    continue
                                item_node_id = str(item.get("node_id") or "").strip() or None
                                if item_node_id != node_id:
                                    continue
                                candidate = str(item.get("job_id") or "").strip()
                                if candidate:
                                    job_id = candidate
                                    reused_existing = True
                                    break

                    if not job_id:
                        enqueue_body: Dict[str, Any] = {"limit": limit}
                        if node_id:
                            enqueue_body["node_id"] = node_id
                        if branch:
                            enqueue_body["branch"] = branch
                        resp = await _bff_json(
                            "POST",
                            f"/pipelines/{pipeline_id}/build",
                            db_name=db_name,
                            principal_id=principal_id,
                            principal_type=principal_type,
                            json_body=enqueue_body,
                            timeout_seconds=30.0,
                        )
                        if resp.get("error"):
                            return resp
                        data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
                        job_id = str(data.get("job_id") or "").strip()
                        if not job_id:
                            return {"error": "build enqueue did not return job_id", "response": resp}

                    if not wait:
                        # If the caller provided a job_id, return a one-shot status snapshot.
                        if job_id_arg:
                            runs = await _fetch_runs()
                            if runs.get("error"):
                                return {
                                    "status": "queued",
                                    "job_id": job_id,
                                    "reused_existing_job": reused_existing,
                                }
                            runs_data = runs.get("data") if isinstance(runs.get("data"), dict) else {}
                            run_list = runs_data.get("runs") if isinstance(runs_data.get("runs"), list) else []
                            selected_run: Optional[Dict[str, Any]] = None
                            for item in run_list:
                                if not isinstance(item, dict):
                                    continue
                                if str(item.get("job_id") or "").strip() == job_id:
                                    selected_run = item
                                    break
                            if selected_run:
                                status_value = str(selected_run.get("status") or "").strip().upper() or "QUEUED"
                                if status_value in {"SUCCESS", "FAILED"}:
                                    output_json = (
                                        selected_run.get("output_json")
                                        if isinstance(selected_run.get("output_json"), dict)
                                        else {}
                                    )
                                    trimmed = _trim_build_output(output_json, max_rows=6)
                                    masked = mask_pii(trimmed)
                                    result = {
                                        "status": status_value.lower(),
                                        "job_id": job_id,
                                        "reused_existing_job": reused_existing,
                                        "artifact_id": output_json.get("artifact_id") if isinstance(output_json, dict) else None,
                                        "output": masked,
                                    }
                                    # Include error details for failed jobs
                                    if status_value == "FAILED":
                                        result.update(_extract_spark_error_details(selected_run))
                                    return result
                                return {
                                    "status": status_value.lower(),
                                    "job_id": job_id,
                                    "reused_existing_job": reused_existing,
                                }
                        return {
                            "status": "queued",
                            "job_id": job_id,
                            "reused_existing_job": reused_existing,
                            "limit": limit,
                        }

                    deadline = asyncio.get_running_loop().time() + max(1.0, timeout_seconds)
                    last_status: Optional[str] = None
                    while asyncio.get_running_loop().time() < deadline:
                        runs = await _fetch_runs()
                        if runs.get("error"):
                            return runs
                        runs_data = runs.get("data") if isinstance(runs.get("data"), dict) else {}
                        run_list = runs_data.get("runs") if isinstance(runs_data.get("runs"), list) else []
                        selected_run: Optional[Dict[str, Any]] = None
                        for item in run_list:
                            if not isinstance(item, dict):
                                continue
                            if str(item.get("job_id") or "").strip() == job_id:
                                selected_run = item
                                break
                        if not selected_run:
                            await asyncio.sleep(max(0.2, poll_s))
                            continue
                        status_value = str(selected_run.get("status") or "").strip().upper()
                        last_status = status_value
                        if status_value in {"SUCCESS", "FAILED"}:
                            output_json = selected_run.get("output_json") if isinstance(selected_run.get("output_json"), dict) else {}
                            trimmed = _trim_build_output(output_json, max_rows=6)
                            masked = mask_pii(trimmed)
                            result = {
                                "status": status_value.lower(),
                                "job_id": job_id,
                                "reused_existing_job": reused_existing,
                                "artifact_id": output_json.get("artifact_id") if isinstance(output_json, dict) else None,
                                "output": masked,
                            }
                            # Include error details for failed jobs
                            if status_value == "FAILED":
                                result.update(_extract_spark_error_details(selected_run))
                            return result
                        await asyncio.sleep(max(0.2, poll_s))

                    return {
                        "status": "timeout",
                        "job_id": job_id,
                        "reused_existing_job": reused_existing,
                        "last_status": last_status,
                        "message": "build still running",
                    }

                if name == "pipeline_deploy_promote_build":
                    pipeline_id = str(arguments.get("pipeline_id") or "").strip()
                    if not pipeline_id:
                        return {"status": "invalid", "errors": ["pipeline_id is required"]}
                    build_job_id = str(arguments.get("build_job_id") or "").strip()
                    if not build_job_id:
                        return {"status": "invalid", "errors": ["build_job_id is required"]}
                    node_id = str(arguments.get("node_id") or "").strip()
                    if not node_id:
                        return {"status": "invalid", "errors": ["node_id is required"]}
                    db_name = str(arguments.get("db_name") or "").strip()
                    dataset_name = str(arguments.get("dataset_name") or "").strip()
                    if not db_name or not dataset_name:
                        return {"status": "invalid", "errors": ["db_name and dataset_name are required"]}
                    principal_id = str(arguments.get("principal_id") or "").strip() or None
                    principal_type = str(arguments.get("principal_type") or "").strip() or None
                    branch = str(arguments.get("branch") or "").strip() or None
                    definition_json = (
                        arguments.get("definition_json") if isinstance(arguments.get("definition_json"), dict) else None
                    )
                    pipeline_spec_commit_id = str(arguments.get("pipeline_spec_commit_id") or "").strip() or None

                    # Deploy hash mismatches happen when the pipeline definition changes between build and deploy.
                    # Prefer the exact build snapshot (pipeline_spec_commit_id -> pipeline_versions.definition_json).
                    if not definition_json:
                        if not pipeline_spec_commit_id:
                            runs = await _bff_json(
                                "GET",
                                f"/pipelines/{pipeline_id}/runs",
                                db_name=db_name,
                                principal_id=principal_id,
                                principal_type=principal_type,
                                params={"limit": 50},
                                timeout_seconds=15.0,
                            )
                            if not runs.get("error"):
                                runs_data = runs.get("data") if isinstance(runs.get("data"), dict) else {}
                                run_list = runs_data.get("runs") if isinstance(runs_data.get("runs"), list) else []
                                selected_run: Optional[Dict[str, Any]] = None
                                for item in run_list:
                                    if not isinstance(item, dict):
                                        continue
                                    if str(item.get("job_id") or "").strip() == build_job_id:
                                        selected_run = item
                                        break
                                if selected_run:
                                    output_json = (
                                        selected_run.get("output_json")
                                        if isinstance(selected_run.get("output_json"), dict)
                                        else {}
                                    )
                                    pipeline_spec_commit_id = (
                                        str(
                                            selected_run.get("pipeline_spec_commit_id")
                                            or output_json.get("pipeline_spec_commit_id")
                                            or ""
                                        ).strip()
                                        or None
                                    )
                        if pipeline_spec_commit_id:
                            try:
                                pipeline_registry = await self._ensure_pipeline_registry()
                                version = await pipeline_registry.get_version(
                                    pipeline_id=pipeline_id,
                                    lakefs_commit_id=pipeline_spec_commit_id,
                                    branch=branch,
                                )
                                if version:
                                    definition_json = dict(version.definition_json or {})
                            except Exception as exc:
                                logger.warning(
                                    "pipeline_deploy_promote_build: failed to fetch pipeline version snapshot pipeline_id=%s commit=%s: %s",
                                    pipeline_id,
                                    pipeline_spec_commit_id,
                                    exc,
                                )

                    payload: Dict[str, Any] = {
                        "promote_build": True,
                        "build_job_id": build_job_id,
                        "node_id": node_id,
                        "output": {"db_name": db_name, "dataset_name": dataset_name},
                        "replay_on_deploy": bool(arguments.get("replay_on_deploy") or False),
                    }
                    if definition_json:
                        payload["definition_json"] = definition_json
                    artifact_id = str(arguments.get("artifact_id") or "").strip() or None
                    if artifact_id:
                        payload["artifact_id"] = artifact_id
                    if branch:
                        payload["branch"] = branch

                    resp = await _bff_json(
                        "POST",
                        f"/pipelines/{pipeline_id}/deploy",
                        db_name=db_name,
                        principal_id=principal_id,
                        principal_type=principal_type,
                        json_body=payload,
                        timeout_seconds=60.0,
                    )
                    if resp.get("error"):
                        # Common and expected race: deploy called before build completes.
                        if resp.get("status_code") == 409 and isinstance(resp.get("response"), dict):
                            detail = resp.get("response", {}).get("detail")
                            if isinstance(detail, dict) and str(detail.get("code") or "").strip() == "BUILD_NOT_SUCCESS":
                                return {
                                    "status": "not_ready",
                                    "pipeline_id": pipeline_id,
                                    "build_job_id": build_job_id,
                                    "build_status": detail.get("build_status") or detail.get("buildStatus"),
                                    "errors": detail.get("errors"),
                                    "message": detail.get("message") or "Build is not successful yet",
                                }
                            if isinstance(detail, dict) and str(detail.get("code") or "").strip() == "REPLAY_REQUIRED":
                                # Deploy can be blocked when the target dataset exists and the deploy would change
                                # its schema/contract. The caller must either:
                                # - retry with replay_on_deploy=true, or
                                # - choose a new dataset_name.
                                return {
                                    "status": "replay_required",
                                    "pipeline_id": pipeline_id,
                                    "build_job_id": build_job_id,
                                    "node_id": node_id,
                                    "db_name": db_name,
                                    "dataset_name": dataset_name,
                                    "code": "REPLAY_REQUIRED",
                                    "message": detail.get("message") or resp.get("error") or "Replay is required to deploy",
                                    "detail": detail,
                                    "hint": "Retry with replay_on_deploy=true OR deploy to a new dataset_name.",
                                }
                            if isinstance(detail, str) and "definition" in detail.lower() and "match" in detail.lower():
                                return {
                                    "status": "conflict",
                                    "pipeline_id": pipeline_id,
                                    "code": "DEFINITION_MISMATCH",
                                    "message": detail,
                                    "hint": "Pass definition_json (exact build snapshot) or pipeline_spec_commit_id from the build output.",
                                }
                        return resp
                    data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
                    outputs = data.get("outputs") or []

                    # Extract dataset_ids from outputs for easier downstream use
                    dataset_ids: List[str] = []
                    dataset_version_ids: List[str] = []
                    for output in outputs:
                        if isinstance(output, dict):
                            ds_id = str(output.get("dataset_id") or "").strip()
                            dv_id = str(output.get("dataset_version_id") or "").strip()
                            if ds_id:
                                dataset_ids.append(ds_id)
                            if dv_id:
                                dataset_version_ids.append(dv_id)

                    return {
                        "status": "success",
                        "pipeline_id": data.get("pipeline_id") or pipeline_id,
                        "job_id": data.get("job_id"),
                        "deployed_commit_id": data.get("deployed_commit_id"),
                        "artifact_id": data.get("artifact_id"),
                        "outputs": outputs,
                        # Critical: Expose dataset_ids at top level for Agent to easily chain to objectify
                        "dataset_ids": dataset_ids,
                        "dataset_version_ids": dataset_version_ids,
                        "definition_included": bool(definition_json),
                        "pipeline_spec_commit_id": pipeline_spec_commit_id,
                    }

                # ── Debugging Tools ──────────────────────────────────────────
                # Note: debug_get_errors, debug_get_execution_log, debug_explain_failure
                # are handled in the agent loop (they need access to agent state).

                if name == "debug_inspect_node":
                    plan = arguments.get("plan") or {}
                    node_id = str(arguments.get("node_id") or "").strip()
                    if not node_id:
                        return _tool_error("node_id is required")

                    definition = plan.get("definition_json") or {}
                    nodes = definition.get("nodes") or []
                    edges = definition.get("edges") or []

                    # Find the target node
                    node = next((n for n in nodes if n.get("id") == node_id), None)
                    if not node:
                        available_nodes = [n.get("id") for n in nodes if n.get("id")]
                        payload = _tool_error(
                            f"Node '{node_id}' not found in plan",
                            status_code=404,
                            code=ErrorCode.RESOURCE_NOT_FOUND,
                            category=ErrorCategory.RESOURCE,
                        )
                        payload["available_nodes"] = available_nodes[:20]
                        return payload

                    # Find input and output edges
                    input_edges = [e for e in edges if e.get("target") == node_id]
                    output_edges = [e for e in edges if e.get("source") == node_id]

                    result: Dict[str, Any] = {
                        "status": "success",
                        "node_id": node_id,
                        "node_type": node.get("type"),
                        "node_config": node.get("data") or node.get("config") or {},
                        "inputs": [
                            {"from_node": e.get("source"), "handle": e.get("sourceHandle")}
                            for e in input_edges
                        ],
                        "outputs": [
                            {"to_node": e.get("target"), "handle": e.get("targetHandle")}
                            for e in output_edges
                        ],
                        "input_count": len(input_edges),
                        "output_count": len(output_edges),
                    }

                    # Include sample data if requested and available
                    if arguments.get("include_sample"):
                        node_data = node.get("data") or {}
                        if node_data.get("preview"):
                            result["sample_data"] = node_data.get("preview")

                    return result

                if name == "debug_dry_run":
                    plan = arguments.get("plan") or {}
                    check_joins = arguments.get("check_joins", True)

                    errors: List[str] = []
                    warnings: List[str] = []

                    # Basic structure validation
                    if not plan:
                        errors.append("Plan is empty")
                        return {"status": "invalid", "errors": errors, "warnings": warnings, "dry_run_passed": False}

                    definition = plan.get("definition_json") or {}
                    nodes = definition.get("nodes") or []
                    edges = definition.get("edges") or []

                    if not nodes:
                        errors.append("Plan has no nodes")
                    if not edges and len(nodes) > 1:
                        warnings.append("Plan has multiple nodes but no edges connecting them")

                    # Check for required node types
                    node_types = [n.get("type") for n in nodes]
                    has_input = any(t in ("input", "dataset_input", "source") for t in node_types)
                    has_output = any(t in ("output", "dataset_output", "sink") for t in node_types)

                    if not has_input:
                        errors.append("Plan has no input/source node")
                    if not has_output:
                        warnings.append("Plan has no output/sink node")

                    # Check node references in edges
                    node_ids = {n.get("id") for n in nodes if n.get("id")}
                    for edge in edges:
                        source = edge.get("source")
                        target = edge.get("target")
                        if source and source not in node_ids:
                            errors.append(f"Edge references non-existent source node: {source}")
                        if target and target not in node_ids:
                            errors.append(f"Edge references non-existent target node: {target}")

                    # Check join nodes if requested
                    if check_joins:
                        for node in nodes:
                            if node.get("type") == "join":
                                node_data = node.get("data") or {}
                                left_key = node_data.get("left_key") or node_data.get("leftKey")
                                right_key = node_data.get("right_key") or node_data.get("rightKey")
                                if not left_key:
                                    errors.append(f"Join node '{node.get('id')}' missing left_key")
                                if not right_key:
                                    errors.append(f"Join node '{node.get('id')}' missing right_key")

                    # Check for orphan nodes (no incoming or outgoing edges)
                    for node in nodes:
                        nid = node.get("id")
                        ntype = node.get("type") or ""
                        has_incoming = any(e.get("target") == nid for e in edges)
                        has_outgoing = any(e.get("source") == nid for e in edges)

                        # Input nodes don't need incoming edges
                        if ntype in ("input", "dataset_input", "source") and not has_outgoing:
                            warnings.append(f"Input node '{nid}' has no outgoing edges")
                        # Output nodes don't need outgoing edges
                        elif ntype in ("output", "dataset_output", "sink") and not has_incoming:
                            warnings.append(f"Output node '{nid}' has no incoming edges")
                        # Other nodes should have both
                        elif ntype not in ("input", "dataset_input", "source", "output", "dataset_output", "sink"):
                            if not has_incoming and not has_outgoing:
                                warnings.append(f"Node '{nid}' is orphaned (no edges)")

                    return {
                        "status": "success" if not errors else "invalid",
                        "errors": errors,
                        "warnings": warnings,
                        "dry_run_passed": not errors,
                        "node_count": len(nodes),
                        "edge_count": len(edges),
                    }

                # ── Objectify Tool Handlers ──────────────────────────────────────
                if name == "objectify_suggest_mapping":
                    dataset_id = str(arguments.get("dataset_id") or "").strip()
                    target_class_id = str(arguments.get("target_class_id") or "").strip()
                    db_name = str(arguments.get("db_name") or "").strip()
                    branch = str(arguments.get("branch") or "main").strip()

                    if not dataset_id or not target_class_id or not db_name:
                        return _missing_required_params("objectify_suggest_mapping", ["dataset_id", "target_class_id", "db_name"], arguments)

                    dataset_registry, _ = await self._ensure_registries()
                    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
                    if not dataset:
                        return _tool_error(
                            f"Dataset not found: {dataset_id}",
                            status_code=404,
                            code=ErrorCode.RESOURCE_NOT_FOUND,
                            category=ErrorCategory.RESOURCE,
                        )

                    # Get dataset schema columns
                    schema_json = dataset.schema_json or {}
                    columns = schema_json.get("columns", [])
                    source_columns = []
                    source_types: Dict[str, str] = {}
                    for col in columns:
                        if isinstance(col, dict):
                            col_name = str(col.get("name") or col.get("column") or "").strip()
                            col_type = str(col.get("type") or col.get("data_type") or "xsd:string").strip()
                        else:
                            col_name = str(col).strip()
                            col_type = "xsd:string"
                        if col_name:
                            source_columns.append(col_name)
                            source_types[col_name] = col_type

                    # Fetch target class properties from OMS
                    target_properties: List[Dict[str, Any]] = []
                    try:
                        base_url = _bff_api_base_url()
                        async with httpx.AsyncClient(timeout=30.0) as client:
                            resp = await client.get(
                                f"{base_url}/databases/{db_name}/ontology/classes/{target_class_id}",
                                params={"branch": branch},
                                headers={"X-Admin-Token": os.environ.get("ADMIN_TOKEN", "")},
                            )
                            if resp.status_code == 200:
                                data = resp.json().get("data", {})
                                target_properties = data.get("properties", [])
                    except Exception as exc:
                        logger.warning("Failed to fetch class properties: %s", exc)

                    # Build suggested mappings using name matching heuristics
                    suggestions: List[Dict[str, Any]] = []
                    target_prop_names = {str(p.get("name") or "").strip().lower(): p for p in target_properties if isinstance(p, dict)}

                    for src_col in source_columns:
                        src_lower = src_col.lower().replace("_", "").replace("-", "")
                        best_match = None
                        confidence = 0.0

                        for tgt_name, tgt_prop in target_prop_names.items():
                            tgt_lower = tgt_name.replace("_", "").replace("-", "")

                            # Exact match
                            if src_lower == tgt_lower:
                                best_match = tgt_prop.get("name")
                                confidence = 1.0
                                break
                            # Contains match
                            if src_lower in tgt_lower or tgt_lower in src_lower:
                                if confidence < 0.7:
                                    best_match = tgt_prop.get("name")
                                    confidence = 0.7
                            # Common suffixes (id, name, date, etc.)
                            for suffix in ["id", "name", "date", "time", "count", "amount", "price", "email", "phone"]:
                                if src_lower.endswith(suffix) and tgt_lower.endswith(suffix):
                                    if confidence < 0.5:
                                        best_match = tgt_prop.get("name")
                                        confidence = 0.5

                        suggestions.append({
                            "source_field": src_col,
                            "source_type": source_types.get(src_col, "xsd:string"),
                            "target_field": best_match,
                            "target_type": target_prop_names.get((best_match or "").lower(), {}).get("type"),
                            "confidence": confidence,
                            "auto_mapped": best_match is not None,
                        })

                    mapped_count = sum(1 for s in suggestions if s.get("auto_mapped"))
                    return {
                        "status": "success",
                        "dataset_id": dataset_id,
                        "target_class_id": target_class_id,
                        "suggestions": suggestions,
                        "source_columns": source_columns,
                        "target_properties": [p.get("name") for p in target_properties if isinstance(p, dict)],
                        "summary": {
                            "total_source_columns": len(source_columns),
                            "total_target_properties": len(target_properties),
                            "auto_mapped": mapped_count,
                            "unmapped": len(source_columns) - mapped_count,
                        },
                    }

                if name == "objectify_create_mapping_spec":
                    dataset_id = str(arguments.get("dataset_id") or "").strip()
                    target_class_id = str(arguments.get("target_class_id") or "").strip()
                    mappings = arguments.get("mappings") or []
                    db_name = str(arguments.get("db_name") or "").strip()
                    dataset_branch = str(arguments.get("dataset_branch") or "").strip() or None
                    auto_sync = bool(arguments.get("auto_sync", True))
                    options = arguments.get("options") or {}

                    if not dataset_id or not target_class_id or not mappings or not db_name:
                        return _missing_required_params("objectify_create_mapping_spec", ["dataset_id", "target_class_id", "mappings", "db_name"], arguments)

                    # Validate mappings format
                    normalized_mappings: List[Dict[str, str]] = []
                    for m in mappings:
                        if not isinstance(m, dict):
                            continue
                        src = str(m.get("source_field") or "").strip()
                        tgt = str(m.get("target_field") or "").strip()
                        if src and tgt:
                            normalized_mappings.append({"source_field": src, "target_field": tgt})

                    if not normalized_mappings:
                        return _tool_error("No valid mappings provided")

                    # Get dataset info
                    dataset_registry, _ = await self._ensure_registries()
                    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
                    if not dataset:
                        return _tool_error(
                            f"Dataset not found: {dataset_id}",
                            status_code=404,
                            code=ErrorCode.RESOURCE_NOT_FOUND,
                            category=ErrorCategory.RESOURCE,
                        )

                    objectify_registry = await self._ensure_objectify_registry()

                    # Create mapping spec
                    from uuid import uuid4
                    from shared.utils.schema_hash import compute_schema_hash

                    # Try to get schema_hash from dataset.schema_json first
                    schema_json = dataset.schema_json or {}
                    schema_columns = schema_json.get("columns", [])
                    schema_hash = compute_schema_hash(schema_columns) if schema_columns else None

                    # If not available, try to get from latest version
                    if not schema_hash:
                        latest_version = await dataset_registry.get_latest_version(dataset_id=dataset_id)
                        if latest_version:
                            # Version has schema_hash attribute
                            schema_hash = getattr(latest_version, "schema_hash", None)
                            # If still not available, try to compute from version's sample_json
                            if not schema_hash and hasattr(latest_version, "sample_json"):
                                sample_json = latest_version.sample_json or {}
                                sample_columns = sample_json.get("columns", [])
                                schema_hash = compute_schema_hash(sample_columns) if sample_columns else None

                    if not schema_hash:
                        payload = _tool_error(
                            "Cannot determine schema_hash for dataset",
                            status_code=422,
                            code=ErrorCode.REQUEST_VALIDATION_FAILED,
                            category=ErrorCategory.INPUT,
                            context={"dataset_id": dataset_id},
                        )
                        payload["hint"] = "Dataset has no schema information. Try uploading a new version with schema."
                        payload["dataset_id"] = dataset_id
                        return payload

                    mapping_spec = await objectify_registry.create_mapping_spec(
                        dataset_id=dataset_id,
                        dataset_branch=dataset_branch or dataset.branch,
                        artifact_output_name=dataset.name,
                        schema_hash=schema_hash,
                        target_class_id=target_class_id,
                        mappings=normalized_mappings,
                        auto_sync=auto_sync,
                        status="ACTIVE",
                        options=options,
                    )

                    return {
                        "status": "success",
                        "mapping_spec_id": mapping_spec.mapping_spec_id,
                        "dataset_id": dataset_id,
                        "target_class_id": target_class_id,
                        "mappings_count": len(normalized_mappings),
                        "auto_sync": auto_sync,
                        "schema_hash": schema_hash,
                        "message": f"Mapping spec created. Use objectify_run to execute transformation.",
                    }

                if name == "objectify_list_mapping_specs":
                    dataset_id = str(arguments.get("dataset_id") or "").strip()
                    limit = int(arguments.get("limit") or 50)

                    if not dataset_id:
                        return _missing_required_params("objectify_list_mapping_specs", ["dataset_id"], arguments)

                    objectify_registry = await self._ensure_objectify_registry()
                    specs = await objectify_registry.list_mapping_specs(dataset_id=dataset_id, limit=limit)

                    return {
                        "status": "success",
                        "dataset_id": dataset_id,
                        "mapping_specs": [
                            {
                                "mapping_spec_id": s.mapping_spec_id,
                                "target_class_id": s.target_class_id,
                                "status": s.status,
                                "auto_sync": s.auto_sync,
                                "version": s.version,
                                "created_at": s.created_at.isoformat() if s.created_at else None,
                            }
                            for s in specs
                        ],
                        "count": len(specs),
                    }

                if name == "objectify_run":
                    dataset_id = str(arguments.get("dataset_id") or "").strip()
                    mapping_spec_id = str(arguments.get("mapping_spec_id") or "").strip() or None
                    dataset_version_id = str(arguments.get("dataset_version_id") or "").strip() or None
                    db_name = str(arguments.get("db_name") or "").strip()
                    max_rows = arguments.get("max_rows")
                    batch_size = arguments.get("batch_size")

                    if not dataset_id or not db_name:
                        return _missing_required_params("objectify_run", ["dataset_id", "db_name"], arguments)

                    dataset_registry, _ = await self._ensure_registries()
                    objectify_registry = await self._ensure_objectify_registry()

                    # Get dataset
                    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
                    if not dataset:
                        return _tool_error(
                            f"Dataset not found: {dataset_id}",
                            status_code=404,
                            code=ErrorCode.RESOURCE_NOT_FOUND,
                            category=ErrorCategory.RESOURCE,
                        )

                    # Get or find mapping spec
                    mapping_spec = None
                    if mapping_spec_id:
                        mapping_spec = await objectify_registry.get_mapping_spec(mapping_spec_id=mapping_spec_id)
                    else:
                        # Find active mapping spec for dataset
                        specs = await objectify_registry.list_mapping_specs(dataset_id=dataset_id, limit=10)
                        active_specs = [s for s in specs if s.status == "ACTIVE" and s.auto_sync]
                        if active_specs:
                            mapping_spec = await objectify_registry.get_mapping_spec(mapping_spec_id=active_specs[0].mapping_spec_id)

                    if not mapping_spec:
                        payload = _tool_error(
                            "No active mapping spec found for dataset",
                            status_code=404,
                            code=ErrorCode.RESOURCE_NOT_FOUND,
                            category=ErrorCategory.RESOURCE,
                            external_code="MAPPING_SPEC_NOT_FOUND",
                            context={"dataset_id": dataset_id},
                        )
                        payload["hint"] = "Use objectify_create_mapping_spec to create a mapping first"
                        return payload

                    # Get dataset version
                    if dataset_version_id:
                        version = await dataset_registry.get_version(version_id=dataset_version_id)
                    else:
                        version = await dataset_registry.get_latest_version(dataset_id=dataset_id)

                    if not version:
                        return _tool_error(
                            "No dataset version found",
                            status_code=404,
                            code=ErrorCode.RESOURCE_NOT_FOUND,
                            category=ErrorCategory.RESOURCE,
                            external_code="DATASET_VERSION_MISSING",
                            context={"dataset_id": dataset_id},
                        )

                    # Create objectify job
                    from uuid import uuid4

                    job_id = str(uuid4())
                    dedupe_key = objectify_registry.build_dedupe_key(
                        dataset_id=dataset_id,
                        dataset_branch=dataset.branch,
                        mapping_spec_id=mapping_spec.mapping_spec_id,
                        mapping_spec_version=mapping_spec.version,
                        dataset_version_id=version.version_id,
                        artifact_id=None,
                        artifact_output_name=dataset.name,
                    )

                    # Check for existing job with same dedupe key
                    existing = await objectify_registry.get_objectify_job_by_dedupe_key(dedupe_key=dedupe_key)
                    if existing:
                        return {
                            "status": "already_exists",
                            "job_id": existing.job_id,
                            "job_status": existing.status,
                            "message": "An objectify job already exists for this dataset version and mapping spec",
                        }

                    options: Dict[str, Any] = dict(mapping_spec.options or {})
                    if max_rows is not None:
                        options["max_rows"] = int(max_rows)
                    if batch_size is not None:
                        options["batch_size"] = int(batch_size)

                    job = ObjectifyJob(
                        job_id=job_id,
                        db_name=db_name,
                        dataset_id=dataset_id,
                        dataset_version_id=version.version_id,
                        artifact_output_name=dataset.name,
                        dedupe_key=dedupe_key,
                        dataset_branch=dataset.branch,
                        artifact_key=version.artifact_key or "",
                        mapping_spec_id=mapping_spec.mapping_spec_id,
                        mapping_spec_version=mapping_spec.version,
                        target_class_id=mapping_spec.target_class_id,
                        ontology_branch=options.get("ontology_branch"),
                        max_rows=options.get("max_rows"),
                        batch_size=options.get("batch_size"),
                        allow_partial=bool(options.get("allow_partial")),
                        options=options,
                    )

                    await objectify_registry.enqueue_objectify_job(job=job)

                    return {
                        "status": "success",
                        "job_id": job_id,
                        "dataset_id": dataset_id,
                        "dataset_version_id": version.version_id,
                        "mapping_spec_id": mapping_spec.mapping_spec_id,
                        "target_class_id": mapping_spec.target_class_id,
                        "message": "Objectify job enqueued. Use objectify_get_status to check progress.",
                    }

                if name == "objectify_get_status":
                    job_id = str(arguments.get("job_id") or "").strip()

                    if not job_id:
                        return _missing_required_params("objectify_get_status", ["job_id"], arguments)

                    objectify_registry = await self._ensure_objectify_registry()
                    job = await objectify_registry.get_objectify_job(job_id=job_id)

                    if not job:
                        return _tool_error(
                            f"Objectify job not found: {job_id}",
                            status_code=404,
                            code=ErrorCode.RESOURCE_NOT_FOUND,
                            category=ErrorCategory.RESOURCE,
                            external_code="OBJECTIFY_JOB_NOT_FOUND",
                            context={"job_id": job_id},
                        )

                    # Extract statistics from report if available
                    report = job.report or {}
                    return {
                        "status": "success",
                        "job_id": job.job_id,
                        "job_status": job.status,
                        "dataset_id": job.dataset_id,
                        "target_class_id": job.target_class_id,
                        "created_at": job.created_at.isoformat() if job.created_at else None,
                        "updated_at": job.updated_at.isoformat() if job.updated_at else None,
                        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                        "error": job.error,
                        "rows_processed": report.get("rows_processed"),
                        "rows_failed": report.get("rows_failed"),
                        "instances_created": report.get("instances_created"),
                    }

                if name == "objectify_wait":
                    job_id = str(arguments.get("job_id") or "").strip()
                    timeout_seconds = float(arguments.get("timeout_seconds") or 300)
                    poll_interval = float(arguments.get("poll_interval_seconds") or 2)

                    if not job_id:
                        return _missing_required_params("objectify_wait", ["job_id"], arguments)

                    objectify_registry = await self._ensure_objectify_registry()

                    # Poll until completion or timeout
                    elapsed = 0.0
                    final_statuses = {"completed", "failed", "cancelled"}

                    while elapsed < timeout_seconds:
                        job = await objectify_registry.get_objectify_job(job_id=job_id)

                        if not job:
                            return _tool_error(
                                f"Objectify job not found: {job_id}",
                                status_code=404,
                                code=ErrorCode.RESOURCE_NOT_FOUND,
                                category=ErrorCategory.RESOURCE,
                                external_code="OBJECTIFY_JOB_NOT_FOUND",
                                context={"job_id": job_id},
                            )

                        job_status = (job.status or "").lower()

                        if job_status in final_statuses:
                            # Extract statistics from report if available
                            report = job.report or {}
                            return {
                                "status": "success" if job_status == "completed" else "failed",
                                "job_id": job.job_id,
                                "job_status": job.status,
                                "dataset_id": job.dataset_id,
                                "target_class_id": job.target_class_id,
                                "created_at": job.created_at.isoformat() if job.created_at else None,
                                "updated_at": job.updated_at.isoformat() if job.updated_at else None,
                                "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                                "error": job.error,
                                "rows_processed": report.get("rows_processed"),
                                "rows_failed": report.get("rows_failed"),
                                "instances_created": report.get("instances_created"),
                                "wait_elapsed_seconds": elapsed,
                            }

                        await asyncio.sleep(poll_interval)
                        elapsed += poll_interval

                    # Timeout reached
                    job = await objectify_registry.get_objectify_job(job_id=job_id)
                    return {
                        "status": "timeout",
                        "error": f"Objectify job did not complete within {timeout_seconds}s",
                        "job_id": job_id,
                        "job_status": job.status if job else "unknown",
                        "wait_elapsed_seconds": elapsed,
                        "hint": "Job may still be running. Call objectify_wait again or use objectify_get_status to check.",
                    }

                if name == "ontology_register_object_type":
                    logger.info("ontology_register_object_type handler reached! name=%s", name)
                    db_name = str(arguments.get("db_name") or "").strip()
                    class_id = str(arguments.get("class_id") or "").strip()
                    dataset_id = str(arguments.get("dataset_id") or "").strip()
                    primary_key = arguments.get("primary_key") or []
                    title_key = arguments.get("title_key") or []
                    branch = str(arguments.get("branch") or "main").strip()

                    if not db_name or not class_id or not dataset_id:
                        return _missing_required_params(
                            "ontology_register_object_type",
                            ["db_name", "class_id", "dataset_id", "primary_key", "title_key"],
                            arguments,
                        )

                    # Normalize key fields
                    pk_list = [str(k).strip() for k in primary_key if str(k).strip()] if isinstance(primary_key, list) else [str(primary_key).strip()]
                    title_list = [str(k).strip() for k in title_key if str(k).strip()] if isinstance(title_key, list) else [str(title_key).strip()]

                    if not pk_list:
                        return _tool_error("primary_key must be a non-empty list of field names")
                    if not title_list:
                        return _tool_error("title_key must be a non-empty list of field names")

                    try:
                        # First, get the current head commit for optimistic concurrency
                        # Use /head endpoint (same as objectify_worker)
                        head_resp = await _oms_json(
                            "GET",
                            f"/api/v1/version/{db_name}/head",
                            params={"branch": branch},
                        )
                        head_data = head_resp.get("data") if isinstance(head_resp.get("data"), dict) else {}
                        head_commit = head_data.get("head_commit_id") or head_data.get("commit") or head_data.get("head_commit") or ""
                        if not head_commit:
                            payload = _tool_error(
                                f"No head commit found for branch '{branch}'. Please create an ontology class first.",
                                status_code=404,
                                code=ErrorCode.RESOURCE_NOT_FOUND,
                                category=ErrorCategory.RESOURCE,
                            )
                            return payload

                        # Build the object_type resource payload
                        object_type_payload: Dict[str, Any] = {
                            "id": class_id,
                            "label": class_id,
                            "description": f"Object type contract for {class_id}",
                            "spec": {
                                "status": "ACTIVE",
                                "pk_spec": {
                                    "primary_key": pk_list,
                                    "title_key": title_list,
                                },
                                "backing_source": {
                                    "dataset_id": dataset_id,
                                },
                            },
                        }

                        # Create or update the object_type resource
                        # Try POST first (create new), fall back to PUT (update existing)
                        resp = await _oms_json(
                            "POST",
                            f"/api/v1/database/{db_name}/ontology/resources/object_type",
                            params={"branch": branch, "expected_head_commit": head_commit},
                            json_body=object_type_payload,
                            timeout_seconds=30.0,
                        )

                        # If POST fails with 409 Conflict (already exists), try PUT to update
                        if resp.get("error") and "409" in str(resp.get("error")):
                            logger.info("object_type exists, updating with PUT")
                            resp = await _oms_json(
                                "PUT",
                                f"/api/v1/database/{db_name}/ontology/resources/object_type/{class_id}",
                                params={"branch": branch, "expected_head_commit": head_commit},
                                json_body=object_type_payload,
                                timeout_seconds=30.0,
                            )

                        if resp.get("error"):
                            payload = _tool_error(
                                str(resp.get("error") or "OMS request failed"),
                                status_code=502,
                                code=ErrorCode.UPSTREAM_ERROR,
                                category=ErrorCategory.UPSTREAM,
                                context={"db_name": db_name, "class_id": class_id},
                            )
                            payload["db_name"] = db_name
                            payload["class_id"] = class_id
                            return payload

                        return {
                            "status": "success",
                            "message": f"Object type '{class_id}' registered successfully",
                            "db_name": db_name,
                            "class_id": class_id,
                            "dataset_id": dataset_id,
                            "primary_key": pk_list,
                            "title_key": title_list,
                            "hint": "You can now run objectify_run to create instances",
                        }

                    except Exception as exc:
                        logger.error("ontology_register_object_type failed: %s", exc)
                        payload = _tool_error(
                            str(exc),
                            status_code=500,
                            code=ErrorCode.INTERNAL_ERROR,
                            category=ErrorCategory.INTERNAL,
                            context={"db_name": db_name, "class_id": class_id},
                        )
                        payload["db_name"] = db_name
                        payload["class_id"] = class_id
                        return payload

                if name == "ontology_query_instances":
                    db_name = str(arguments.get("db_name") or "").strip()
                    class_id = str(arguments.get("class_id") or "").strip()
                    limit = int(arguments.get("limit") or 10)
                    limit = max(1, min(limit, 100))
                    branch = str(arguments.get("branch") or "main").strip()
                    filters = arguments.get("filters") or {}

                    if not db_name or not class_id:
                        return _missing_required_params("ontology_query_instances", ["db_name", "class_id"], arguments)

                    # Build simple graph query request
                    query_body: Dict[str, Any] = {
                        "class_type": class_id,
                        "limit": limit,
                    }
                    if filters and isinstance(filters, dict):
                        query_body["filters"] = filters

                    try:
                        resp = await _bff_json(
                            "POST",
                            f"/graph-query/{db_name}/simple",
                            json_body=query_body,
                            params={"base_branch": branch},
                            timeout_seconds=30.0,
                        )

                        if resp.get("error"):
                            payload = _tool_error(
                                str(resp.get("error") or "Query failed"),
                                status_code=502,
                                code=ErrorCode.UPSTREAM_ERROR,
                                category=ErrorCategory.UPSTREAM,
                                context={"db_name": db_name, "class_id": class_id},
                            )
                            payload["db_name"] = db_name
                            payload["class_id"] = class_id
                            return payload

                        data = resp.get("data") if isinstance(resp.get("data"), dict) else resp
                        instances = data.get("instances") or data.get("results") or data.get("rows") or []
                        total_count = data.get("total_count") or data.get("count") or len(instances)

                        # Mask PII in sample instances
                        masked_instances = mask_pii(instances[:limit]) if instances else []

                        return {
                            "status": "success",
                            "db_name": db_name,
                            "class_id": class_id,
                            "branch": branch,
                            "total_count": total_count,
                            "returned_count": len(masked_instances),
                            "instances": masked_instances,
                        }
                    except Exception as exc:
                        logger.warning("ontology_query_instances failed: %s", exc)
                        payload = _tool_error(
                            f"Query failed: {str(exc)[:200]}",
                            status_code=500,
                            code=ErrorCode.INTERNAL_ERROR,
                            category=ErrorCategory.INTERNAL,
                            context={"db_name": db_name, "class_id": class_id},
                        )
                        payload["db_name"] = db_name
                        payload["class_id"] = class_id
                        return payload

                # ── Enterprise Features: FK Detection, Incremental Objectify, Schema Drift ──
                if name == "detect_foreign_keys":
                    db_name = str(arguments.get("db_name") or "").strip()
                    dataset_id = str(arguments.get("dataset_id") or "").strip()
                    confidence_threshold = float(arguments.get("confidence_threshold") or 0.6)
                    include_sample = bool(arguments.get("include_sample_analysis", True))
                    branch = str(arguments.get("branch") or "main").strip()

                    if not db_name or not dataset_id:
                        return _missing_required_params("detect_foreign_keys", ["db_name", "dataset_id"], arguments)

                    try:
                        from shared.services.pipeline.fk_pattern_detector import (
                            ForeignKeyPatternDetector,
                            FKDetectionConfig,
                            TargetCandidate,
                        )

                        dataset_registry, _ = await self._ensure_registries()
                        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)

                        if not dataset:
                            return _tool_error(
                                f"Dataset not found: {dataset_id}",
                                status_code=404,
                                code=ErrorCode.RESOURCE_NOT_FOUND,
                                category=ErrorCategory.RESOURCE,
                            )

                        schema = dataset.schema_json or {}
                        columns = schema.get("columns") or schema.get("fields") or []

                        # Get other datasets/object_types as target candidates
                        datasets = await dataset_registry.list_datasets(db_name=db_name, branch=branch, limit=100)
                        target_candidates = []
                        for ds in datasets:
                            if ds.dataset_id == dataset_id:
                                continue
                            ds_schema = ds.schema_json or {}
                            ds_columns = ds_schema.get("columns") or ds_schema.get("fields") or []
                            pk_cols = [c.get("name") for c in ds_columns if c.get("name", "").lower() in ("id", "pk")]
                            if not pk_cols and ds_columns:
                                pk_cols = [ds_columns[0].get("name", "id")]
                            target_candidates.append(TargetCandidate(
                                candidate_type="dataset",
                                candidate_id=ds.dataset_id,
                                candidate_name=ds.name or ds.dataset_id,
                                pk_columns=pk_cols,
                            ))

                        config = FKDetectionConfig(min_confidence=confidence_threshold)
                        detector = ForeignKeyPatternDetector(config)
                        patterns = detector.detect_patterns(
                            source_dataset_id=dataset_id,
                            source_schema=columns,
                            target_candidates=target_candidates,
                        )

                        return {
                            "status": "success",
                            "dataset_id": dataset_id,
                            "db_name": db_name,
                            "patterns_found": len(patterns),
                            "patterns": [
                                {
                                    "source_column": p.source_column,
                                    "target_dataset_id": p.target_dataset_id,
                                    "target_object_type": p.target_object_type,
                                    "target_pk_field": p.target_pk_field,
                                    "confidence": p.confidence,
                                    "detection_method": p.detection_method,
                                    "reasons": p.reasons,
                                }
                                for p in patterns
                            ],
                        }
                    except Exception as exc:
                        logger.warning("detect_foreign_keys failed: %s", exc)
                        return _tool_error(
                            str(exc)[:300],
                            status_code=500,
                            code=ErrorCode.INTERNAL_ERROR,
                            category=ErrorCategory.INTERNAL,
                        )

                if name == "create_link_type_from_fk":
                    db_name = str(arguments.get("db_name") or "").strip()
                    fk_pattern = arguments.get("fk_pattern") or {}
                    source_class_id = str(arguments.get("source_class_id") or "").strip()
                    target_class_id = str(arguments.get("target_class_id") or "").strip()
                    predicate = str(arguments.get("predicate") or "").strip()
                    cardinality = str(arguments.get("cardinality") or "n:1").strip()
                    branch = str(arguments.get("branch") or "main").strip()

                    if not db_name or not fk_pattern or not source_class_id or not target_class_id:
                        return _missing_required_params(
                            "create_link_type_from_fk",
                            ["db_name", "fk_pattern", "source_class_id", "target_class_id"],
                            arguments,
                        )

                    # Generate predicate if not provided
                    if not predicate:
                        source_col = fk_pattern.get("source_column", "")
                        import re
                        name_part = re.sub(r"(_id|_fk|_key|Id|Fk)$", "", source_col)
                        predicate = "has" + "".join(w.capitalize() for w in name_part.split("_"))

                    try:
                        link_type_body = {
                            "predicate": predicate,
                            "source_class": source_class_id,
                            "target_class": target_class_id,
                            "cardinality": cardinality,
                            "relationship_spec": {
                                "spec_type": "foreign_key",
                                "source_column": fk_pattern.get("source_column"),
                                "target_pk_field": fk_pattern.get("target_pk_field") or "id",
                            },
                        }

                        resp = await _bff_json(
                            "POST",
                            f"/databases/{db_name}/link-types",
                            json_body=link_type_body,
                            params={"branch": branch},
                            timeout_seconds=30.0,
                        )

                        if resp.get("error"):
                            payload = _tool_error(
                                str(resp.get("error") or "BFF request failed"),
                                status_code=502,
                                code=ErrorCode.UPSTREAM_ERROR,
                                category=ErrorCategory.UPSTREAM,
                                context={"db_name": db_name},
                            )
                            payload["db_name"] = db_name
                            return payload

                        return {
                            "status": "success",
                            "link_type_created": True,
                            "predicate": predicate,
                            "source_class": source_class_id,
                            "target_class": target_class_id,
                            "cardinality": cardinality,
                        }
                    except Exception as exc:
                        logger.warning("create_link_type_from_fk failed: %s", exc)
                        return _tool_error(
                            str(exc)[:300],
                            status_code=500,
                            code=ErrorCode.INTERNAL_ERROR,
                            category=ErrorCategory.INTERNAL,
                        )

                if name == "trigger_incremental_objectify":
                    db_name = str(arguments.get("db_name") or "").strip()
                    mapping_spec_id = str(arguments.get("mapping_spec_id") or "").strip()
                    execution_mode = str(arguments.get("execution_mode") or "incremental").strip()
                    watermark_column = str(arguments.get("watermark_column") or "").strip()
                    force_full = bool(arguments.get("force_full_refresh", False))
                    branch = str(arguments.get("branch") or "main").strip()

                    if not db_name or not mapping_spec_id:
                        return _missing_required_params(
                            "trigger_incremental_objectify",
                            ["db_name", "mapping_spec_id"],
                            arguments,
                        )

                    try:
                        objectify_registry = await self._ensure_objectify_registry()

                        # Reset watermark if force_full
                        if force_full:
                            await objectify_registry.delete_watermark(
                                mapping_spec_id=mapping_spec_id,
                                dataset_branch=branch,
                            )

                        # Get current watermark
                        watermark = await objectify_registry.get_watermark(
                            mapping_spec_id=mapping_spec_id,
                            dataset_branch=branch,
                        )

                        # Build trigger request with execution mode
                        trigger_body = {
                            "execution_mode": execution_mode,
                            "watermark_column": watermark_column or (watermark.get("watermark_column") if watermark else None),
                            "previous_watermark": watermark.get("watermark_value") if watermark and not force_full else None,
                        }

                        resp = await _bff_json(
                            "POST",
                            f"/objectify/mapping-specs/{mapping_spec_id}/trigger",
                            json_body=trigger_body,
                            params={"branch": branch},
                            timeout_seconds=60.0,
                        )

	                        if resp.get("error"):
	                            return _tool_error(
	                                "Objectify trigger failed",
	                                detail=str(resp.get("error"))[:500],
	                                code=ErrorCode.UPSTREAM_ERROR,
	                                category=ErrorCategory.UPSTREAM,
	                                status_code=502,
	                                context={"mapping_spec_id": mapping_spec_id, "branch": branch},
	                            )

                        return {
                            "status": "success",
                            "mapping_spec_id": mapping_spec_id,
                            "execution_mode": execution_mode,
                            "watermark_column": trigger_body.get("watermark_column"),
                            "previous_watermark": trigger_body.get("previous_watermark"),
                            "job_id": resp.get("job_id") or resp.get("data", {}).get("job_id"),
                        }
	                    except Exception as exc:
	                        logger.warning("trigger_incremental_objectify failed: %s", exc)
	                        return _tool_error(
	                            "trigger_incremental_objectify failed",
	                            detail=str(exc)[:300],
	                            code=ErrorCode.INTERNAL_ERROR,
	                            category=ErrorCategory.INTERNAL,
	                            status_code=500,
	                            context={"mapping_spec_id": mapping_spec_id, "branch": branch},
	                        )

                if name == "get_objectify_watermark":
                    mapping_spec_id = str(arguments.get("mapping_spec_id") or "").strip()
                    dataset_branch = str(arguments.get("dataset_branch") or "main").strip()

                    if not mapping_spec_id:
                        return _missing_required_params("get_objectify_watermark", ["mapping_spec_id"], arguments)

                    try:
                        objectify_registry = await self._ensure_objectify_registry()
                        watermark = await objectify_registry.get_watermark(
                            mapping_spec_id=mapping_spec_id,
                            dataset_branch=dataset_branch,
                        )

                        if not watermark:
                            return {
                                "status": "not_found",
                                "mapping_spec_id": mapping_spec_id,
                                "message": "No watermark found - objectify has not run in incremental mode yet",
                            }

                        return {
                            "status": "success",
                            **watermark,
                        }
	                    except Exception as exc:
	                        logger.warning("get_objectify_watermark failed: %s", exc)
	                        return _tool_error(
	                            "get_objectify_watermark failed",
	                            detail=str(exc)[:300],
	                            code=ErrorCode.INTERNAL_ERROR,
	                            category=ErrorCategory.INTERNAL,
	                            status_code=500,
	                            context={"mapping_spec_id": mapping_spec_id, "dataset_branch": dataset_branch},
	                        )

                if name == "check_schema_drift":
                    db_name = str(arguments.get("db_name") or "").strip()
                    mapping_spec_id = str(arguments.get("mapping_spec_id") or "").strip()
                    dataset_version_id = str(arguments.get("dataset_version_id") or "").strip() or None

                    if not db_name or not mapping_spec_id:
                        return _missing_required_params("check_schema_drift", ["db_name", "mapping_spec_id"], arguments)

                    try:
                        from shared.services.core.schema_drift_detector import SchemaDriftDetector

                        objectify_registry = await self._ensure_objectify_registry()
                        dataset_registry, _ = await self._ensure_registries()

	                        spec = await objectify_registry.get_mapping_spec(mapping_spec_id=mapping_spec_id)
	                        if not spec:
	                            return _tool_error(
	                                f"Mapping spec not found: {mapping_spec_id}",
	                                code=ErrorCode.RESOURCE_NOT_FOUND,
	                                category=ErrorCategory.RESOURCE,
	                                status_code=404,
	                                context={"mapping_spec_id": mapping_spec_id},
	                            )

	                        dataset = await dataset_registry.get_dataset(dataset_id=str(spec.dataset_id))
	                        if not dataset:
	                            return _tool_error(
	                                f"Dataset not found: {spec.dataset_id}",
	                                code=ErrorCode.RESOURCE_NOT_FOUND,
	                                category=ErrorCategory.RESOURCE,
	                                status_code=404,
	                                context={"dataset_id": str(spec.dataset_id)},
	                            )

                        if dataset_version_id:
                            version = await dataset_registry.get_version(version_id=dataset_version_id)
                        else:
                            version = await dataset_registry.get_latest_version(dataset_id=str(spec.dataset_id))

	                        if not version:
	                            return _tool_error(
	                                "No dataset version found",
	                                code=ErrorCode.RESOURCE_NOT_FOUND,
	                                category=ErrorCategory.RESOURCE,
	                                status_code=404,
	                                context={"dataset_id": str(spec.dataset_id), "version_id": dataset_version_id},
	                            )

                        current_schema = version.schema_json or {}
                        current_columns = current_schema.get("columns") or current_schema.get("fields") or []

                        detector = SchemaDriftDetector()
                        drift = detector.detect_drift(
                            subject_type="dataset",
                            subject_id=str(spec.dataset_id),
                            db_name=db_name,
                            current_schema=current_columns,
                            previous_hash=spec.schema_hash,
                        )

                        if not drift:
                            return {
                                "status": "compatible",
                                "mapping_spec_id": mapping_spec_id,
                                "message": "Schema matches expected state - no drift detected",
                                "current_hash": version.schema_hash,
                            }

                        # Broadcast schema drift via WebSocket for real-time notifications
                        try:
                            drift_payload = detector.to_notification_payload(drift)
                            ws_service = await self._ensure_websocket_service()
                            if ws_service:
                                await ws_service.publish_schema_drift(
                                    db_name=db_name,
                                    drift_payload=drift_payload,
                                )
                                logger.info("Schema drift broadcast sent for %s/%s", db_name, mapping_spec_id)
                        except Exception as ws_exc:
                            logger.warning("Failed to broadcast schema drift: %s", ws_exc)

                        return {
                            "status": "drift_detected",
                            "mapping_spec_id": mapping_spec_id,
                            "severity": drift.severity,
                            "drift_type": drift.drift_type,
                            "change_summary": drift.change_summary,
                            "changes": [
                                {
                                    "change_type": c.change_type,
                                    "column_name": c.column_name,
                                    "impact": c.impact,
                                }
                                for c in drift.changes
                            ],
                            "is_breaking": drift.is_breaking,
                            "current_hash": drift.current_hash,
                            "previous_hash": drift.previous_hash,
                        }
	                    except Exception as exc:
	                        logger.warning("check_schema_drift failed: %s", exc)
	                        return _tool_error(
	                            "check_schema_drift failed",
	                            detail=str(exc)[:300],
	                            code=ErrorCode.INTERNAL_ERROR,
	                            category=ErrorCategory.INTERNAL,
	                            status_code=500,
	                            context={"db_name": db_name, "mapping_spec_id": mapping_spec_id},
	                        )

                if name == "list_schema_changes":
                    db_name = str(arguments.get("db_name") or "").strip()
                    subject_type = str(arguments.get("subject_type") or "").strip()
                    subject_id = str(arguments.get("subject_id") or "").strip()
                    severity = str(arguments.get("severity") or "").strip() or None
                    limit = int(arguments.get("limit") or 20)

                    if not db_name or not subject_type or not subject_id:
                        return _missing_required_params(
                            "list_schema_changes",
                            ["db_name", "subject_type", "subject_id"],
                            arguments,
                        )

                    try:
                        # Query schema_drift_history table
                        dataset_registry, _ = await self._ensure_registries()
                        pool = dataset_registry._pool

                        query = """
                            SELECT drift_id, subject_type, subject_id, db_name,
                                   previous_hash, current_hash, drift_type, severity,
                                   changes, detected_at, acknowledged_at
                            FROM schema_drift_history
                            WHERE db_name = $1 AND subject_type = $2 AND subject_id = $3
                        """
                        params = [db_name, subject_type, subject_id]

                        if severity:
                            query += " AND severity = $4"
                            params.append(severity)

                        query += " ORDER BY detected_at DESC LIMIT $" + str(len(params) + 1)
                        params.append(limit)

                        async with pool.acquire() as conn:
                            rows = await conn.fetch(query, *params)

                        changes = [
                            {
                                "drift_id": str(row["drift_id"]),
                                "drift_type": row["drift_type"],
                                "severity": row["severity"],
                                "changes": row["changes"],
                                "detected_at": row["detected_at"].isoformat() if row["detected_at"] else None,
                                "acknowledged": row["acknowledged_at"] is not None,
                            }
                            for row in rows
                        ]

                        return {
                            "status": "success",
                            "db_name": db_name,
                            "subject_type": subject_type,
                            "subject_id": subject_id,
                            "total_changes": len(changes),
                            "changes": changes,
                        }
	                    except Exception as exc:
	                        logger.warning("list_schema_changes failed: %s", exc)
                        # Table might not exist yet
                        if "does not exist" in str(exc):
                            return {
                                "status": "success",
                                "db_name": db_name,
                                "subject_type": subject_type,
                                "subject_id": subject_id,
                                "total_changes": 0,
	                                "changes": [],
	                                "message": "No schema change history available",
	                            }
	                        return _tool_error(
	                            "list_schema_changes failed",
	                            detail=str(exc)[:300],
	                            code=ErrorCode.INTERNAL_ERROR,
	                            category=ErrorCategory.INTERNAL,
	                            status_code=500,
	                            context={"db_name": db_name, "subject_type": subject_type, "subject_id": subject_id},
	                        )

                # ── Dataset Lookup Tools ─────────────────────────────────────
                if name == "dataset_get_by_name":
                    db_name = str(arguments.get("db_name") or "").strip()
                    dataset_name = str(arguments.get("dataset_name") or "").strip()
                    branch = str(arguments.get("branch") or "main").strip()

                    if not db_name or not dataset_name:
                        return _missing_required_params("dataset_get_by_name", ["db_name", "dataset_name"], arguments)

                    dataset_registry, _ = await self._ensure_registries()
                    dataset = await dataset_registry.get_dataset_by_name(
                        db_name=db_name,
                        name=dataset_name,
                        branch=branch,
                    )

                    if not dataset:
                        return {
                            "status": "not_found",
                            "error": f"Dataset not found: {dataset_name} in {db_name}/{branch}",
                            "db_name": db_name,
                            "dataset_name": dataset_name,
                            "branch": branch,
                        }

                    return {
                        "status": "success",
                        "dataset_id": dataset.dataset_id,
                        "db_name": dataset.db_name,
                        "name": dataset.name,
                        "branch": dataset.branch,
                        "source_type": dataset.source_type,
                        "schema": dataset.schema_json,
                        "created_at": dataset.created_at.isoformat() if dataset.created_at else None,
                    }

                if name == "dataset_get_latest_version":
                    dataset_id = str(arguments.get("dataset_id") or "").strip()

                    if not dataset_id:
                        return _missing_required_params("dataset_get_latest_version", ["dataset_id"], arguments)

                    dataset_registry, _ = await self._ensure_registries()
                    version = await dataset_registry.get_latest_version(dataset_id=dataset_id)

                    if not version:
                        return {
                            "status": "not_found",
                            "error": f"No version found for dataset: {dataset_id}",
                            "dataset_id": dataset_id,
                        }

                    return {
                        "status": "success",
                        "version_id": version.version_id,
                        "dataset_id": version.dataset_id,
                        "artifact_key": version.artifact_key,
                        "lakefs_commit_id": version.lakefs_commit_id,
                        "row_count": version.row_count,
                        "created_at": version.created_at.isoformat() if version.created_at else None,
                    }

                if name == "dataset_validate_columns":
                    dataset_id = str(arguments.get("dataset_id") or "").strip()
                    columns = arguments.get("columns") or []

                    if not dataset_id:
                        return _missing_required_params("dataset_validate_columns", ["dataset_id"], arguments)
                    if not columns or not isinstance(columns, list):
                        return _missing_required_params("dataset_validate_columns", ["columns"], arguments)

                    # Normalize column names to check
                    columns_to_check = [str(c).strip() for c in columns if str(c).strip()]

                    dataset_registry, _ = await self._ensure_registries()
                    dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)

	                    if not dataset:
	                        return _tool_error(
	                            f"Dataset not found: {dataset_id}",
	                            code=ErrorCode.RESOURCE_NOT_FOUND,
	                            category=ErrorCategory.RESOURCE,
	                            status_code=404,
	                            context={"dataset_id": dataset_id},
	                        )

                    # Get schema from dataset
                    schema_json = dataset.schema_json or {}
                    schema_columns = schema_json.get("columns") or schema_json.get("fields") or []

                    # Extract column names from schema
                    available_columns: List[str] = []
                    for col in schema_columns:
                        if isinstance(col, dict):
                            col_name = col.get("name") or col.get("column_name") or ""
                            if col_name:
                                available_columns.append(str(col_name))
                        elif isinstance(col, str):
                            available_columns.append(col)

                    # Validate columns
                    valid_columns: List[str] = []
                    invalid_columns: List[str] = []
                    suggestions: Dict[str, List[str]] = {}

                    available_lower = {c.lower(): c for c in available_columns}

                    for col in columns_to_check:
                        col_lower = col.lower()
                        if col in available_columns:
                            valid_columns.append(col)
                        elif col_lower in available_lower:
                            # Case mismatch - suggest the correct case
                            valid_columns.append(col)
                            suggestions[col] = [available_lower[col_lower]]
                        else:
                            invalid_columns.append(col)
                            # Find similar column names for suggestions
                            similar = [
                                c for c in available_columns
                                if col_lower in c.lower() or c.lower() in col_lower
                            ][:3]
                            if similar:
                                suggestions[col] = similar

                    return {
                        "status": "valid" if not invalid_columns else "invalid",
                        "dataset_id": dataset_id,
                        "valid_columns": valid_columns,
                        "invalid_columns": invalid_columns,
                        "suggestions": suggestions if suggestions else None,
                        "available_columns": available_columns[:50],  # Limit to first 50
                        "total_available_columns": len(available_columns),
                    }

                # Enterprise Enhancement: Helpful error for unknown tools
                similar_tools = [
                    t for t in [
                        "plan_new", "plan_add_input", "plan_add_join",
                        "plan_add_filter", "plan_add_cast", "plan_add_output", "plan_validate",
                        "plan_preview", "plan_execute", "pipeline_deploy_promote_build",
                        "objectify_suggest_mapping", "objectify_create_mapping_spec",
                        "objectify_list_mapping_specs", "objectify_run", "objectify_get_status",
                        "objectify_wait", "ontology_query_instances",
                        "dataset_get_by_name", "dataset_get_latest_version",
                        "dataset_validate_columns",
                    ]
                    if name.lower() in t.lower() or t.lower() in name.lower()
                ]
                return {
                    "error": f"Unknown tool: {name}",
                    "hint": "Check the tool name spelling",
                    "similar_tools": similar_tools[:3] if similar_tools else [],
                }

            except PipelinePlanBuilderError as exc:
                # Enterprise Enhancement: Structured error for plan builder errors
                return _build_tool_error_response(
                    name,
                    exc,
                    arguments=arguments,
                    hint="Review the plan structure and fix the identified issue",
                )
            except Exception as exc:
                logger.exception("pipeline_mcp tool failed name=%s", name)
                # Enterprise Enhancement: Structured error response
                return _build_tool_error_response(name, exc, arguments=arguments)

    async def run(self) -> None:
        async with stdio_server() as (read_stream, write_stream):
            # MCP Python SDK (>=1.0) requires explicit initialization options.
            init = InitializationOptions(
                server_name="pipeline-mcp-server",
                server_version=os.environ.get("SPICE_VERSION", "0.1.0"),
                capabilities=ServerCapabilities(tools=ToolsCapability()),
            )
            await self.server.run(read_stream, write_stream, init)


async def main() -> None:
    server = PipelineMCPServer()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
