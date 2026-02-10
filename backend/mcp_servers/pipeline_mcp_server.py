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
from pathlib import Path
from typing import Any, Dict, List, Optional

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

from mcp_servers.pipeline_mcp_helpers import extract_spark_error_details as _extract_spark_error_details  # noqa: E402
from mcp_servers.pipeline_tools.dataset_tools import build_dataset_tool_handlers  # noqa: E402
from mcp_servers.pipeline_tools.debug_tools import build_debug_tool_handlers  # noqa: E402
from mcp_servers.pipeline_tools.objectify_tools import build_objectify_tool_handlers  # noqa: E402
from mcp_servers.pipeline_tools.ontology_tools import build_ontology_tool_handlers  # noqa: E402
from mcp_servers.pipeline_tools.pipeline_tools import build_pipeline_tool_handlers  # noqa: E402
from mcp_servers.pipeline_tools.plan_tools import build_plan_tool_handlers  # noqa: E402
from mcp_servers.pipeline_tools.registry import merge_tool_handlers  # noqa: E402
from mcp_servers.pipeline_tools.schema_tools import build_schema_tool_handlers  # noqa: E402

from shared.config.settings import get_settings  # noqa: E402
from shared.services.registries.dataset_profile_registry import DatasetProfileRegistry  # noqa: E402
from shared.services.registries.dataset_registry import DatasetRegistry  # noqa: E402
from shared.services.registries.pipeline_registry import PipelineRegistry  # noqa: E402
from shared.services.registries.objectify_registry import ObjectifyRegistry  # noqa: E402
from shared.services.pipeline.pipeline_plan_builder import PipelinePlanBuilderError  # noqa: E402
from shared.utils.app_logger import configure_logging  # noqa: E402

logger = logging.getLogger(__name__)


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
                            # - rename={\"old\":\"new\"}
                            # - rename=[{\"from\":\"old\",\"to\":\"new\"}]
                            # - renames/mappings=[{\"from\":\"old\",\"to\":\"new\"}]
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
                {
                    "name": "dataset_list",
                    "description": "List all datasets in a database/project with their column names and metadata. Use this to understand what data is available before building pipelines.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string", "description": "Database/project name"},
                            "branch": {"type": "string", "description": "Branch (default: main)"},
                        },
                        "required": ["db_name"],
                    },
                },
                {
                    "name": "dataset_sample",
                    "description": "Get a small sample of actual data rows from a dataset (PII-masked). Use this to understand data patterns, values, and quality before designing transformations.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "dataset_id": {"type": "string", "description": "Dataset ID"},
                            "limit": {"type": "integer", "description": "Number of rows to sample (default: 20, max: 50)"},
                            "columns": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Optional: only return these columns",
                            },
                        },
                        "required": ["dataset_id"],
                    },
                },
                {
                    "name": "dataset_profile",
                    "description": "Get column-level statistics for a dataset: null ratio, distinct count, top values, numeric histogram. Use to understand data quality and distributions.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "dataset_id": {"type": "string", "description": "Dataset ID"},
                            "columns": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Optional: only profile these columns",
                            },
                        },
                        "required": ["dataset_id"],
                    },
                },
                {
                    "name": "data_query",
                    "description": "Execute ad-hoc SQL query on dataset sample rows using DuckDB. Supports GROUP BY, JOIN, WINDOW, CTE, and all standard SQL. Table name is 'data'. Max 500 result rows. Query runs on sample rows, not full dataset.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "dataset_id": {"type": "string", "description": "Dataset ID to query"},
                            "sql": {
                                "type": "string",
                                "description": "SQL query. Table name is 'data'. Example: SELECT category, COUNT(*) as cnt FROM data GROUP BY category ORDER BY cnt DESC",
                            },
                            "limit": {"type": "integer", "default": 100, "description": "Max result rows (default: 100, max: 500)"},
                        },
                        "required": ["dataset_id", "sql"],
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
                    "name": "reconcile_relationships",
                    "description": (
                        "Auto-populate relationships on ES instances by detecting FK references between ontology classes. "
                        "Call this AFTER objectify completes for all classes to link instances via foreign keys "
                        "(e.g. Order.customer_id → Customer). Returns per-relationship stats."
                    ),
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string", "description": "Database name"},
                            "branch": {"type": "string", "default": "main", "description": "Ontology branch"},
                        },
                        "required": ["db_name"],
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

        tool_handlers = merge_tool_handlers(
            build_plan_tool_handlers(),
            build_pipeline_tool_handlers(),
            build_debug_tool_handlers(),
            build_objectify_tool_handlers(),
            build_ontology_tool_handlers(),
            build_schema_tool_handlers(),
            build_dataset_tool_handlers(),
        )

        @self.server.call_tool()
        async def call_tool(name: str, arguments: Dict[str, Any]) -> Any:
            logger.info("call_tool invoked: name=%s", name)
            # Enterprise Enhancement (2026-01): Rate limiting to prevent runaway loops
            rate_limit_error = _rate_limiter.check_and_record(name)
            if rate_limit_error:
                logger.warning("MCP tool rate limited: %s", rate_limit_error)
                return {"error": rate_limit_error, "tool": name, "retry_after_seconds": 60}

            try:
                handler = tool_handlers.get(name)
                if handler is not None:
                    return await handler(self, arguments)

                # Enterprise Enhancement: Helpful error for unknown tools
                tools = await list_tools()
                tool_names = [getattr(t, "name", "") for t in tools]
                similar_tools = [t for t in tool_names if t and (name.lower() in t.lower() or t.lower() in name.lower())]
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
    configure_logging(get_settings().observability.log_level)
    server = PipelineMCPServer()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
