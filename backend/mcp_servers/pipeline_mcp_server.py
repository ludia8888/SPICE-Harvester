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

from mcp_servers.pipeline_tools.dataset_tools import build_dataset_tool_handlers  # noqa: E402
from mcp_servers.pipeline_tools.debug_tools import build_debug_tool_handlers  # noqa: E402
from mcp_servers.pipeline_mcp_helpers import extract_spark_error_details as _extract_spark_error_details  # noqa: E402
from mcp_servers.pipeline_mcp_rate_limit import ToolCallRateLimiter, resolve_tool_call_scope  # noqa: E402
from mcp_servers.pipeline_mcp_tool_specs import build_pipeline_mcp_tool_specs  # noqa: E402
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
        self._rate_limiter = ToolCallRateLimiter()
        self._setup_handlers()

    def _current_request_context(self) -> Any:
        try:
            return self.server.request_context
        except Exception:
            return None

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
            return [Tool(**spec) for spec in build_pipeline_mcp_tool_specs()]

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
            rate_limit_error = self._rate_limiter.check_and_record(
                name,
                scope=resolve_tool_call_scope(
                    arguments,
                    request_context=self._current_request_context(),
                ),
            )
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
