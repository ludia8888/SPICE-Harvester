#!/usr/bin/env python3
"""
BFF SDK MCP Server

Wraps the SPICE-Harvester BFF REST API as MCP tools,
organized by the same domain groups as the generated SDK
(spice_harvester_sdk).

Domains exposed:
  - Database Management (CRUD)
  - Ontology Management (classes, extensions)
  - Pipeline Builder (catalog, execution, datasets, UDFs)
  - Graph (query, simple query, multi-hop, paths)
  - Instance Management (async CRUD)
  - Lineage (provenance tracking)
  - Governance (key specs, migration plans, gate policies)
  - Objectify (DAG runs, mapping specs)
  - Admin / Health (health, replay, reindex)
  - Generic BFF API call (any endpoint)

Usage:
  SPICE_BASE_URL=http://localhost:8002 \
  SPICE_ADMIN_TOKEN=change_me \
  python bff_sdk_mcp_server.py
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool
from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.foundry.rids import build_rid, parse_rid
from shared.services.core.runtime_status import availability_surface

# ---------------------------------------------------------------------------
# Path setup (repo / container)
# ---------------------------------------------------------------------------
_this_file = Path(__file__).resolve()
_backend_root = _this_file.parents[1]
_repo_root = _this_file.parents[2] if len(_this_file.parents) > 2 else _backend_root
for _p in (str(_backend_root), str(_repo_root)):
    if _p and _p not in sys.path:
        sys.path.append(_p)

from mcp_servers.pipeline_mcp_http import (  # noqa: E402
    bff_json,
    bff_v2_json,
    resolve_db_name_for_bff_call,
)
from mcp_servers.feature_helpers import build_objectify_run_body, build_pipeline_execution_payload  # noqa: E402
from mcp_servers.pipeline_mcp_errors import tool_error  # noqa: E402

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")

_STREAMABLE_SESSION_IDLE_TTL_SECONDS = 60 * 15
_STREAMABLE_SESSION_MAX = 512

# ---------------------------------------------------------------------------
# Common helpers
# ---------------------------------------------------------------------------

def _s(val: Any) -> str:
    return str(val or "").strip()


def _opt(val: Any) -> Optional[str]:
    v = _s(val)
    return v if v else None


def _json_result(data: Any) -> list:
    """Wrap result for MCP text content."""
    from mcp.types import TextContent
    text = json.dumps(data, ensure_ascii=False, indent=2, default=str)
    return [TextContent(type="text", text=text)]


def _common_args(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Extract common BFF call args: db_name, principal_id, principal_type."""
    return {
        "db_name": _s(arguments.get("db_name", "")),
        "principal_id": _opt(arguments.get("principal_id")),
        "principal_type": _opt(arguments.get("principal_type")),
    }


def _coerce_pipeline_id(value: Any) -> str:
    raw = _s(value)
    if not raw:
        return raw
    if raw.startswith("pipeline://"):
        return raw[len("pipeline://") :].strip()
    try:
        kind, rid_id = parse_rid(raw)
    except ValueError:
        return raw
    return rid_id if kind == "pipeline" else raw


def _health_surface(*, transport: str) -> Dict[str, Any]:
    surface = availability_surface(
        service="bff-sdk-mcp-server",
        container_ready=True,
        runtime_status={"ready": True, "issues": []},
        dependency_status_overrides={"transport": "ready"},
        status_reason_override=f"{transport} transport ready",
        message=f"{transport} transport ready",
    )
    surface["dependency_details"]["transport"] = {
        "dependency": "transport",
        "state": "ready",
        "classification": "healthy",
        "classifications": [],
        "message": f"{transport} transport ready",
        "messages": [f"{transport} transport ready"],
        "components": [transport],
        "affected_features": [f"mcp.{transport}"],
        "affects_readiness": False,
        "issue_count": 0,
        "source": "derived",
    }
    surface["server"] = "bff-sdk-mcp-server"
    surface["transport"] = transport
    surface["tools"] = len(TOOL_SPECS)
    return surface


def _pipeline_target_rid(value: Any) -> str:
    raw = _s(value)
    if not raw:
        raise ValueError("pipeline_id is required")
    try:
        kind, rid_id = parse_rid(raw)
        if kind == "pipeline":
            return build_rid("pipeline", rid_id)
    except ValueError:
        pass
    pipeline_id = _coerce_pipeline_id(raw)
    return build_rid("pipeline", pipeline_id)


def _extract_dataset_ids(payload: Dict[str, Any]) -> List[str]:
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    datasets = data.get("datasets") if isinstance(data.get("datasets"), list) else []
    dataset_ids: List[str] = []
    for item in datasets:
        if not isinstance(item, dict):
            continue
        candidate = _s(item.get("dataset_id") or item.get("datasetId") or item.get("id"))
        if candidate:
            dataset_ids.append(candidate)
    return dataset_ids


@dataclass
class _StreamableSessionState:
    session_id: str
    transport: Any
    task: asyncio.Task[Any]
    last_seen: float


class _StreamableSessionRegistry:
    def __init__(self, *, idle_ttl_seconds: int = _STREAMABLE_SESSION_IDLE_TTL_SECONDS, max_sessions: int = _STREAMABLE_SESSION_MAX):
        self._idle_ttl_seconds = idle_ttl_seconds
        self._max_sessions = max_sessions
        self._sessions: Dict[str, _StreamableSessionState] = {}
        self._lock = asyncio.Lock()

    async def get_or_create(self, requested_session_id: Optional[str], *, transport_factory: Any, server_runner_factory: Any) -> _StreamableSessionState:
        stale_states: list[_StreamableSessionState] = []
        async with self._lock:
            stale_states.extend(self._evict_idle_locked())

            session_id = str(requested_session_id or "").strip()
            state = self._sessions.get(session_id) if session_id else None
            if state is not None:
                state.last_seen = time.monotonic()
            else:
                transport = transport_factory(session_id or str(uuid.uuid4()))
                session_id = str(getattr(transport, "_session_id", None) or session_id or uuid.uuid4())
                task = asyncio.create_task(server_runner_factory(transport))
                state = _StreamableSessionState(
                    session_id=session_id,
                    transport=transport,
                    task=task,
                    last_seen=time.monotonic(),
                )
                self._sessions[session_id] = state
                task.add_done_callback(lambda _: self._sessions.pop(session_id, None))
                stale_states.extend(self._evict_over_capacity_locked())

        for stale_state in stale_states:
            await self._close_state(stale_state)
        return state

    async def close(self, session_id: Optional[str]) -> None:
        if not session_id:
            return
        async with self._lock:
            state = self._sessions.pop(str(session_id), None)
        if state is not None:
            await self._close_state(state)

    async def shutdown(self) -> None:
        async with self._lock:
            states = list(self._sessions.values())
            self._sessions.clear()
        for state in states:
            await self._close_state(state)

    def _evict_idle_locked(self) -> list[_StreamableSessionState]:
        now = time.monotonic()
        stale_states: list[_StreamableSessionState] = []
        for session_id, state in list(self._sessions.items()):
            if now - state.last_seen < self._idle_ttl_seconds:
                continue
            removed = self._sessions.pop(session_id, None)
            if removed is not None:
                stale_states.append(removed)
        return stale_states

    def _evict_over_capacity_locked(self) -> list[_StreamableSessionState]:
        evicted: list[_StreamableSessionState] = []
        while len(self._sessions) > self._max_sessions:
            oldest = min(self._sessions.values(), key=lambda state: state.last_seen)
            self._sessions.pop(oldest.session_id, None)
            evicted.append(oldest)
        return evicted

    async def _close_state(self, state: _StreamableSessionState) -> None:
        state.task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await state.task
        transport = state.transport
        for close_name in ("aclose", "close"):
            closer = getattr(transport, close_name, None)
            if closer is None:
                continue
            try:
                result = closer()
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.debug("Failed to close streamable MCP transport for session %s", state.session_id, exc_info=True)
            break

# ═══════════════════════════════════════════════════════════════════════════
# Tool definitions (organised by SDK domain)
# ═══════════════════════════════════════════════════════════════════════════

_DB_NAME_PROP = {"type": "string", "description": "Database (project) name"}
_BRANCH_PROP = {"type": "string", "description": "Branch name (default: main)"}
_PRINCIPAL_PROPS = {
    "principal_id": {"type": "string", "description": "Actor / user ID (optional)"},
    "principal_type": {"type": "string", "description": "Actor type: user | service (optional)"},
}

TOOL_SPECS: List[Dict[str, Any]] = [
    # ── Database Management ──────────────────────────────────────────────
    {
        "name": "bff_list_databases",
        "description": "List all databases (projects).",
        "inputSchema": {
            "type": "object",
            "properties": {**_PRINCIPAL_PROPS},
        },
    },
    {
        "name": "bff_create_database",
        "description": "Create a new database (project).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Database name"},
                "description": {"type": "string", "description": "Optional description"},
                **_PRINCIPAL_PROPS,
            },
            "required": ["name"],
        },
    },
    {
        "name": "bff_delete_database",
        "description": "Delete a database (project). Requires expected_seq for OCC.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "expected_seq": {"type": "integer", "description": "Expected aggregate sequence (OCC)"},
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name", "expected_seq"],
        },
    },

    # ── Ontology Management ──────────────────────────────────────────────
    {
        "name": "bff_list_ontology_classes",
        "description": "List all ontology classes in a database.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "branch": _BRANCH_PROP,
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name"],
        },
    },
    {
        "name": "bff_get_ontology_class",
        "description": "Get a single ontology class by class_id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "class_id": {"type": "string", "description": "Ontology class ID"},
                "branch": _BRANCH_PROP,
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name", "class_id"],
        },
    },
    {
        "name": "bff_create_ontology_class",
        "description": "Create a new ontology class.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "branch": _BRANCH_PROP,
                "class_data": {
                    "type": "object",
                    "description": "Ontology class definition (id, label, description, properties, relationships)",
                },
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name", "class_data"],
        },
    },

    # ── Pipeline Builder ─────────────────────────────────────────────────
    {
        "name": "bff_list_pipelines",
        "description": "List all pipelines in a database.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "branch": _BRANCH_PROP,
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name"],
        },
    },
    {
        "name": "bff_get_pipeline",
        "description": "Get pipeline detail by pipeline_id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "pipeline_id": {"type": "string", "description": "Pipeline ID"},
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name", "pipeline_id"],
        },
    },
    {
        "name": "bff_list_datasets",
        "description": "List datasets in a pipeline.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "pipeline_id": {"type": "string", "description": "Pipeline ID"},
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name", "pipeline_id"],
        },
    },
    {
        "name": "bff_execute_pipeline",
        "description": "Execute (run) a pipeline. Modes: profile, preview, build.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "pipeline_id": {"type": "string", "description": "Pipeline ID"},
                "mode": {"type": "string", "enum": ["profile", "preview", "build"], "description": "Execution mode"},
                "branch": _BRANCH_PROP,
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name", "pipeline_id", "mode"],
        },
    },

    # ── Graph Queries ────────────────────────────────────────────────────
    {
        "name": "bff_graph_query",
        "description": "Execute a graph query (full mode with hops, filters).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "query": {
                    "type": "object",
                    "description": "GraphQueryRequest body (start_nodes, hops, filters, limit)",
                },
                "branch": _BRANCH_PROP,
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name", "query"],
        },
    },
    {
        "name": "bff_simple_graph_query",
        "description": "Execute a simple graph query (keyword search + optional class filter).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "query": {
                    "type": "object",
                    "description": "SimpleGraphQueryRequest body (keyword, class_filter, limit)",
                },
                "branch": _BRANCH_PROP,
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name", "query"],
        },
    },

    # ── Instance Management ──────────────────────────────────────────────
    {
        "name": "bff_create_instance",
        "description": "Create a new instance (async, label-based).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "class_label": {"type": "string", "description": "Class label (human-readable)"},
                "data": {"type": "object", "description": "Instance data (key-value)"},
                "branch": _BRANCH_PROP,
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name", "class_label", "data"],
        },
    },
    {
        "name": "bff_update_instance",
        "description": "Update an instance (async, label-based). Requires expected_seq for OCC.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "class_label": {"type": "string", "description": "Class label"},
                "instance_id": {"type": "string", "description": "Instance ID"},
                "data": {"type": "object", "description": "Updated fields"},
                "expected_seq": {"type": "integer", "description": "Expected sequence (OCC)"},
                "branch": _BRANCH_PROP,
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name", "class_label", "instance_id", "data", "expected_seq"],
        },
    },
    {
        "name": "bff_delete_instance",
        "description": "Delete an instance (async).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "class_label": {"type": "string", "description": "Class label"},
                "instance_id": {"type": "string", "description": "Instance ID"},
                "expected_seq": {"type": "integer", "description": "Expected sequence (OCC)"},
                "branch": _BRANCH_PROP,
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name", "class_label", "instance_id", "expected_seq"],
        },
    },

    # ── Lineage ──────────────────────────────────────────────────────────
    {
        "name": "bff_get_lineage",
        "description": "Get lineage (provenance) graph for a node.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "node_id": {"type": "string", "description": "Lineage node ID (e.g. event:<id>, artifact:es:<index>/<doc>)"},
                "direction": {"type": "string", "enum": ["upstream", "downstream", "both"], "description": "Traversal direction"},
                "max_depth": {"type": "integer", "description": "Max traversal depth (default: 5)"},
                "branch": _BRANCH_PROP,
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name", "node_id"],
        },
    },

    # ── Governance ───────────────────────────────────────────────────────
    {
        "name": "bff_list_key_specs",
        "description": "List governance key specs (deduplication / identity keys).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "dataset_id": {"type": "string", "description": "Optional dataset filter"},
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name"],
        },
    },
    {
        "name": "bff_list_schema_migration_plans",
        "description": "List schema migration plans.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "status": {"type": "string", "description": "Filter by status (optional)"},
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name"],
        },
    },

    # ── Objectify ────────────────────────────────────────────────────────
    {
        "name": "bff_list_objectify_runs",
        "description": "List objectify DAG runs.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "pipeline_id": {"type": "string", "description": "Pipeline ID"},
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name", "pipeline_id"],
        },
    },
    {
        "name": "bff_start_objectify",
        "description": "Start objectify for all datasets in a pipeline via /api/v1/objectify/datasets/{dataset_id}/run fan-out.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "db_name": _DB_NAME_PROP,
                "pipeline_id": {"type": "string", "description": "Pipeline ID or pipeline RID"},
                "branch": _BRANCH_PROP,
                "mapping_spec_id": {"type": "string", "description": "Optional mapping spec to force for each dataset"},
                "target_class_id": {"type": "string", "description": "Optional target class id (OMS mapping resolution)"},
                "dataset_version_id": {"type": "string", "description": "Optional dataset version id override"},
                "artifact_output_name": {"type": "string", "description": "Optional artifact output name"},
                "max_rows": {"type": "integer", "description": "Optional max rows per objectify job"},
                "batch_size": {"type": "integer", "description": "Optional batch size per objectify job"},
                "allow_partial": {"type": "boolean", "description": "Allow partial objectify writes"},
                **_PRINCIPAL_PROPS,
            },
            "required": ["db_name", "pipeline_id"],
        },
    },

    # ── Admin / Health ───────────────────────────────────────────────────
    {
        "name": "bff_health_check",
        "description": "BFF health check (includes OMS connectivity status).",
        "inputSchema": {"type": "object", "properties": {}},
    },

    # ── Generic BFF API call ─────────────────────────────────────────────
    {
        "name": "bff_api_call",
        "description": (
            "Generic BFF REST API call for any endpoint not covered by specialized tools. "
            "Specify method, path (e.g. /databases), and optional JSON body / query params."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "method": {"type": "string", "enum": ["GET", "POST", "PUT", "PATCH", "DELETE"], "description": "HTTP method"},
                "path": {"type": "string", "description": "API path starting with / (e.g. /databases, /pipelines/{id}/runs)"},
                "db_name": _DB_NAME_PROP,
                "body": {"type": "object", "description": "JSON body (for POST/PUT/PATCH)"},
                "params": {"type": "object", "description": "Query parameters"},
                "api_version": {"type": "string", "enum": ["v1", "v2"], "description": "API version prefix (default: v1)"},
                **_PRINCIPAL_PROPS,
            },
            "required": ["method", "path"],
        },
    },
]


# ═══════════════════════════════════════════════════════════════════════════
# Tool handlers
# ═══════════════════════════════════════════════════════════════════════════

async def _handle_tool(name: str, arguments: Dict[str, Any]) -> list:
    """Dispatch a tool call to the matching handler."""
    ca = _common_args(arguments)

    # ── Database ─────────────────────────────────────────────────────────
    if name == "bff_list_databases":
        r = await bff_json("GET", "/databases", **ca)
        return _json_result(r)

    if name == "bff_create_database":
        body = {"name": _s(arguments["name"])}
        desc = _opt(arguments.get("description"))
        if desc:
            body["description"] = desc
        ca["db_name"] = _s(arguments.get("name", ""))
        r = await bff_json("POST", "/databases", json_body=body, **ca)
        return _json_result(r)

    if name == "bff_delete_database":
        db = _s(arguments["db_name"])
        seq = int(arguments["expected_seq"])
        r = await bff_json("DELETE", f"/databases/{db}", params={"expected_seq": seq}, **ca)
        return _json_result(r)

    # ── Ontology ─────────────────────────────────────────────────────────
    if name == "bff_list_ontology_classes":
        db = _s(arguments["db_name"])
        branch = _s(arguments.get("branch")) or "main"
        r = await bff_json("GET", f"/databases/{db}/ontology", params={"branch": branch}, **ca)
        return _json_result(r)

    if name == "bff_get_ontology_class":
        db = _s(arguments["db_name"])
        cid = _s(arguments["class_id"])
        branch = _s(arguments.get("branch")) or "main"
        r = await bff_json("GET", f"/databases/{db}/ontology/{cid}", params={"branch": branch}, **ca)
        return _json_result(r)

    if name == "bff_create_ontology_class":
        db = _s(arguments["db_name"])
        branch = _s(arguments.get("branch")) or "main"
        body = arguments.get("class_data", {})
        r = await bff_json("POST", f"/databases/{db}/ontology", json_body=body, params={"branch": branch}, **ca)
        return _json_result(r)

    # ── Pipeline ─────────────────────────────────────────────────────────
    if name == "bff_list_pipelines":
        db = _s(arguments["db_name"])
        branch = _s(arguments.get("branch")) or "main"
        r = await bff_json("GET", "/pipelines", params={"branch": branch}, **ca)
        return _json_result(r)

    if name == "bff_get_pipeline":
        pid = _coerce_pipeline_id(arguments["pipeline_id"])
        r = await bff_json("GET", f"/pipelines/{pid}", **ca)
        return _json_result(r)

    if name == "bff_list_datasets":
        pid = _coerce_pipeline_id(arguments["pipeline_id"])
        r = await bff_json("GET", f"/pipelines/{pid}/datasets", **ca)
        return _json_result(r)

    if name == "bff_execute_pipeline":
        pid = _coerce_pipeline_id(arguments["pipeline_id"])
        mode = _s(arguments["mode"])
        branch = _s(arguments.get("branch")) or "main"
        payload = build_pipeline_execution_payload(
            pipeline_id=pid,
            mode=mode,
            branch=branch,
        )
        r = await bff_v2_json("POST", "/v2/orchestration/builds/create", json_body=payload, **ca)
        return _json_result(r)

    # ── Graph ────────────────────────────────────────────────────────────
    if name == "bff_graph_query":
        db = _s(arguments["db_name"])
        branch = _s(arguments.get("branch")) or "main"
        r = await bff_json(
            "POST",
            f"/api/v1/graph-query/{db}",
            json_body=arguments.get("query", {}),
            params={"base_branch": branch},
            **ca,
        )
        return _json_result(r)

    if name == "bff_simple_graph_query":
        db = _s(arguments["db_name"])
        branch = _s(arguments.get("branch")) or "main"
        r = await bff_json(
            "POST",
            f"/api/v1/graph-query/{db}/simple",
            json_body=arguments.get("query", {}),
            params={"base_branch": branch},
            **ca,
        )
        return _json_result(r)

    # ── Instances ────────────────────────────────────────────────────────
    if name == "bff_create_instance":
        db = _s(arguments["db_name"])
        label = _s(arguments["class_label"])
        branch = _s(arguments.get("branch")) or "main"
        body = {"data": arguments.get("data", {})}
        r = await bff_json(
            "POST",
            f"/databases/{db}/instances/{label}/create",
            json_body=body,
            params={"branch": branch},
            **ca,
        )
        return _json_result(r)

    if name == "bff_update_instance":
        db = _s(arguments["db_name"])
        label = _s(arguments["class_label"])
        iid = _s(arguments["instance_id"])
        seq = int(arguments["expected_seq"])
        branch = _s(arguments.get("branch")) or "main"
        body = {"data": arguments.get("data", {})}
        r = await bff_json(
            "PUT",
            f"/databases/{db}/instances/{label}/{iid}/update",
            json_body=body,
            params={"branch": branch, "expected_seq": seq},
            **ca,
        )
        return _json_result(r)

    if name == "bff_delete_instance":
        db = _s(arguments["db_name"])
        label = _s(arguments["class_label"])
        iid = _s(arguments["instance_id"])
        seq = int(arguments["expected_seq"])
        branch = _s(arguments.get("branch")) or "main"
        r = await bff_json(
            "DELETE",
            f"/databases/{db}/instances/{label}/{iid}/delete",
            params={"branch": branch, "expected_seq": seq},
            **ca,
        )
        return _json_result(r)

    # ── Lineage ──────────────────────────────────────────────────────────
    if name == "bff_get_lineage":
        db = _s(arguments["db_name"])
        node_id = _s(arguments["node_id"])
        branch = _s(arguments.get("branch")) or "main"
        params: Dict[str, Any] = {"branch": branch}
        direction = _opt(arguments.get("direction"))
        if direction:
            params["direction"] = direction
        depth = arguments.get("max_depth")
        if depth is not None:
            params["max_depth"] = int(depth)
        r = await bff_json("GET", f"/lineage/{db}/graph/{node_id}", params=params, **ca)
        return _json_result(r)

    # ── Governance ───────────────────────────────────────────────────────
    if name == "bff_list_key_specs":
        params = {}
        did = _opt(arguments.get("dataset_id"))
        if did:
            params["dataset_id"] = did
        r = await bff_json("GET", "/key-specs", params=params, **ca)
        return _json_result(r)

    if name == "bff_list_schema_migration_plans":
        params: Dict[str, Any] = {}
        st = _opt(arguments.get("status"))
        if st:
            params["status"] = st
        r = await bff_json("GET", "/schema-migration-plans", params=params, **ca)
        return _json_result(r)

    # ── Objectify ────────────────────────────────────────────────────────
    if name == "bff_list_objectify_runs":
        pid = _coerce_pipeline_id(arguments["pipeline_id"])
        legacy = await bff_json("GET", f"/pipelines/{pid}/objectify/runs", **ca)
        if not legacy.get("error"):
            return _json_result(legacy)
        runs_resp = await bff_json("GET", f"/pipelines/{pid}/runs", params={"limit": 200}, **ca)
        data = runs_resp.get("data") if isinstance(runs_resp.get("data"), dict) else {}
        runs = data.get("runs") if isinstance(data.get("runs"), list) else []
        filtered = [
            run
            for run in runs
            if isinstance(run, dict) and str(run.get("mode") or "").strip().lower() == "objectify"
        ]
        return _json_result(
            {
                "status": "success",
                "message": "Fallback to /pipelines/{id}/runs filter(mode=objectify)",
                "data": {"runs": filtered, "count": len(filtered), "pipeline_id": pid},
                "legacy_error": legacy.get("error"),
            }
        )

    if name == "bff_start_objectify":
        pid = _coerce_pipeline_id(arguments["pipeline_id"])
        target_rid = _pipeline_target_rid(arguments["pipeline_id"])
        datasets_resp = await bff_json("GET", f"/pipelines/{pid}/datasets", **ca)
        if datasets_resp.get("error"):
            payload = tool_error(
                "Failed to list pipeline datasets for objectify start",
                detail=str(datasets_resp.get("error") or "pipeline datasets lookup failed"),
                status_code=502,
                code=ErrorCode.UPSTREAM_ERROR,
                category=ErrorCategory.UPSTREAM,
                context={"tool": name, "pipeline_id": pid, "targetRid": target_rid},
                operation="bff_start_objectify.list_pipeline_datasets",
            )
            payload["pipeline_id"] = pid
            payload["targetRid"] = target_rid
            payload["upstream_error"] = datasets_resp.get("error")
            return _json_result(payload)
        dataset_ids = _extract_dataset_ids(datasets_resp)
        if not dataset_ids:
            payload = tool_error(
                "No datasets found in pipeline; cannot start objectify",
                detail="Pipeline has no datasets to objectify",
                status_code=404,
                code=ErrorCode.RESOURCE_NOT_FOUND,
                category=ErrorCategory.RESOURCE,
                context={"tool": name, "pipeline_id": pid, "targetRid": target_rid},
                operation="bff_start_objectify.validate_pipeline_datasets",
            )
            payload["pipeline_id"] = pid
            payload["targetRid"] = target_rid
            payload["datasets_response"] = datasets_resp
            return _json_result(payload)

        run_body = build_objectify_run_body(arguments)
        jobs: List[Dict[str, Any]] = []
        failures: List[Dict[str, Any]] = []
        for dataset_id in dataset_ids:
            resp = await bff_json(
                "POST",
                f"/objectify/datasets/{dataset_id}/run",
                json_body=run_body,
                **ca,
            )
            entry = {"dataset_id": dataset_id, "response": resp}
            if resp.get("error"):
                failures.append(entry)
            else:
                jobs.append(entry)

        status_value = "success" if jobs else "error"
        return _json_result(
            {
                "status": status_value,
                "pipeline_id": pid,
                "targetRid": _pipeline_target_rid(arguments["pipeline_id"]),
                "dataset_count": len(dataset_ids),
                "enqueued_jobs": jobs,
                "failed_jobs": failures,
            }
        )

    # ── Admin / Health ───────────────────────────────────────────────────
    if name == "bff_health_check":
        r = await bff_json("GET", "/health", db_name="", principal_id=None, principal_type=None)
        return _json_result(r)

    # ── Generic API call ─────────────────────────────────────────────────
    if name == "bff_api_call":
        method = _s(arguments["method"]).upper()
        path = _s(arguments["path"])
        body = arguments.get("body")
        params = arguments.get("params")
        version = _s(arguments.get("api_version")) or "v1"
        effective_db_name = resolve_db_name_for_bff_call(
            db_name=ca.get("db_name", ""),
            path=path,
            params=params if isinstance(params, dict) else None,
            json_body=body if isinstance(body, dict) else None,
        )
        call_args = {**ca, "db_name": effective_db_name}
        if version == "v2":
            r = await bff_v2_json(method, path, json_body=body, params=params, **call_args)
        else:
            r = await bff_json(method, path, json_body=body, params=params, **call_args)
        return _json_result(r)

    return _json_result({"error": f"Unknown tool: {name}"})


# ═══════════════════════════════════════════════════════════════════════════
# MCP Server
# ═══════════════════════════════════════════════════════════════════════════

class BffSdkMCPServer:
    """MCP server that exposes BFF SDK operations as tools."""

    def __init__(self) -> None:
        self.server = Server("bff-sdk-mcp-server")
        self._setup_handlers()

    def _setup_handlers(self) -> None:
        @self.server.list_tools()
        async def list_tools() -> List[Tool]:
            return [
                Tool(
                    name=spec["name"],
                    description=spec["description"],
                    inputSchema=spec["inputSchema"],
                )
                for spec in TOOL_SPECS
            ]

        @self.server.call_tool()
        async def call_tool(name: str, arguments: Optional[Dict[str, Any]] = None) -> list:
            arguments = arguments or {}
            logger.info("Tool call: %s args=%s", name, list(arguments.keys()))
            try:
                return await _handle_tool(name, arguments)
            except Exception as exc:
                logger.exception("Tool %s failed", name)
                return _json_result({"error": str(exc), "tool": name})


async def main_stdio() -> None:
    """Run as stdio transport (for Claude Code CLI)."""
    server = BffSdkMCPServer()
    logger.info("Starting BFF SDK MCP Server (stdio)")
    options = server.server.create_initialization_options()
    async with stdio_server() as (read_stream, write_stream):
        await server.server.run(read_stream, write_stream, options)


def main_sse(host: str = "0.0.0.0", port: int = 9090) -> None:
    """Run as SSE transport (for Claude.ai web connector via ngrok)."""
    import uvicorn
    from starlette.applications import Starlette
    from starlette.middleware import Middleware
    from starlette.middleware.cors import CORSMiddleware
    from starlette.routing import Route
    from starlette.responses import JSONResponse
    from mcp.server.sse import SseServerTransport

    mcp = BffSdkMCPServer()
    sse = SseServerTransport("/messages/")

    async def handle_sse(request):
        async with sse.connect_sse(
            request.scope, request.receive, request._send
        ) as (read, write):
            await mcp.server.run(read, write, mcp.server.create_initialization_options())

    async def handle_messages(request):
        await sse.handle_post_message(request.scope, request.receive, request._send)

    async def handle_health(request):
        return JSONResponse(_health_surface(transport="sse"))

    app = Starlette(
        routes=[
            Route("/health", handle_health),
            Route("/sse", handle_sse),
            Route("/messages/", handle_messages, methods=["POST"]),
        ],
        middleware=[
            Middleware(
                CORSMiddleware,
                allow_origins=["*"],
                allow_methods=["*"],
                allow_headers=["*"],
            ),
        ],
    )

    logger.info("Starting BFF SDK MCP Server (SSE) on %s:%d", host, port)
    logger.info("  SSE endpoint: http://%s:%d/sse", host, port)
    logger.info("  Health:       http://%s:%d/health", host, port)
    uvicorn.run(app, host=host, port=port, log_level="info")


def main_streamable_http(host: str = "0.0.0.0", port: int = 9090) -> None:
    """Run as Streamable HTTP transport (MCP 2025-03 spec)."""
    import uvicorn
    from contextlib import asynccontextmanager
    from starlette.applications import Starlette
    from starlette.routing import Route
    from starlette.responses import JSONResponse
    from mcp.server.streamable_http import StreamableHTTPServerTransport

    mcp = BffSdkMCPServer()
    session_registry = _StreamableSessionRegistry()

    @asynccontextmanager
    async def lifespan(_app):
        yield
        await session_registry.shutdown()

    async def handle_mcp(request):
        requested_session_id = request.headers.get("mcp-session-id")
        state = await session_registry.get_or_create(
            requested_session_id,
            transport_factory=lambda session_id: StreamableHTTPServerTransport(
                mcp_session_id=session_id,
            ),
            server_runner_factory=lambda transport: mcp.server.run(
                transport._read_stream,
                transport._write_stream,
                mcp.server.create_initialization_options(),
            ),
        )
        try:
            await state.transport.handle_request(request.scope, request.receive, request._send)
        finally:
            if request.method.upper() == "DELETE":
                await session_registry.close(state.session_id)

    async def handle_health(request):
        return JSONResponse(_health_surface(transport="streamable-http"))

    app = Starlette(
        routes=[
            Route("/health", handle_health),
            Route("/mcp", handle_mcp, methods=["GET", "POST", "DELETE"]),
        ],
        lifespan=lifespan,
    )

    logger.info("Starting BFF SDK MCP Server (Streamable HTTP) on %s:%d", host, port)
    logger.info("  MCP endpoint: http://%s:%d/mcp", host, port)
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description="BFF SDK MCP Server")
    parser.add_argument(
        "--transport", choices=["stdio", "sse", "streamable-http"],
        default="stdio", help="Transport mode (default: stdio)",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=9090, help="Bind port (default: 9090)")
    args = parser.parse_args()

    if args.transport == "sse":
        main_sse(host=args.host, port=args.port)
    elif args.transport == "streamable-http":
        main_streamable_http(host=args.host, port=args.port)
    else:
        asyncio.run(main_stdio())
