#!/usr/bin/env python3
"""
Ontology MCP Server

Exposes tools for building/editing ontology schemas via MCP,
enabling an autonomous agent to create/modify ontology definitions through tool calls.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcp.server import InitializationOptions, Server
from mcp.server.stdio import stdio_server
from mcp.types import ServerCapabilities, Tool, ToolsCapability

# Import path setup for repo/container layouts
_this_file = Path(__file__).resolve()
_backend_root = _this_file.parents[1]
_repo_root = _this_file.parents[2] if len(_this_file.parents) > 2 else _backend_root
for _path in (str(_backend_root), str(_repo_root)):
    if _path and _path not in sys.path:
        sys.path.append(_path)

from shared.config.settings import get_settings
from shared.utils.llm_safety import mask_pii

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _bff_api_base_url() -> str:
    """Internal helper for MCP tools that need to call the BFF's REST API."""
    base = get_settings().services.bff_base_url.rstrip("/")
    return f"{base}/api/v1"


def _bff_admin_token() -> Optional[str]:
    return (get_settings().clients.bff_admin_token or "").strip() or None


def _normalize_string(value: Any) -> str:
    """Normalize a value to a trimmed string."""
    return str(value or "").strip()


def _normalize_string_list(value: Any) -> List[str]:
    """Normalize to a list of strings."""
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    out: List[str] = []
    for item in items:
        text = str(item or "").strip()
        if text:
            out.append(text)
    return out


def _mask_observation(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Mask PII in tool observations."""
    return mask_pii(payload or {}, max_string_chars=200)


def _build_error_response(tool_name: str, error: str, hint: Optional[str] = None) -> Dict[str, Any]:
    """Build a structured error response."""
    response: Dict[str, Any] = {"error": error, "tool": tool_name}
    if hint:
        response["hint"] = hint
    return response


class OntologyMCPServer:
    """MCP server for ontology building tools."""

    def __init__(self) -> None:
        self.server = Server("ontology-mcp-server")
        self._working_ontologies: Dict[str, Dict[str, Any]] = {}  # session_id -> ontology
        self._setup_handlers()

    def _get_or_create_ontology(self, session_id: str) -> Dict[str, Any]:
        """Get or create a working ontology for a session."""
        if session_id not in self._working_ontologies:
            self._working_ontologies[session_id] = {
                "id": "",
                "label": "",
                "description": "",
                "parent_class": None,
                "abstract": False,
                "properties": [],
                "relationships": [],
                "metadata": {},
            }
        return self._working_ontologies[session_id]

    def _setup_handlers(self) -> None:
        @self.server.list_tools()
        async def list_tools() -> List[Tool]:
            tool_specs: List[Dict[str, Any]] = [
                # ==================== Initialization ====================
                {
                    "name": "ontology_new",
                    "description": "Create a new empty ontology schema in memory.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string", "description": "Session identifier"},
                            "class_id": {"type": "string", "description": "Class ID (e.g., entity_customer)"},
                            "label": {"type": "string", "description": "Display label"},
                            "description": {"type": "string", "description": "Optional description"},
                        },
                        "required": ["session_id", "class_id", "label"],
                    },
                },
                {
                    "name": "ontology_load",
                    "description": "Load an existing ontology class into memory for editing.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string"},
                            "db_name": {"type": "string"},
                            "class_id": {"type": "string"},
                            "branch": {"type": "string"},
                        },
                        "required": ["session_id", "db_name", "class_id"],
                    },
                },
                {
                    "name": "ontology_reset",
                    "description": "Reset the working ontology to empty state.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string"},
                        },
                        "required": ["session_id"],
                    },
                },
                # ==================== Class Metadata ====================
                {
                    "name": "ontology_set_class_meta",
                    "description": "Set class metadata (label, description, parent_class).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string"},
                            "class_id": {"type": "string"},
                            "label": {"type": "string"},
                            "description": {"type": "string"},
                            "parent_class": {"type": "string"},
                        },
                        "required": ["session_id"],
                    },
                },
                {
                    "name": "ontology_set_abstract",
                    "description": "Set whether the class is abstract.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string"},
                            "abstract": {"type": "boolean"},
                        },
                        "required": ["session_id", "abstract"],
                    },
                },
                # ==================== Property Management ====================
                {
                    "name": "ontology_add_property",
                    "description": "Add a property to the ontology class.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string"},
                            "name": {"type": "string", "description": "Property name"},
                            "type": {"type": "string", "description": "Data type (xsd:string, xsd:integer, etc.)"},
                            "label": {"type": "string", "description": "Display label"},
                            "description": {"type": "string"},
                            "required": {"type": "boolean"},
                            "primary_key": {"type": "boolean"},
                            "title_key": {"type": "boolean"},
                            "default": {"description": "Default value"},
                            "constraints": {"type": "object"},
                        },
                        "required": ["session_id", "name", "type", "label"],
                    },
                },
                {
                    "name": "ontology_update_property",
                    "description": "Update an existing property.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string"},
                            "name": {"type": "string", "description": "Property name to update"},
                            "type": {"type": "string"},
                            "label": {"type": "string"},
                            "description": {"type": "string"},
                            "required": {"type": "boolean"},
                            "primary_key": {"type": "boolean"},
                            "title_key": {"type": "boolean"},
                            "default": {},
                            "constraints": {"type": "object"},
                        },
                        "required": ["session_id", "name"],
                    },
                },
                {
                    "name": "ontology_remove_property",
                    "description": "Remove a property from the ontology class.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string"},
                            "name": {"type": "string", "description": "Property name to remove"},
                        },
                        "required": ["session_id", "name"],
                    },
                },
                {
                    "name": "ontology_set_primary_key",
                    "description": "Set a property as the primary key.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string"},
                            "property_name": {"type": "string"},
                        },
                        "required": ["session_id", "property_name"],
                    },
                },
                # ==================== Relationship Management ====================
                {
                    "name": "ontology_add_relationship",
                    "description": "Add a relationship to the ontology class.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string"},
                            "predicate": {"type": "string", "description": "Relationship predicate (e.g., hasOrders)"},
                            "target": {"type": "string", "description": "Target class ID"},
                            "label": {"type": "string", "description": "Display label"},
                            "cardinality": {"type": "string", "description": "1:1, 1:n, n:1, n:m"},
                            "description": {"type": "string"},
                            "inverse_predicate": {"type": "string"},
                            "inverse_label": {"type": "string"},
                        },
                        "required": ["session_id", "predicate", "target", "label"],
                    },
                },
                {
                    "name": "ontology_update_relationship",
                    "description": "Update an existing relationship.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string"},
                            "predicate": {"type": "string", "description": "Predicate to update"},
                            "target": {"type": "string"},
                            "label": {"type": "string"},
                            "cardinality": {"type": "string"},
                            "description": {"type": "string"},
                            "inverse_predicate": {"type": "string"},
                            "inverse_label": {"type": "string"},
                        },
                        "required": ["session_id", "predicate"],
                    },
                },
                {
                    "name": "ontology_remove_relationship",
                    "description": "Remove a relationship from the ontology class.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string"},
                            "predicate": {"type": "string", "description": "Predicate to remove"},
                        },
                        "required": ["session_id", "predicate"],
                    },
                },
                # ==================== Schema Inference ====================
                {
                    "name": "ontology_infer_schema_from_data",
                    "description": "Infer schema types from sample data using FunnelClient.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "columns": {"type": "array", "items": {"type": "string"}},
                            "data": {"type": "array", "items": {"type": "array"}},
                            "class_name": {"type": "string"},
                        },
                        "required": ["columns", "data"],
                    },
                },
                {
                    "name": "ontology_suggest_mappings",
                    "description": "Suggest field mappings between source schema and target ontology.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "source_schema": {
                                "type": "array",
                                "items": {"type": "object"},
                                "description": "Source fields [{name, type, ...}]",
                            },
                            "target_class_id": {"type": "string"},
                            "db_name": {"type": "string"},
                            "branch": {"type": "string"},
                            "sample_data": {"type": "array"},
                        },
                        "required": ["source_schema", "target_class_id", "db_name"],
                    },
                },
                # ==================== Validation ====================
                {
                    "name": "ontology_validate",
                    "description": "Validate the working ontology structure.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string"},
                        },
                        "required": ["session_id"],
                    },
                },
                {
                    "name": "ontology_check_relationships",
                    "description": "Check if relationship targets exist in the database.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string"},
                            "db_name": {"type": "string"},
                            "branch": {"type": "string"},
                        },
                        "required": ["session_id", "db_name"],
                    },
                },
                {
                    "name": "ontology_check_circular_refs",
                    "description": "Check for circular references in parent class chain.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string"},
                            "db_name": {"type": "string"},
                            "branch": {"type": "string"},
                        },
                        "required": ["session_id", "db_name"],
                    },
                },
                # ==================== Query ====================
                {
                    "name": "ontology_list_classes",
                    "description": "List all ontology classes in the database.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string"},
                            "branch": {"type": "string"},
                            "limit": {"type": "integer"},
                            "offset": {"type": "integer"},
                        },
                        "required": ["db_name"],
                    },
                },
                {
                    "name": "ontology_get_class",
                    "description": "Get details of a specific ontology class.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string"},
                            "class_id": {"type": "string"},
                            "branch": {"type": "string"},
                        },
                        "required": ["db_name", "class_id"],
                    },
                },
                {
                    "name": "ontology_search_classes",
                    "description": "Search ontology classes by label or property names.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string"},
                            "query": {"type": "string", "description": "Search query"},
                            "branch": {"type": "string"},
                            "limit": {"type": "integer"},
                        },
                        "required": ["db_name", "query"],
                    },
                },
                # ==================== Save ====================
                {
                    "name": "ontology_create",
                    "description": "Create the ontology class in the database.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string"},
                            "db_name": {"type": "string"},
                            "branch": {"type": "string"},
                        },
                        "required": ["session_id", "db_name"],
                    },
                },
                {
                    "name": "ontology_update",
                    "description": "Update an existing ontology class in the database.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string"},
                            "db_name": {"type": "string"},
                            "class_id": {"type": "string"},
                            "branch": {"type": "string"},
                            "expected_seq": {"type": "integer"},
                        },
                        "required": ["session_id", "db_name", "class_id"],
                    },
                },
                {
                    "name": "ontology_preview",
                    "description": "Preview the working ontology without saving.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {"type": "string"},
                        },
                        "required": ["session_id"],
                    },
                },
            ]

            return [
                Tool(
                    name=spec["name"],
                    description=spec.get("description", ""),
                    inputSchema=spec.get("inputSchema", {"type": "object", "properties": {}}),
                )
                for spec in tool_specs
            ]

        @self.server.call_tool()
        async def call_tool(name: str, arguments: Dict[str, Any]) -> Any:
            """Handle tool calls."""
            try:
                return await self._handle_tool_call(name, arguments)
            except Exception as exc:
                logger.warning("Ontology MCP tool failed: tool=%s err=%s", name, exc)
                return _build_error_response(name, str(exc))

    async def _handle_tool_call(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Route tool calls to implementations."""
        # ==================== Initialization ====================
        if name == "ontology_new":
            return await self._tool_ontology_new(arguments)
        if name == "ontology_load":
            return await self._tool_ontology_load(arguments)
        if name == "ontology_reset":
            return await self._tool_ontology_reset(arguments)

        # ==================== Class Metadata ====================
        if name == "ontology_set_class_meta":
            return await self._tool_ontology_set_class_meta(arguments)
        if name == "ontology_set_abstract":
            return await self._tool_ontology_set_abstract(arguments)

        # ==================== Property Management ====================
        if name == "ontology_add_property":
            return await self._tool_ontology_add_property(arguments)
        if name == "ontology_update_property":
            return await self._tool_ontology_update_property(arguments)
        if name == "ontology_remove_property":
            return await self._tool_ontology_remove_property(arguments)
        if name == "ontology_set_primary_key":
            return await self._tool_ontology_set_primary_key(arguments)

        # ==================== Relationship Management ====================
        if name == "ontology_add_relationship":
            return await self._tool_ontology_add_relationship(arguments)
        if name == "ontology_update_relationship":
            return await self._tool_ontology_update_relationship(arguments)
        if name == "ontology_remove_relationship":
            return await self._tool_ontology_remove_relationship(arguments)

        # ==================== Schema Inference ====================
        if name == "ontology_infer_schema_from_data":
            return await self._tool_ontology_infer_schema_from_data(arguments)
        if name == "ontology_suggest_mappings":
            return await self._tool_ontology_suggest_mappings(arguments)

        # ==================== Validation ====================
        if name == "ontology_validate":
            return await self._tool_ontology_validate(arguments)
        if name == "ontology_check_relationships":
            return await self._tool_ontology_check_relationships(arguments)
        if name == "ontology_check_circular_refs":
            return await self._tool_ontology_check_circular_refs(arguments)

        # ==================== Query ====================
        if name == "ontology_list_classes":
            return await self._tool_ontology_list_classes(arguments)
        if name == "ontology_get_class":
            return await self._tool_ontology_get_class(arguments)
        if name == "ontology_search_classes":
            return await self._tool_ontology_search_classes(arguments)

        # ==================== Save ====================
        if name == "ontology_create":
            return await self._tool_ontology_create(arguments)
        if name == "ontology_update":
            return await self._tool_ontology_update(arguments)
        if name == "ontology_preview":
            return await self._tool_ontology_preview(arguments)

        return _build_error_response(name, f"Unknown tool: {name}")

    # ==================== Tool Implementations ====================

    async def _tool_ontology_new(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new empty ontology in memory."""
        session_id = _normalize_string(args.get("session_id"))
        class_id = _normalize_string(args.get("class_id"))
        label = _normalize_string(args.get("label"))
        description = _normalize_string(args.get("description"))

        if not session_id:
            return _build_error_response("ontology_new", "session_id is required")
        if not class_id:
            return _build_error_response("ontology_new", "class_id is required")
        if not label:
            return _build_error_response("ontology_new", "label is required")

        ontology = {
            "id": class_id,
            "label": label,
            "description": description or None,
            "parent_class": None,
            "abstract": False,
            "properties": [],
            "relationships": [],
            "metadata": {},
        }
        self._working_ontologies[session_id] = ontology

        return {
            "status": "success",
            "message": f"Created new ontology '{class_id}'",
            "ontology_summary": {
                "id": class_id,
                "label": label,
                "property_count": 0,
                "relationship_count": 0,
            },
        }

    async def _tool_ontology_load(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Load an existing ontology class into memory."""
        session_id = _normalize_string(args.get("session_id"))
        db_name = _normalize_string(args.get("db_name"))
        class_id = _normalize_string(args.get("class_id"))
        branch = _normalize_string(args.get("branch")) or "main"

        if not session_id or not db_name or not class_id:
            return _build_error_response("ontology_load", "session_id, db_name, and class_id are required")

        try:
            # Import here to avoid circular imports
            from bff.services.oms_client import OMSClient
            async with OMSClient() as client:
                result = await client.get_ontology(db_name, class_id, branch=branch)
                data = result.get("data") if isinstance(result, dict) else None
                if not data:
                    return _build_error_response("ontology_load", f"Class '{class_id}' not found")

                ontology = {
                    "id": data.get("id", class_id),
                    "label": data.get("label", ""),
                    "description": data.get("description"),
                    "parent_class": data.get("parent_class"),
                    "abstract": bool(data.get("abstract", False)),
                    "properties": list(data.get("properties") or []),
                    "relationships": list(data.get("relationships") or []),
                    "metadata": dict(data.get("metadata") or {}),
                    "_seq": data.get("seq"),  # For optimistic locking
                }
                self._working_ontologies[session_id] = ontology

                return {
                    "status": "success",
                    "message": f"Loaded ontology '{class_id}'",
                    "ontology_summary": {
                        "id": ontology["id"],
                        "label": ontology["label"],
                        "property_count": len(ontology["properties"]),
                        "relationship_count": len(ontology["relationships"]),
                    },
                }
        except Exception as exc:
            return _build_error_response("ontology_load", str(exc))

    async def _tool_ontology_reset(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Reset working ontology to empty state."""
        session_id = _normalize_string(args.get("session_id"))
        if not session_id:
            return _build_error_response("ontology_reset", "session_id is required")

        if session_id in self._working_ontologies:
            del self._working_ontologies[session_id]

        return {"status": "success", "message": "Ontology reset to empty state"}

    async def _tool_ontology_set_class_meta(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Set class metadata."""
        session_id = _normalize_string(args.get("session_id"))
        if not session_id:
            return _build_error_response("ontology_set_class_meta", "session_id is required")

        ontology = self._get_or_create_ontology(session_id)

        if "class_id" in args:
            ontology["id"] = _normalize_string(args["class_id"])
        if "label" in args:
            ontology["label"] = _normalize_string(args["label"])
        if "description" in args:
            ontology["description"] = _normalize_string(args["description"]) or None
        if "parent_class" in args:
            ontology["parent_class"] = _normalize_string(args["parent_class"]) or None

        return {
            "status": "success",
            "updated": {
                "id": ontology.get("id"),
                "label": ontology.get("label"),
                "description": ontology.get("description"),
                "parent_class": ontology.get("parent_class"),
            },
        }

    async def _tool_ontology_set_abstract(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Set whether class is abstract."""
        session_id = _normalize_string(args.get("session_id"))
        if not session_id:
            return _build_error_response("ontology_set_abstract", "session_id is required")

        ontology = self._get_or_create_ontology(session_id)
        ontology["abstract"] = bool(args.get("abstract", False))

        return {"status": "success", "abstract": ontology["abstract"]}

    async def _tool_ontology_add_property(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Add a property to the ontology."""
        session_id = _normalize_string(args.get("session_id"))
        name = _normalize_string(args.get("name"))
        prop_type = _normalize_string(args.get("type"))
        label = _normalize_string(args.get("label"))

        if not session_id or not name or not prop_type or not label:
            return _build_error_response("ontology_add_property", "session_id, name, type, and label are required")

        ontology = self._get_or_create_ontology(session_id)
        properties = ontology.get("properties", [])

        # Check for duplicate
        existing_names = {p.get("name") for p in properties}
        if name in existing_names:
            return _build_error_response("ontology_add_property", f"Property '{name}' already exists")

        new_prop = {
            "name": name,
            "type": prop_type,
            "label": label,
            "required": bool(args.get("required", False)),
            "primary_key": bool(args.get("primary_key", False)),
            "title_key": bool(args.get("title_key", False)),
        }
        if args.get("description"):
            new_prop["description"] = _normalize_string(args["description"])
        if args.get("default") is not None:
            new_prop["default"] = args["default"]
        if args.get("constraints"):
            new_prop["constraints"] = args["constraints"]

        properties.append(new_prop)
        ontology["properties"] = properties

        return {
            "status": "success",
            "message": f"Added property '{name}'",
            "property_count": len(properties),
        }

    async def _tool_ontology_update_property(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing property."""
        session_id = _normalize_string(args.get("session_id"))
        name = _normalize_string(args.get("name"))

        if not session_id or not name:
            return _build_error_response("ontology_update_property", "session_id and name are required")

        ontology = self._get_or_create_ontology(session_id)
        properties = ontology.get("properties", [])

        prop_idx = next((i for i, p in enumerate(properties) if p.get("name") == name), None)
        if prop_idx is None:
            return _build_error_response("ontology_update_property", f"Property '{name}' not found")

        prop = properties[prop_idx]
        if "type" in args:
            prop["type"] = _normalize_string(args["type"])
        if "label" in args:
            prop["label"] = _normalize_string(args["label"])
        if "description" in args:
            prop["description"] = _normalize_string(args["description"]) or None
        if "required" in args:
            prop["required"] = bool(args["required"])
        if "primary_key" in args:
            prop["primary_key"] = bool(args["primary_key"])
        if "title_key" in args:
            prop["title_key"] = bool(args["title_key"])
        if "default" in args:
            prop["default"] = args["default"]
        if "constraints" in args:
            prop["constraints"] = args["constraints"]

        return {"status": "success", "message": f"Updated property '{name}'"}

    async def _tool_ontology_remove_property(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Remove a property."""
        session_id = _normalize_string(args.get("session_id"))
        name = _normalize_string(args.get("name"))

        if not session_id or not name:
            return _build_error_response("ontology_remove_property", "session_id and name are required")

        ontology = self._get_or_create_ontology(session_id)
        properties = ontology.get("properties", [])

        new_props = [p for p in properties if p.get("name") != name]
        if len(new_props) == len(properties):
            return _build_error_response("ontology_remove_property", f"Property '{name}' not found")

        ontology["properties"] = new_props
        return {"status": "success", "message": f"Removed property '{name}'", "property_count": len(new_props)}

    async def _tool_ontology_set_primary_key(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Set a property as primary key."""
        session_id = _normalize_string(args.get("session_id"))
        property_name = _normalize_string(args.get("property_name"))

        if not session_id or not property_name:
            return _build_error_response("ontology_set_primary_key", "session_id and property_name are required")

        ontology = self._get_or_create_ontology(session_id)
        properties = ontology.get("properties", [])

        found = False
        for prop in properties:
            if prop.get("name") == property_name:
                prop["primary_key"] = True
                found = True
            else:
                prop["primary_key"] = False

        if not found:
            return _build_error_response("ontology_set_primary_key", f"Property '{property_name}' not found")

        return {"status": "success", "primary_key": property_name}

    async def _tool_ontology_add_relationship(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Add a relationship to the ontology."""
        session_id = _normalize_string(args.get("session_id"))
        predicate = _normalize_string(args.get("predicate"))
        target = _normalize_string(args.get("target"))
        label = _normalize_string(args.get("label"))

        if not session_id or not predicate or not target or not label:
            return _build_error_response(
                "ontology_add_relationship", "session_id, predicate, target, and label are required"
            )

        ontology = self._get_or_create_ontology(session_id)
        relationships = ontology.get("relationships", [])

        # Check for duplicate
        existing_predicates = {r.get("predicate") for r in relationships}
        if predicate in existing_predicates:
            return _build_error_response("ontology_add_relationship", f"Relationship '{predicate}' already exists")

        cardinality = _normalize_string(args.get("cardinality")) or "1:n"
        valid_cardinalities = ["1:1", "1:n", "n:1", "n:m"]
        if cardinality not in valid_cardinalities:
            return _build_error_response(
                "ontology_add_relationship", f"Invalid cardinality '{cardinality}'. Use: {valid_cardinalities}"
            )

        new_rel = {
            "predicate": predicate,
            "target": target,
            "label": label,
            "cardinality": cardinality,
        }
        if args.get("description"):
            new_rel["description"] = _normalize_string(args["description"])
        if args.get("inverse_predicate"):
            new_rel["inverse_predicate"] = _normalize_string(args["inverse_predicate"])
        if args.get("inverse_label"):
            new_rel["inverse_label"] = _normalize_string(args["inverse_label"])

        relationships.append(new_rel)
        ontology["relationships"] = relationships

        return {
            "status": "success",
            "message": f"Added relationship '{predicate}' -> '{target}'",
            "relationship_count": len(relationships),
        }

    async def _tool_ontology_update_relationship(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing relationship."""
        session_id = _normalize_string(args.get("session_id"))
        predicate = _normalize_string(args.get("predicate"))

        if not session_id or not predicate:
            return _build_error_response("ontology_update_relationship", "session_id and predicate are required")

        ontology = self._get_or_create_ontology(session_id)
        relationships = ontology.get("relationships", [])

        rel_idx = next((i for i, r in enumerate(relationships) if r.get("predicate") == predicate), None)
        if rel_idx is None:
            return _build_error_response("ontology_update_relationship", f"Relationship '{predicate}' not found")

        rel = relationships[rel_idx]
        if "target" in args:
            rel["target"] = _normalize_string(args["target"])
        if "label" in args:
            rel["label"] = _normalize_string(args["label"])
        if "cardinality" in args:
            rel["cardinality"] = _normalize_string(args["cardinality"])
        if "description" in args:
            rel["description"] = _normalize_string(args["description"]) or None
        if "inverse_predicate" in args:
            rel["inverse_predicate"] = _normalize_string(args["inverse_predicate"]) or None
        if "inverse_label" in args:
            rel["inverse_label"] = _normalize_string(args["inverse_label"]) or None

        return {"status": "success", "message": f"Updated relationship '{predicate}'"}

    async def _tool_ontology_remove_relationship(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Remove a relationship."""
        session_id = _normalize_string(args.get("session_id"))
        predicate = _normalize_string(args.get("predicate"))

        if not session_id or not predicate:
            return _build_error_response("ontology_remove_relationship", "session_id and predicate are required")

        ontology = self._get_or_create_ontology(session_id)
        relationships = ontology.get("relationships", [])

        new_rels = [r for r in relationships if r.get("predicate") != predicate]
        if len(new_rels) == len(relationships):
            return _build_error_response("ontology_remove_relationship", f"Relationship '{predicate}' not found")

        ontology["relationships"] = new_rels
        return {"status": "success", "message": f"Removed relationship '{predicate}'", "relationship_count": len(new_rels)}

    async def _tool_ontology_infer_schema_from_data(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Infer schema from sample data using FunnelClient."""
        columns = _normalize_string_list(args.get("columns"))
        data = args.get("data")
        class_name = _normalize_string(args.get("class_name"))

        if not columns or not isinstance(data, list):
            return _build_error_response("ontology_infer_schema_from_data", "columns and data are required")

        try:
            from bff.services.funnel_client import FunnelClient
            async with FunnelClient() as client:
                result = await client.analyze_and_suggest_schema(
                    data=data,
                    columns=columns,
                    class_name=class_name or None,
                )
                return {
                    "status": "success",
                    "analysis": result.get("analysis"),
                    "schema_suggestion": result.get("schema_suggestion"),
                }
        except Exception as exc:
            return _build_error_response("ontology_infer_schema_from_data", str(exc))

    async def _tool_ontology_suggest_mappings(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Suggest mappings between source schema and target ontology."""
        source_schema = args.get("source_schema")
        target_class_id = _normalize_string(args.get("target_class_id"))
        db_name = _normalize_string(args.get("db_name"))
        branch = _normalize_string(args.get("branch")) or "main"
        sample_data = args.get("sample_data")

        if not source_schema or not target_class_id or not db_name:
            return _build_error_response(
                "ontology_suggest_mappings", "source_schema, target_class_id, and db_name are required"
            )

        try:
            # Get target ontology schema
            from bff.services.oms_client import OMSClient
            async with OMSClient() as client:
                result = await client.get_ontology(db_name, target_class_id, branch=branch)
                target_data = result.get("data") if isinstance(result, dict) else None
                if not target_data:
                    return _build_error_response("ontology_suggest_mappings", f"Target class '{target_class_id}' not found")

            # Build target schema from properties
            target_properties = target_data.get("properties") or []
            target_schema = [
                {
                    "name": p.get("name"),
                    "type": p.get("type", "xsd:string"),
                    "label": p.get("label"),
                    "aliases": [p.get("label")] if p.get("label") else [],
                }
                for p in target_properties
            ]

            # Use MappingSuggestionService
            from bff.services.mapping_suggestion_service import MappingSuggestionService
            service = MappingSuggestionService()
            suggestion = service.suggest_mappings(
                source_schema=source_schema,
                target_schema=target_schema,
                sample_data=sample_data,
            )

            return {
                "status": "success",
                "mappings": [
                    {
                        "source_field": m.source_field,
                        "target_field": m.target_field,
                        "confidence": m.confidence,
                        "match_type": m.match_type,
                        "reasons": m.reasons,
                    }
                    for m in suggestion.mappings
                ],
                "unmapped_source_fields": suggestion.unmapped_source_fields,
                "unmapped_target_fields": suggestion.unmapped_target_fields,
                "overall_confidence": suggestion.overall_confidence,
            }
        except Exception as exc:
            return _build_error_response("ontology_suggest_mappings", str(exc))

    async def _tool_ontology_validate(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Validate the working ontology structure."""
        session_id = _normalize_string(args.get("session_id"))
        if not session_id:
            return _build_error_response("ontology_validate", "session_id is required")

        ontology = self._working_ontologies.get(session_id)
        if not ontology:
            return _build_error_response("ontology_validate", "No working ontology found")

        errors: List[str] = []
        warnings: List[str] = []

        # Required fields
        if not ontology.get("id"):
            errors.append("Class ID is required")
        if not ontology.get("label"):
            errors.append("Label is required")

        # Property validation
        properties = ontology.get("properties", [])
        prop_names = [p.get("name") for p in properties]
        if len(prop_names) != len(set(prop_names)):
            errors.append("Duplicate property names found")

        primary_keys = [p for p in properties if p.get("primary_key")]
        if len(primary_keys) > 1:
            warnings.append("Multiple primary keys defined; only one is typically expected")
        if not primary_keys:
            warnings.append("No primary key defined")

        # Relationship validation
        relationships = ontology.get("relationships", [])
        predicates = [r.get("predicate") for r in relationships]
        if len(predicates) != len(set(predicates)):
            errors.append("Duplicate relationship predicates found")

        valid_cardinalities = {"1:1", "1:n", "n:1", "n:m"}
        for rel in relationships:
            if rel.get("cardinality") not in valid_cardinalities:
                errors.append(f"Invalid cardinality for relationship '{rel.get('predicate')}': {rel.get('cardinality')}")

        status_value = "valid" if not errors else "invalid"
        return {
            "status": status_value,
            "errors": errors,
            "warnings": warnings,
            "summary": {
                "id": ontology.get("id"),
                "label": ontology.get("label"),
                "property_count": len(properties),
                "relationship_count": len(relationships),
            },
        }

    async def _tool_ontology_check_relationships(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Check if relationship targets exist."""
        session_id = _normalize_string(args.get("session_id"))
        db_name = _normalize_string(args.get("db_name"))
        branch = _normalize_string(args.get("branch")) or "main"

        if not session_id or not db_name:
            return _build_error_response("ontology_check_relationships", "session_id and db_name are required")

        ontology = self._working_ontologies.get(session_id)
        if not ontology:
            return _build_error_response("ontology_check_relationships", "No working ontology found")

        relationships = ontology.get("relationships", [])
        if not relationships:
            return {"status": "valid", "message": "No relationships to check"}

        try:
            from bff.services.oms_client import OMSClient
            async with OMSClient() as client:
                result = await client.list_ontologies(db_name, branch=branch)
                classes_data = result.get("data") or result.get("classes") or []
                existing_ids = {c.get("id") for c in classes_data if isinstance(c, dict)}

            missing_targets: List[str] = []
            for rel in relationships:
                target = rel.get("target")
                if target and target not in existing_ids:
                    missing_targets.append(target)

            if missing_targets:
                return {
                    "status": "warning",
                    "missing_targets": missing_targets,
                    "message": f"Relationship targets not found: {missing_targets}",
                }
            return {"status": "valid", "message": "All relationship targets exist"}
        except Exception as exc:
            return _build_error_response("ontology_check_relationships", str(exc))

    async def _tool_ontology_check_circular_refs(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Check for circular references in parent class chain."""
        session_id = _normalize_string(args.get("session_id"))
        db_name = _normalize_string(args.get("db_name"))
        branch = _normalize_string(args.get("branch")) or "main"

        if not session_id or not db_name:
            return _build_error_response("ontology_check_circular_refs", "session_id and db_name are required")

        ontology = self._working_ontologies.get(session_id)
        if not ontology:
            return _build_error_response("ontology_check_circular_refs", "No working ontology found")

        parent_class = ontology.get("parent_class")
        if not parent_class:
            return {"status": "valid", "message": "No parent class defined"}

        class_id = ontology.get("id")
        if not class_id:
            return _build_error_response("ontology_check_circular_refs", "Class ID not set")

        try:
            from bff.services.oms_client import OMSClient
            async with OMSClient() as client:
                visited = {class_id}
                current = parent_class

                while current:
                    if current in visited:
                        return {
                            "status": "invalid",
                            "circular_ref": True,
                            "message": f"Circular reference detected: {current} already in chain",
                        }
                    visited.add(current)

                    result = await client.get_ontology(db_name, current, branch=branch)
                    data = result.get("data") if isinstance(result, dict) else None
                    if not data:
                        break
                    current = data.get("parent_class")

            return {"status": "valid", "message": "No circular references found"}
        except Exception as exc:
            return _build_error_response("ontology_check_circular_refs", str(exc))

    async def _tool_ontology_list_classes(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """List all ontology classes."""
        db_name = _normalize_string(args.get("db_name"))
        branch = _normalize_string(args.get("branch")) or "main"
        limit = int(args.get("limit") or 100)
        offset = int(args.get("offset") or 0)

        if not db_name:
            return _build_error_response("ontology_list_classes", "db_name is required")

        try:
            from bff.services.oms_client import OMSClient
            async with OMSClient() as client:
                result = await client.list_ontologies(db_name, branch=branch)
                classes_data = result.get("data") or result.get("classes") or []

                # Apply pagination
                paginated = classes_data[offset : offset + limit]
                summaries = [
                    {
                        "id": c.get("id"),
                        "label": c.get("label"),
                        "parent_class": c.get("parent_class"),
                        "abstract": c.get("abstract", False),
                        "property_count": len(c.get("properties") or []),
                        "relationship_count": len(c.get("relationships") or []),
                    }
                    for c in paginated
                    if isinstance(c, dict)
                ]

                return {
                    "status": "success",
                    "classes": summaries,
                    "total": len(classes_data),
                    "offset": offset,
                    "limit": limit,
                }
        except Exception as exc:
            return _build_error_response("ontology_list_classes", str(exc))

    async def _tool_ontology_get_class(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Get details of a specific ontology class."""
        db_name = _normalize_string(args.get("db_name"))
        class_id = _normalize_string(args.get("class_id"))
        branch = _normalize_string(args.get("branch")) or "main"

        if not db_name or not class_id:
            return _build_error_response("ontology_get_class", "db_name and class_id are required")

        try:
            from bff.services.oms_client import OMSClient
            async with OMSClient() as client:
                result = await client.get_ontology(db_name, class_id, branch=branch)
                data = result.get("data") if isinstance(result, dict) else None
                if not data:
                    return _build_error_response("ontology_get_class", f"Class '{class_id}' not found")

                return {
                    "status": "success",
                    "class": {
                        "id": data.get("id"),
                        "label": data.get("label"),
                        "description": data.get("description"),
                        "parent_class": data.get("parent_class"),
                        "abstract": data.get("abstract", False),
                        "properties": data.get("properties") or [],
                        "relationships": data.get("relationships") or [],
                        "seq": data.get("seq"),
                    },
                }
        except Exception as exc:
            return _build_error_response("ontology_get_class", str(exc))

    async def _tool_ontology_search_classes(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Search ontology classes by label or property names."""
        db_name = _normalize_string(args.get("db_name"))
        query = _normalize_string(args.get("query"))
        branch = _normalize_string(args.get("branch")) or "main"
        limit = int(args.get("limit") or 20)

        if not db_name or not query:
            return _build_error_response("ontology_search_classes", "db_name and query are required")

        try:
            from bff.services.oms_client import OMSClient
            async with OMSClient() as client:
                result = await client.list_ontologies(db_name, branch=branch)
                classes_data = result.get("data") or result.get("classes") or []

                query_lower = query.lower()
                matches: List[Dict[str, Any]] = []

                for c in classes_data:
                    if not isinstance(c, dict):
                        continue

                    # Match by ID or label
                    label = str(c.get("label") or "")
                    class_id = str(c.get("id") or "")
                    if query_lower in label.lower() or query_lower in class_id.lower():
                        matches.append(c)
                        continue

                    # Match by property names
                    properties = c.get("properties") or []
                    for prop in properties:
                        prop_name = str(prop.get("name") or "")
                        prop_label = str(prop.get("label") or "")
                        if query_lower in prop_name.lower() or query_lower in prop_label.lower():
                            matches.append(c)
                            break

                matches = matches[:limit]
                summaries = [
                    {
                        "id": c.get("id"),
                        "label": c.get("label"),
                        "parent_class": c.get("parent_class"),
                        "property_count": len(c.get("properties") or []),
                    }
                    for c in matches
                ]

                return {
                    "status": "success",
                    "query": query,
                    "results": summaries,
                    "result_count": len(summaries),
                }
        except Exception as exc:
            return _build_error_response("ontology_search_classes", str(exc))

    async def _tool_ontology_create(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Create the ontology class in the database."""
        session_id = _normalize_string(args.get("session_id"))
        db_name = _normalize_string(args.get("db_name"))
        branch = _normalize_string(args.get("branch")) or "main"

        if not session_id or not db_name:
            return _build_error_response("ontology_create", "session_id and db_name are required")

        ontology = self._working_ontologies.get(session_id)
        if not ontology:
            return _build_error_response("ontology_create", "No working ontology found")

        # Validate before save
        validation = await self._tool_ontology_validate({"session_id": session_id})
        if validation.get("status") == "invalid":
            return {
                "status": "invalid",
                "errors": validation.get("errors", []),
                "hint": "Fix validation errors before creating",
            }

        try:
            from bff.services.oms_client import OMSClient
            # Build payload
            payload = {
                "id": ontology.get("id"),
                "label": ontology.get("label"),
                "description": ontology.get("description"),
                "parent_class": ontology.get("parent_class"),
                "abstract": ontology.get("abstract", False),
                "properties": ontology.get("properties", []),
                "relationships": ontology.get("relationships", []),
                "metadata": ontology.get("metadata", {}),
            }

            async with OMSClient() as client:
                result = await client.create_ontology(db_name, payload, branch=branch)

            return {
                "status": "success",
                "message": f"Created ontology class '{ontology.get('id')}'",
                "class_id": ontology.get("id"),
                "result": _mask_observation(result) if isinstance(result, dict) else result,
            }
        except Exception as exc:
            return _build_error_response("ontology_create", str(exc))

    async def _tool_ontology_update(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing ontology class."""
        session_id = _normalize_string(args.get("session_id"))
        db_name = _normalize_string(args.get("db_name"))
        class_id = _normalize_string(args.get("class_id"))
        branch = _normalize_string(args.get("branch")) or "main"
        expected_seq = args.get("expected_seq")

        if not session_id or not db_name or not class_id:
            return _build_error_response("ontology_update", "session_id, db_name, and class_id are required")

        ontology = self._working_ontologies.get(session_id)
        if not ontology:
            return _build_error_response("ontology_update", "No working ontology found")

        # Use stored seq if not provided
        if expected_seq is None:
            expected_seq = ontology.get("_seq")
        if expected_seq is None:
            return _build_error_response(
                "ontology_update",
                "expected_seq is required for update (use ontology_load to get current seq)",
            )

        try:
            from bff.services.oms_client import OMSClient
            payload = {
                "label": ontology.get("label"),
                "description": ontology.get("description"),
                "parent_class": ontology.get("parent_class"),
                "abstract": ontology.get("abstract", False),
                "properties": ontology.get("properties", []),
                "relationships": ontology.get("relationships", []),
                "metadata": ontology.get("metadata", {}),
            }

            async with OMSClient() as client:
                result = await client.update_ontology(
                    db_name,
                    class_id,
                    payload,
                    expected_seq=int(expected_seq),
                    branch=branch,
                )

            return {
                "status": "success",
                "message": f"Updated ontology class '{class_id}'",
                "class_id": class_id,
                "result": _mask_observation(result) if isinstance(result, dict) else result,
            }
        except Exception as exc:
            return _build_error_response("ontology_update", str(exc))

    async def _tool_ontology_preview(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Preview the working ontology without saving."""
        session_id = _normalize_string(args.get("session_id"))
        if not session_id:
            return _build_error_response("ontology_preview", "session_id is required")

        ontology = self._working_ontologies.get(session_id)
        if not ontology:
            return _build_error_response("ontology_preview", "No working ontology found")

        # Remove internal fields
        preview = {k: v for k, v in ontology.items() if not k.startswith("_")}
        return {
            "status": "success",
            "ontology": preview,
        }


# Entry point for running as MCP server
async def main() -> None:
    import os
    server = OntologyMCPServer()
    async with stdio_server() as (read, write):
        init_options = InitializationOptions(
            server_name="ontology-mcp-server",
            server_version=os.environ.get("SPICE_VERSION", "0.1.0"),
            capabilities=ServerCapabilities(tools=ToolsCapability()),
        )
        await server.server.run(read, write, init_options)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
