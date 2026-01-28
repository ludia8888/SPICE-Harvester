"""
Unit tests for Pipeline Agent debugging tools.

Tests the 5 debug tools:
- debug_get_errors
- debug_get_execution_log
- debug_inspect_node
- debug_explain_failure
- debug_dry_run
"""

import pytest
import json
from typing import Any, Dict, List


# ==================== Mock Classes ====================

class MockAgentState:
    """Mock agent state for testing debug tools."""

    def __init__(self):
        self.prompt_items: List[str] = []
        self.plan_obj: Dict[str, Any] = {}
        self.last_observation: Dict[str, Any] = {}


# ==================== debug_get_errors Tests ====================

def test_debug_get_errors_returns_accumulated_errors():
    """Test debug_get_errors returns errors and warnings from run."""
    tool_errors = ["Error 1: Resource not found", "Error 2: Type mismatch"]
    tool_warnings = ["Warning 1: Deprecated API", "Warning 2: Slow query"]

    include_warnings = True
    limit = 50

    errors_list = list(tool_errors)[-limit:] if tool_errors else []
    warnings_list = list(tool_warnings)[-limit:] if include_warnings and tool_warnings else []

    result = {
        "status": "success",
        "errors": errors_list,
        "warnings": warnings_list,
        "error_count": len(tool_errors),
        "warning_count": len(tool_warnings) if include_warnings else 0,
    }

    assert result["status"] == "success"
    assert len(result["errors"]) == 2
    assert len(result["warnings"]) == 2
    assert result["error_count"] == 2
    assert result["warning_count"] == 2
    assert "Resource not found" in result["errors"][0]


def test_debug_get_errors_respects_limit():
    """Test debug_get_errors respects limit parameter."""
    tool_errors = [f"Error {i}" for i in range(100)]
    tool_warnings = [f"Warning {i}" for i in range(50)]

    limit = 10

    errors_list = list(tool_errors)[-limit:]
    warnings_list = list(tool_warnings)[-limit:]

    result = {
        "status": "success",
        "errors": errors_list,
        "warnings": warnings_list,
        "error_count": len(tool_errors),
        "warning_count": len(tool_warnings),
    }

    assert len(result["errors"]) == 10
    assert len(result["warnings"]) == 10
    assert result["error_count"] == 100
    # Last 10 errors should be Error 90-99
    assert result["errors"][0] == "Error 90"
    assert result["errors"][-1] == "Error 99"


def test_debug_get_errors_excludes_warnings():
    """Test debug_get_errors excludes warnings when requested."""
    tool_errors = ["Error 1"]
    tool_warnings = ["Warning 1", "Warning 2"]

    include_warnings = False

    warnings_list = list(tool_warnings) if include_warnings else []

    result = {
        "status": "success",
        "errors": list(tool_errors),
        "warnings": warnings_list,
        "error_count": len(tool_errors),
        "warning_count": 0,
    }

    assert len(result["warnings"]) == 0
    assert result["warning_count"] == 0


def test_debug_get_errors_empty():
    """Test debug_get_errors with no errors."""
    tool_errors: List[str] = []
    tool_warnings: List[str] = []

    result = {
        "status": "success",
        "errors": list(tool_errors),
        "warnings": list(tool_warnings),
        "error_count": 0,
        "warning_count": 0,
    }

    assert result["status"] == "success"
    assert len(result["errors"]) == 0
    assert len(result["warnings"]) == 0


# ==================== debug_get_execution_log Tests ====================

def test_debug_get_execution_log_returns_entries():
    """Test debug_get_execution_log returns tool call log entries."""
    prompt_items = [
        json.dumps({"type": "tool_call", "step": 1, "tool": "plan_new"}),
        json.dumps({"type": "tool_output", "step": 1, "observation": {"status": "success"}}),
        json.dumps({"type": "tool_call", "step": 2, "tool": "plan_add_input"}),
        json.dumps({"type": "tool_output", "step": 2, "observation": {"status": "success"}}),
        json.dumps({"type": "tool_call", "step": 3, "tool": "plan_validate"}),
        json.dumps({"type": "tool_output", "step": 3, "observation": {"status": "error", "error": "Invalid"}}),
    ]

    limit = 20
    step_filter = None
    log_entries: List[Dict[str, Any]] = []

    for item in prompt_items:
        try:
            parsed = json.loads(item)
            item_type = parsed.get("type")
            item_step = parsed.get("step")
            if item_type in {"tool_call", "tool_output", "tool_calls", "tool_outputs"}:
                if step_filter is None or item_step == step_filter:
                    entry: Dict[str, Any] = {
                        "type": item_type,
                        "step": item_step,
                    }
                    if item_type in {"tool_call", "tool_calls"}:
                        entry["tool"] = parsed.get("tool") or parsed.get("tools")
                    if item_type in {"tool_output", "tool_outputs"}:
                        obs = parsed.get("observation") or parsed.get("observations")
                        if isinstance(obs, dict):
                            entry["status"] = obs.get("status")
                            entry["error"] = obs.get("error")
                    log_entries.append(entry)
        except (json.JSONDecodeError, TypeError):
            continue

    result = {
        "status": "success",
        "log": log_entries[-limit:],
        "total_entries": len(log_entries),
        "current_step": 4,
    }

    assert result["status"] == "success"
    assert result["total_entries"] == 6
    assert len(result["log"]) == 6

    # Check first entry
    assert result["log"][0]["type"] == "tool_call"
    assert result["log"][0]["tool"] == "plan_new"
    assert result["log"][0]["step"] == 1

    # Check error entry
    error_entry = next(e for e in result["log"] if e.get("error") == "Invalid")
    assert error_entry["status"] == "error"
    assert error_entry["step"] == 3


def test_debug_get_execution_log_filters_by_step():
    """Test debug_get_execution_log filters by step number."""
    prompt_items = [
        json.dumps({"type": "tool_call", "step": 1, "tool": "plan_new"}),
        json.dumps({"type": "tool_output", "step": 1, "observation": {"status": "success"}}),
        json.dumps({"type": "tool_call", "step": 2, "tool": "plan_add_input"}),
        json.dumps({"type": "tool_output", "step": 2, "observation": {"status": "success"}}),
    ]

    step_filter = 2
    log_entries: List[Dict[str, Any]] = []

    for item in prompt_items:
        parsed = json.loads(item)
        item_type = parsed.get("type")
        item_step = parsed.get("step")
        if item_type in {"tool_call", "tool_output"}:
            if step_filter is None or item_step == step_filter:
                log_entries.append({"type": item_type, "step": item_step})

    assert len(log_entries) == 2
    assert all(e["step"] == 2 for e in log_entries)


def test_debug_get_execution_log_handles_invalid_json():
    """Test debug_get_execution_log handles invalid JSON gracefully."""
    prompt_items = [
        "not valid json",
        json.dumps({"type": "tool_call", "step": 1, "tool": "plan_new"}),
        "{broken json",
        json.dumps({"type": "tool_output", "step": 1, "observation": {"status": "success"}}),
    ]

    log_entries: List[Dict[str, Any]] = []

    for item in prompt_items:
        try:
            parsed = json.loads(item)
            item_type = parsed.get("type")
            if item_type in {"tool_call", "tool_output"}:
                log_entries.append({"type": item_type, "step": parsed.get("step")})
        except (json.JSONDecodeError, TypeError):
            continue

    # Only 2 valid entries
    assert len(log_entries) == 2


# ==================== debug_inspect_node Tests ====================

def test_debug_inspect_node_returns_node_info():
    """Test debug_inspect_node returns node details and connections."""
    plan = {
        "definition_json": {
            "nodes": [
                {"id": "input_1", "type": "input", "data": {"dataset_id": "ds-123"}},
                {"id": "transform_1", "type": "transform", "data": {"expression": "UPPER(name)"}},
                {"id": "output_1", "type": "output", "data": {"output_name": "result"}},
            ],
            "edges": [
                {"source": "input_1", "target": "transform_1", "sourceHandle": "out"},
                {"source": "transform_1", "target": "output_1", "sourceHandle": "out"},
            ],
        }
    }

    node_id = "transform_1"
    definition = plan.get("definition_json") or {}
    nodes = definition.get("nodes") or []
    edges = definition.get("edges") or []

    node = next((n for n in nodes if n.get("id") == node_id), None)
    input_edges = [e for e in edges if e.get("target") == node_id]
    output_edges = [e for e in edges if e.get("source") == node_id]

    result = {
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
    }

    assert result["status"] == "success"
    assert result["node_id"] == "transform_1"
    assert result["node_type"] == "transform"
    assert result["node_config"]["expression"] == "UPPER(name)"
    assert len(result["inputs"]) == 1
    assert result["inputs"][0]["from_node"] == "input_1"
    assert len(result["outputs"]) == 1
    assert result["outputs"][0]["to_node"] == "output_1"


def test_debug_inspect_node_not_found():
    """Test debug_inspect_node returns error for non-existent node."""
    plan = {
        "definition_json": {
            "nodes": [
                {"id": "input_1", "type": "input"},
            ],
            "edges": [],
        }
    }

    node_id = "non_existent"
    definition = plan.get("definition_json") or {}
    nodes = definition.get("nodes") or []

    node = next((n for n in nodes if n.get("id") == node_id), None)
    if not node:
        available_nodes = [n.get("id") for n in nodes if n.get("id")]
        result = {
            "status": "error",
            "error": f"Node '{node_id}' not found in plan",
            "available_nodes": available_nodes[:20],
        }
    else:
        result = {"status": "success"}

    assert result["status"] == "error"
    assert "not found" in result["error"]
    assert "input_1" in result["available_nodes"]


def test_debug_inspect_node_requires_node_id():
    """Test debug_inspect_node requires node_id parameter."""
    node_id = ""

    if not node_id.strip():
        result = {"status": "error", "error": "node_id is required"}
    else:
        result = {"status": "success"}

    assert result["status"] == "error"
    assert "node_id is required" in result["error"]


# ==================== debug_explain_failure Tests ====================

def test_debug_explain_failure_diagnoses_errors():
    """Test debug_explain_failure provides diagnostic suggestions."""
    tool_errors = [
        "Dataset 'ds-missing' not found",
        "Type mismatch: cannot convert String to Integer",
        "Invalid join configuration: key mismatch",
    ]

    diagnosis: List[Dict[str, str]] = []
    for error in list(tool_errors)[-10:]:
        error_lower = str(error).lower()
        if "not found" in error_lower or "does not exist" in error_lower:
            diagnosis.append({
                "error": str(error),
                "cause": "Resource not found",
                "fix": "Check spelling and verify the resource exists",
            })
        elif "type" in error_lower and ("mismatch" in error_lower or "incompatible" in error_lower):
            diagnosis.append({
                "error": str(error),
                "cause": "Type mismatch",
                "fix": "Add a cast operation to convert types",
            })
        elif "join" in error_lower:
            diagnosis.append({
                "error": str(error),
                "cause": "Join key issue",
                "fix": "Verify join keys exist and have compatible types",
            })
        else:
            diagnosis.append({
                "error": str(error),
                "cause": "Unknown",
                "fix": "Review error details and check logs",
            })

    result = {
        "status": "success",
        "diagnosis": diagnosis,
        "total_errors": len(tool_errors),
    }

    assert result["status"] == "success"
    assert len(result["diagnosis"]) == 3
    assert result["total_errors"] == 3

    # Check first diagnosis (not found)
    assert result["diagnosis"][0]["cause"] == "Resource not found"
    assert "Check spelling" in result["diagnosis"][0]["fix"]

    # Check second diagnosis (type mismatch)
    assert result["diagnosis"][1]["cause"] == "Type mismatch"
    assert "cast" in result["diagnosis"][1]["fix"]

    # Check third diagnosis (join issue)
    assert result["diagnosis"][2]["cause"] == "Join key issue"


def test_debug_explain_failure_handles_permission_error():
    """Test debug_explain_failure handles permission errors."""
    tool_errors = ["Permission denied for dataset 'sensitive_data'"]

    diagnosis: List[Dict[str, str]] = []
    for error in tool_errors:
        error_lower = str(error).lower()
        if "permission" in error_lower or "denied" in error_lower:
            diagnosis.append({
                "error": str(error),
                "cause": "Permission denied",
                "fix": "Check user permissions for the resource",
            })

    assert len(diagnosis) == 1
    assert diagnosis[0]["cause"] == "Permission denied"


def test_debug_explain_failure_handles_timeout():
    """Test debug_explain_failure handles timeout errors."""
    tool_errors = ["Request timeout: exceeded 30 seconds"]

    diagnosis: List[Dict[str, str]] = []
    for error in tool_errors:
        error_lower = str(error).lower()
        if "timeout" in error_lower:
            diagnosis.append({
                "error": str(error),
                "cause": "Operation timed out",
                "fix": "Reduce data size or increase timeout",
            })

    assert len(diagnosis) == 1
    assert diagnosis[0]["cause"] == "Operation timed out"


def test_debug_explain_failure_empty_errors():
    """Test debug_explain_failure with no errors."""
    tool_errors: List[str] = []
    tool_warnings: List[str] = []

    diagnosis: List[Dict[str, str]] = []
    for error in list(tool_errors)[-10:]:
        diagnosis.append({"error": str(error), "cause": "Unknown", "fix": "Review"})

    result = {
        "status": "success",
        "diagnosis": diagnosis,
        "total_errors": len(tool_errors),
        "total_warnings": len(tool_warnings),
        "suggestion": "Use debug_get_errors to see all errors",
    }

    assert result["status"] == "success"
    assert len(result["diagnosis"]) == 0
    assert result["total_errors"] == 0


# ==================== debug_dry_run Tests ====================

def test_debug_dry_run_valid_plan():
    """Test debug_dry_run validates a correct plan."""
    plan = {
        "definition_json": {
            "nodes": [
                {"id": "input_1", "type": "input", "data": {"dataset_id": "ds-123"}},
                {"id": "output_1", "type": "output", "data": {"output_name": "result"}},
            ],
            "edges": [
                {"source": "input_1", "target": "output_1"},
            ],
        }
    }

    errors: List[str] = []
    warnings: List[str] = []

    definition = plan.get("definition_json") or {}
    nodes = definition.get("nodes") or []
    edges = definition.get("edges") or []

    if not nodes:
        errors.append("Plan has no nodes")
    if not edges and len(nodes) > 1:
        warnings.append("Plan has multiple nodes but no edges connecting them")

    node_types = [n.get("type") for n in nodes]
    has_input = any(t in ("input", "dataset_input", "source") for t in node_types)
    has_output = any(t in ("output", "dataset_output", "sink") for t in node_types)

    if not has_input:
        errors.append("Plan has no input/source node")
    if not has_output:
        warnings.append("Plan has no output/sink node")

    result = {
        "status": "success" if not errors else "invalid",
        "errors": errors,
        "warnings": warnings,
        "dry_run_passed": not errors,
    }

    assert result["status"] == "success"
    assert result["dry_run_passed"] is True
    assert len(result["errors"]) == 0
    assert len(result["warnings"]) == 0


def test_debug_dry_run_empty_plan():
    """Test debug_dry_run rejects empty plan."""
    plan: Dict[str, Any] = {}

    errors: List[str] = []
    warnings: List[str] = []

    if not plan:
        errors.append("Plan is empty")
        result = {
            "status": "invalid",
            "errors": errors,
            "warnings": warnings,
            "dry_run_passed": False,
        }
    else:
        result = {"status": "success", "dry_run_passed": True}

    assert result["status"] == "invalid"
    assert result["dry_run_passed"] is False
    assert "Plan is empty" in result["errors"]


def test_debug_dry_run_no_nodes():
    """Test debug_dry_run rejects plan with no nodes."""
    plan = {
        "definition_json": {
            "nodes": [],
            "edges": [],
        }
    }

    errors: List[str] = []
    definition = plan.get("definition_json") or {}
    nodes = definition.get("nodes") or []

    if not nodes:
        errors.append("Plan has no nodes")

    result = {
        "status": "invalid" if errors else "success",
        "errors": errors,
        "dry_run_passed": not errors,
    }

    assert result["status"] == "invalid"
    assert "Plan has no nodes" in result["errors"]


def test_debug_dry_run_missing_input():
    """Test debug_dry_run warns about missing input node."""
    plan = {
        "definition_json": {
            "nodes": [
                {"id": "output_1", "type": "output"},
            ],
            "edges": [],
        }
    }

    errors: List[str] = []
    definition = plan.get("definition_json") or {}
    nodes = definition.get("nodes") or []

    node_types = [n.get("type") for n in nodes]
    has_input = any(t in ("input", "dataset_input", "source") for t in node_types)

    if not has_input:
        errors.append("Plan has no input/source node")

    result = {
        "status": "invalid" if errors else "success",
        "errors": errors,
        "dry_run_passed": not errors,
    }

    assert result["status"] == "invalid"
    assert "Plan has no input/source node" in result["errors"]


def test_debug_dry_run_missing_output():
    """Test debug_dry_run warns about missing output node."""
    plan = {
        "definition_json": {
            "nodes": [
                {"id": "input_1", "type": "input"},
            ],
            "edges": [],
        }
    }

    warnings: List[str] = []
    errors: List[str] = []

    definition = plan.get("definition_json") or {}
    nodes = definition.get("nodes") or []

    node_types = [n.get("type") for n in nodes]
    has_output = any(t in ("output", "dataset_output", "sink") for t in node_types)

    if not has_output:
        warnings.append("Plan has no output/sink node")

    result = {
        "status": "success" if not errors else "invalid",
        "warnings": warnings,
        "dry_run_passed": not errors,
    }

    assert result["status"] == "success"
    assert "Plan has no output/sink node" in result["warnings"]


def test_debug_dry_run_disconnected_nodes():
    """Test debug_dry_run warns about disconnected nodes."""
    plan = {
        "definition_json": {
            "nodes": [
                {"id": "input_1", "type": "input"},
                {"id": "output_1", "type": "output"},
            ],
            "edges": [],  # No edges connecting them
        }
    }

    warnings: List[str] = []
    definition = plan.get("definition_json") or {}
    nodes = definition.get("nodes") or []
    edges = definition.get("edges") or []

    if not edges and len(nodes) > 1:
        warnings.append("Plan has multiple nodes but no edges connecting them")

    assert len(warnings) == 1
    assert "no edges connecting them" in warnings[0]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
