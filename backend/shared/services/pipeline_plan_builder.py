"""
Deterministic pipeline plan builder helpers.

Goal: let an LLM assemble a PipelinePlan by calling small, constrained tools
instead of emitting a full definition_json in one shot.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from shared.services.pipeline_transform_spec import SUPPORTED_TRANSFORMS


class PipelinePlanBuilderError(ValueError):
    pass


@dataclass(frozen=True)
class PlanMutation:
    plan: Dict[str, Any]
    node_id: Optional[str] = None
    warnings: Tuple[str, ...] = ()


def _ensure_dict(value: Any, *, name: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise PipelinePlanBuilderError(f"{name} must be an object")
    return value


def _ensure_list(value: Any, *, name: str) -> List[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise PipelinePlanBuilderError(f"{name} must be a list")
    return value


def _ensure_str(value: Any, *, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise PipelinePlanBuilderError(f"{name} is required")
    return text


def _ensure_string_list(value: Any, *, name: str) -> List[str]:
    items = _ensure_list(value, name=name)
    out: List[str] = []
    for item in items:
        text = str(item or "").strip()
        if text:
            out.append(text)
    if not out:
        raise PipelinePlanBuilderError(f"{name} must be a non-empty list of strings")
    return out


def _definition(plan: Dict[str, Any]) -> Dict[str, Any]:
    plan_obj = _ensure_dict(plan, name="plan")
    definition = plan_obj.get("definition_json")
    if not isinstance(definition, dict):
        definition = {}
        plan_obj["definition_json"] = definition
    if "nodes" not in definition or not isinstance(definition.get("nodes"), list):
        definition["nodes"] = []
    if "edges" not in definition or not isinstance(definition.get("edges"), list):
        definition["edges"] = []
    return definition


def update_settings(
    plan: Dict[str, Any],
    *,
    set_fields: Optional[Dict[str, Any]] = None,
    unset_fields: Optional[List[str]] = None,
    replace: bool = False,
) -> PlanMutation:
    """
    Patch plan.definition_json.settings (merge by default, replace if requested).

    This is used to control Spark-level behavior (e.g. spark_conf, cast_mode, AQE) without editing per-node metadata.
    """
    definition = _definition(plan)
    existing = definition.get("settings") if isinstance(definition.get("settings"), dict) else {}
    patch = _ensure_dict(set_fields or {}, name="set_fields") if (set_fields is not None) else {}
    next_settings = dict(patch) if replace else dict(existing)
    next_settings.update(patch)
    for key in _ensure_list(unset_fields, name="unset_fields"):
        k = str(key or "").strip()
        if k:
            next_settings.pop(k, None)
    definition["settings"] = next_settings
    return PlanMutation(plan=plan, node_id=None)


def _node_ids(definition: Dict[str, Any]) -> set[str]:
    ids: set[str] = set()
    for node in _ensure_list(definition.get("nodes"), name="definition_json.nodes"):
        if isinstance(node, dict):
            node_id = str(node.get("id") or "").strip()
            if node_id:
                ids.add(node_id)
    return ids


def _find_node(definition: Dict[str, Any], node_id: str) -> Optional[Dict[str, Any]]:
    target = str(node_id or "").strip()
    if not target:
        return None
    for node in _ensure_list(definition.get("nodes"), name="definition_json.nodes"):
        if not isinstance(node, dict):
            continue
        if str(node.get("id") or "").strip() == target:
            return node
    return None


def _unique_node_id(base: str, existing: set[str]) -> str:
    base = str(base or "").strip() or "node"
    candidate = base
    idx = 2
    while candidate in existing:
        candidate = f"{base}_{idx}"
        idx += 1
    return candidate


def new_plan(
    *,
    goal: str,
    db_name: str,
    branch: Optional[str] = None,
    dataset_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    goal_text = _ensure_str(goal, name="goal")
    db = _ensure_str(db_name, name="data_scope.db_name")
    scope: Dict[str, Any] = {"db_name": db}
    if branch:
        scope["branch"] = str(branch).strip() or None
    if dataset_ids:
        scope["dataset_ids"] = [str(item).strip() for item in dataset_ids if str(item).strip()]
    plan: Dict[str, Any] = {
        "goal": goal_text,
        "data_scope": scope,
        "definition_json": {"nodes": [], "edges": []},
        "outputs": [],
        "associations": [],
        "warnings": [],
    }
    return plan


def add_input(
    plan: Dict[str, Any],
    *,
    dataset_id: Optional[str] = None,
    dataset_name: Optional[str] = None,
    dataset_branch: Optional[str] = None,
    read: Optional[Dict[str, Any]] = None,
    node_id: Optional[str] = None,
) -> PlanMutation:
    definition = _definition(plan)
    existing = _node_ids(definition)

    ds_id = str(dataset_id or "").strip() or None
    ds_name = str(dataset_name or "").strip() or None
    if not ds_id and not ds_name:
        raise PipelinePlanBuilderError("dataset_id or dataset_name is required")

    resolved_id = str(node_id or "").strip() or None
    if not resolved_id:
        base = "input"
        if ds_name:
            base = f"input_{ds_name}".replace(" ", "_")
        resolved_id = _unique_node_id(base, existing)
    if resolved_id in existing:
        raise PipelinePlanBuilderError(f"node_id already exists: {resolved_id}")

    metadata: Dict[str, Any] = {}
    if ds_id:
        metadata["datasetId"] = ds_id
    if ds_name:
        metadata["datasetName"] = ds_name
    if dataset_branch:
        metadata["datasetBranch"] = str(dataset_branch).strip() or None
    if isinstance(read, dict) and read:
        # `read` config is interpreted by the Spark pipeline worker to control parsing (csv/json options, schema, etc).
        metadata["read"] = dict(read)

    definition["nodes"].append({"id": resolved_id, "type": "input", "metadata": metadata})
    return PlanMutation(plan=plan, node_id=resolved_id)


def add_external_input(
    plan: Dict[str, Any],
    *,
    read: Dict[str, Any],
    source_name: Optional[str] = None,
    node_id: Optional[str] = None,
) -> PlanMutation:
    """
    Add an input node that is NOT backed by a DatasetRegistry artifact.

    This enables Spark-native sources like JDBC/Kafka or direct file URIs, controlled by `metadata.read`.
    """
    definition = _definition(plan)
    existing = _node_ids(definition)

    read_cfg = _ensure_dict(read, name="read")
    fmt = str(read_cfg.get("format") or read_cfg.get("file_format") or read_cfg.get("fileFormat") or "").strip().lower()
    if not fmt:
        raise PipelinePlanBuilderError("read.format is required for external inputs")

    resolved_id = str(node_id or "").strip() or None
    if not resolved_id:
        resolved_id = _unique_node_id(f"input_{fmt}", existing)
    if resolved_id in existing:
        raise PipelinePlanBuilderError(f"node_id already exists: {resolved_id}")

    metadata: Dict[str, Any] = {"read": dict(read_cfg)}
    if source_name:
        metadata["sourceName"] = str(source_name).strip() or None

    definition["nodes"].append({"id": resolved_id, "type": "input", "metadata": metadata})
    return PlanMutation(plan=plan, node_id=resolved_id)


def configure_input_read(
    plan: Dict[str, Any],
    *,
    node_id: str,
    read: Dict[str, Any],
    replace: bool = False,
) -> PlanMutation:
    """
    Patch input node read configuration.

    The `read` object is passed through to the Spark worker to control parsing/ingestion behavior.
    """
    definition = _definition(plan)
    target = _ensure_str(node_id, name="node_id")
    node = _find_node(definition, target)
    if not node:
        raise PipelinePlanBuilderError(f"node_id missing node: {target}")
    if str(node.get("type") or "").strip().lower() != "input":
        raise PipelinePlanBuilderError("configure_input_read is only valid for input nodes")
    patch = _ensure_dict(read, name="read")
    if not patch:
        raise PipelinePlanBuilderError("read must be a non-empty object")

    existing_meta = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
    existing_read = existing_meta.get("read") if isinstance(existing_meta.get("read"), dict) else {}
    next_read = dict(patch) if replace else dict(existing_read)
    next_read.update(patch)

    next_meta = dict(existing_meta)
    next_meta["read"] = next_read
    node["metadata"] = next_meta
    return PlanMutation(plan=plan, node_id=target)


def add_transform(
    plan: Dict[str, Any],
    *,
    operation: str,
    input_node_ids: List[str],
    metadata: Optional[Dict[str, Any]] = None,
    node_id: Optional[str] = None,
) -> PlanMutation:
    definition = _definition(plan)
    existing = _node_ids(definition)

    op = _ensure_str(operation, name="operation")
    if op not in SUPPORTED_TRANSFORMS:
        raise PipelinePlanBuilderError(f"Unsupported operation: {op}")

    inputs = [str(item).strip() for item in (input_node_ids or []) if str(item).strip()]
    if not inputs:
        raise PipelinePlanBuilderError("input_node_ids is required")
    missing = sorted([node for node in inputs if node not in existing])
    if missing:
        raise PipelinePlanBuilderError(f"input_node_ids missing nodes: {missing}")

    resolved_id = str(node_id or "").strip() or None
    if not resolved_id:
        resolved_id = _unique_node_id(op, existing)
    if resolved_id in existing:
        raise PipelinePlanBuilderError(f"node_id already exists: {resolved_id}")

    meta = dict(metadata or {})
    meta["operation"] = op
    definition["nodes"].append({"id": resolved_id, "type": "transform", "metadata": meta})
    for src in inputs:
        definition["edges"].append({"from": src, "to": resolved_id})
    return PlanMutation(plan=plan, node_id=resolved_id)


def add_join(
    plan: Dict[str, Any],
    *,
    left_node_id: str,
    right_node_id: str,
    left_keys: List[str],
    right_keys: List[str],
    join_type: str = "inner",
    join_hints: Optional[Dict[str, Any]] = None,
    broadcast_left: bool = False,
    broadcast_right: bool = False,
    node_id: Optional[str] = None,
) -> PlanMutation:
    join_type_norm = str(join_type or "inner").strip().lower()
    if join_type_norm == "cross":
        raise PipelinePlanBuilderError("cross join is not allowed (join_type=cross)")
    left = _ensure_str(left_node_id, name="left_node_id")
    right = _ensure_str(right_node_id, name="right_node_id")
    if left == right:
        raise PipelinePlanBuilderError("left_node_id and right_node_id must be different")
    lk = _ensure_string_list(left_keys, name="left_keys")
    rk = _ensure_string_list(right_keys, name="right_keys")
    if len(lk) != len(rk):
        raise PipelinePlanBuilderError("left_keys/right_keys must have the same length")

    hints = None
    if join_hints is not None:
        hints = _ensure_dict(join_hints, name="join_hints")
    return add_transform(
        plan,
        operation="join",
        input_node_ids=[left, right],  # edge order matters for join (LEFT then RIGHT)
        node_id=node_id,
        metadata={
            "joinType": join_type_norm,
            "allowCrossJoin": False,
            "leftKeys": lk,
            "rightKeys": rk,
            **({"joinHints": hints} if hints else {}),
            **({"broadcastLeft": True} if broadcast_left else {}),
            **({"broadcastRight": True} if broadcast_right else {}),
        },
    )


def add_filter(plan: Dict[str, Any], *, input_node_id: str, expression: str, node_id: Optional[str] = None) -> PlanMutation:
    expr = _ensure_str(expression, name="expression")
    src = _ensure_str(input_node_id, name="input_node_id")
    return add_transform(
        plan,
        operation="filter",
        input_node_ids=[src],
        node_id=node_id,
        metadata={"expression": expr},
    )


def add_compute(plan: Dict[str, Any], *, input_node_id: str, expression: str, node_id: Optional[str] = None) -> PlanMutation:
    expr = _ensure_str(expression, name="expression")
    src = _ensure_str(input_node_id, name="input_node_id")
    return add_transform(
        plan,
        operation="compute",
        input_node_ids=[src],
        node_id=node_id,
        metadata={"expression": expr},
    )


def add_compute_column(
    plan: Dict[str, Any],
    *,
    input_node_id: str,
    target_column: str,
    formula: str,
    node_id: Optional[str] = None,
) -> PlanMutation:
    """
    Add a compute transform that writes a single column.

    Prefer this over `add_compute(expression="a = b")` to avoid ambiguity with Spark comparisons.
    """
    src = _ensure_str(input_node_id, name="input_node_id")
    target = _ensure_str(target_column, name="target_column")
    expr = _ensure_str(formula, name="formula")
    return add_transform(
        plan,
        operation="compute",
        input_node_ids=[src],
        node_id=node_id,
        metadata={"targetColumn": target, "formula": expr},
    )


def add_compute_assignments(
    plan: Dict[str, Any],
    *,
    input_node_id: str,
    assignments: List[Dict[str, Any]],
    node_id: Optional[str] = None,
) -> PlanMutation:
    """Add a compute transform that writes multiple columns (assignments)."""
    src = _ensure_str(input_node_id, name="input_node_id")
    items = _ensure_list(assignments, name="assignments")
    normalized: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        col = str(item.get("column") or item.get("target") or item.get("name") or "").strip()
        expr = str(item.get("expression") or item.get("expr") or item.get("formula") or "").strip()
        if col and expr:
            normalized.append({"column": col, "expression": expr})
    if not normalized:
        raise PipelinePlanBuilderError("assignments must include at least one {column,expression}")
    return add_transform(
        plan,
        operation="compute",
        input_node_ids=[src],
        node_id=node_id,
        metadata={"assignments": normalized},
    )


def add_cast(
    plan: Dict[str, Any],
    *,
    input_node_id: str,
    casts: List[Dict[str, Any]],
    node_id: Optional[str] = None,
) -> PlanMutation:
    src = _ensure_str(input_node_id, name="input_node_id")
    items = _ensure_list(casts, name="casts")
    normalized: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        col = str(item.get("column") or "").strip()
        typ = str(item.get("type") or "").strip()
        if not col or not typ:
            continue
        normalized.append({"column": col, "type": typ})
    if not normalized:
        raise PipelinePlanBuilderError("casts must include at least one {column,type}")
    return add_transform(
        plan,
        operation="cast",
        input_node_ids=[src],
        node_id=node_id,
        metadata={"casts": normalized},
    )


def add_rename(
    plan: Dict[str, Any],
    *,
    input_node_id: str,
    rename: Dict[str, str],
    node_id: Optional[str] = None,
) -> PlanMutation:
    src = _ensure_str(input_node_id, name="input_node_id")
    rename_map = _ensure_dict(rename, name="rename")
    cleaned: Dict[str, str] = {}
    for key, value in rename_map.items():
        src_col = str(key or "").strip()
        dst_col = str(value or "").strip()
        if src_col and dst_col:
            cleaned[src_col] = dst_col
    if not cleaned:
        raise PipelinePlanBuilderError("rename must include at least one mapping")
    return add_transform(
        plan,
        operation="rename",
        input_node_ids=[src],
        node_id=node_id,
        metadata={"rename": cleaned},
    )


def add_select(
    plan: Dict[str, Any],
    *,
    input_node_id: str,
    columns: List[str],
    node_id: Optional[str] = None,
) -> PlanMutation:
    src = _ensure_str(input_node_id, name="input_node_id")
    cols = _ensure_string_list(columns, name="columns")
    return add_transform(
        plan,
        operation="select",
        input_node_ids=[src],
        node_id=node_id,
        metadata={"columns": cols},
    )


def add_select_expr(
    plan: Dict[str, Any],
    *,
    input_node_id: str,
    expressions: List[str],
    node_id: Optional[str] = None,
) -> PlanMutation:
    """Add a select transform using Spark SQL selectExpr-style expressions."""
    src = _ensure_str(input_node_id, name="input_node_id")
    exprs = _ensure_string_list(expressions, name="expressions")
    return add_transform(
        plan,
        operation="select",
        input_node_ids=[src],
        node_id=node_id,
        metadata={"expressions": exprs},
    )

def add_drop(
    plan: Dict[str, Any],
    *,
    input_node_id: str,
    columns: List[str],
    node_id: Optional[str] = None,
) -> PlanMutation:
    src = _ensure_str(input_node_id, name="input_node_id")
    cols = _ensure_string_list(columns, name="columns")
    return add_transform(
        plan,
        operation="drop",
        input_node_ids=[src],
        node_id=node_id,
        metadata={"columns": cols},
    )


def add_dedupe(
    plan: Dict[str, Any],
    *,
    input_node_id: str,
    columns: List[str],
    node_id: Optional[str] = None,
) -> PlanMutation:
    src = _ensure_str(input_node_id, name="input_node_id")
    cols = _ensure_string_list(columns, name="columns")
    return add_transform(
        plan,
        operation="dedupe",
        input_node_ids=[src],
        node_id=node_id,
        metadata={"columns": cols},
    )


def add_group_by_expr(
    plan: Dict[str, Any],
    *,
    input_node_id: str,
    group_by: Optional[List[str]] = None,
    aggregate_expressions: Optional[List[Any]] = None,
    operation: str = "groupBy",
    node_id: Optional[str] = None,
) -> PlanMutation:
    """
    Add a groupBy/aggregate node using Spark SQL aggregate expressions.

    aggregate_expressions items:
    - string: "approx_percentile(price, 0.5) as p50"
    - object: {"expr": "sum(price)", "alias": "total_price"}
    """
    src = _ensure_str(input_node_id, name="input_node_id")
    op = _ensure_str(operation, name="operation")
    if op not in {"groupBy", "aggregate"}:
        raise PipelinePlanBuilderError("operation must be groupBy or aggregate")
    group_cols = []
    if group_by:
        group_cols = [str(item).strip() for item in group_by if str(item).strip()]
    expr_items = _ensure_list(aggregate_expressions, name="aggregate_expressions")
    if not expr_items:
        raise PipelinePlanBuilderError("aggregate_expressions is required")
    normalized: List[Any] = []
    for item in expr_items:
        if isinstance(item, str) and item.strip():
            normalized.append(item.strip())
        elif isinstance(item, dict) and (item.get("expr") or item.get("expression")):
            normalized.append(dict(item))
    if not normalized:
        raise PipelinePlanBuilderError("aggregate_expressions must include at least one expression")
    return add_transform(
        plan,
        operation=op,
        input_node_ids=[src],
        node_id=node_id,
        metadata={"groupBy": group_cols, "aggregateExpressions": normalized},
    )


def add_window_expr(
    plan: Dict[str, Any],
    *,
    input_node_id: str,
    expressions: List[Dict[str, Any]],
    node_id: Optional[str] = None,
) -> PlanMutation:
    """Add a window transform that computes one or more Spark SQL window expressions."""
    src = _ensure_str(input_node_id, name="input_node_id")
    items = _ensure_list(expressions, name="expressions")
    normalized: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        col = str(item.get("column") or item.get("name") or "").strip()
        expr = str(item.get("expr") or item.get("expression") or "").strip()
        if col and expr:
            normalized.append({"column": col, "expr": expr})
    if not normalized:
        raise PipelinePlanBuilderError("expressions must include at least one {column,expr}")
    return add_transform(
        plan,
        operation="window",
        input_node_ids=[src],
        node_id=node_id,
        metadata={"window": {"expressions": normalized}},
    )


def add_normalize(
    plan: Dict[str, Any],
    *,
    input_node_id: str,
    columns: List[str],
    trim: bool = True,
    empty_to_null: bool = True,
    whitespace_to_null: bool = True,
    lowercase: bool = False,
    uppercase: bool = False,
    node_id: Optional[str] = None,
) -> PlanMutation:
    src = _ensure_str(input_node_id, name="input_node_id")
    cols = _ensure_string_list(columns, name="columns")
    return add_transform(
        plan,
        operation="normalize",
        input_node_ids=[src],
        node_id=node_id,
        metadata={
            "columns": cols,
            "trim": bool(trim),
            "emptyToNull": bool(empty_to_null),
            "whitespaceToNull": bool(whitespace_to_null),
            "lowercase": bool(lowercase),
            "uppercase": bool(uppercase),
        },
    )


def add_regex_replace(
    plan: Dict[str, Any],
    *,
    input_node_id: str,
    rules: List[Dict[str, Any]],
    node_id: Optional[str] = None,
) -> PlanMutation:
    src = _ensure_str(input_node_id, name="input_node_id")
    raw = _ensure_list(rules, name="rules")
    normalized: List[Dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        column = str(item.get("column") or "").strip()
        pattern = str(item.get("pattern") or "").strip()
        if not column or not pattern:
            continue
        normalized.append(
            {
                "column": column,
                "pattern": pattern,
                "replacement": str(item.get("replacement") or ""),
                "flags": item.get("flags"),
            }
        )
    if not normalized:
        raise PipelinePlanBuilderError("rules must include at least one {column,pattern}")
    return add_transform(
        plan,
        operation="regexReplace",
        input_node_ids=[src],
        node_id=node_id,
        metadata={"rules": normalized},
    )


def add_output(
    plan: Dict[str, Any],
    *,
    input_node_id: str,
    output_name: str,
    output_kind: str = "unknown",
    node_id: Optional[str] = None,
    output_metadata: Optional[Dict[str, Any]] = None,
) -> PlanMutation:
    definition = _definition(plan)
    existing = _node_ids(definition)

    src = _ensure_str(input_node_id, name="input_node_id")
    if src not in existing:
        raise PipelinePlanBuilderError(f"input_node_id missing node: {src}")

    name = _ensure_str(output_name, name="output_name")
    kind = str(output_kind or "unknown").strip().lower() or "unknown"
    if kind not in {"unknown", "object", "link"}:
        raise PipelinePlanBuilderError("output_kind must be one of: unknown|object|link")

    resolved_id = str(node_id or "").strip() or None
    if not resolved_id:
        resolved_id = _unique_node_id(f"output_{name}".replace(" ", "_"), existing)
    if resolved_id in existing:
        raise PipelinePlanBuilderError(f"node_id already exists: {resolved_id}")

    metadata = dict(output_metadata or {})
    metadata["outputName"] = name
    definition["nodes"].append({"id": resolved_id, "type": "output", "metadata": metadata})
    definition["edges"].append({"from": src, "to": resolved_id})

    outputs = _ensure_list(plan.get("outputs"), name="outputs")
    outputs.append({"output_name": name, "output_kind": kind})
    plan["outputs"] = outputs
    return PlanMutation(plan=plan, node_id=resolved_id)


def validate_structure(plan: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """
    Lightweight structural validation for plan.definition_json.

    This intentionally does not resolve dataset schemas (that belongs in full validation/preflight).
    """
    errors: List[str] = []
    warnings: List[str] = []
    try:
        definition = _definition(plan)
    except PipelinePlanBuilderError as exc:
        return [str(exc)], []

    nodes = _ensure_list(definition.get("nodes"), name="definition_json.nodes")
    edges = _ensure_list(definition.get("edges"), name="definition_json.edges")
    node_by_id: Dict[str, Dict[str, Any]] = {}
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("id") or "").strip()
        if not node_id:
            errors.append("node missing id")
            continue
        if node_id in node_by_id:
            errors.append(f"duplicate node id: {node_id}")
            continue
        node_by_id[node_id] = node

    for edge in edges:
        if not isinstance(edge, dict):
            continue
        src = str(edge.get("from") or "").strip()
        dst = str(edge.get("to") or "").strip()
        if not src or not dst:
            errors.append("edge missing from/to")
            continue
        if src not in node_by_id or dst not in node_by_id:
            errors.append(f"edge references missing node: {src}->{dst}")

    incoming: Dict[str, List[str]] = {}
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        src = str(edge.get("from") or "").strip()
        dst = str(edge.get("to") or "").strip()
        if not src or not dst:
            continue
        incoming.setdefault(dst, []).append(src)

    for node_id, node in node_by_id.items():
        node_type = str(node.get("type") or "").strip().lower() or "transform"
        if node_type == "transform":
            metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
            op = str(metadata.get("operation") or "").strip()
            if not op:
                if len(incoming.get(node_id, [])) >= 2:
                    errors.append(f"transform node {node_id} has multiple inputs but no operation")
                continue
            if op not in SUPPORTED_TRANSFORMS:
                errors.append(f"Unsupported operation '{op}' on node {node_id}")
                continue
            if op in {"filter", "compute"} and not str(metadata.get("expression") or "").strip():
                errors.append(f"{op} missing expression on node {node_id}")
            if op in {"select", "drop", "sort", "dedupe", "explode"}:
                cols = metadata.get("columns") or []
                if not cols:
                    errors.append(f"{op} missing columns on node {node_id}")
            if op == "rename":
                rename_map = metadata.get("rename") or {}
                if not rename_map:
                    errors.append(f"rename missing mapping on node {node_id}")
            if op == "cast":
                casts = metadata.get("casts") or []
                if not casts:
                    errors.append(f"cast missing columns on node {node_id}")
            if op in {"groupBy", "aggregate"}:
                aggregates = metadata.get("aggregates") or []
                if not isinstance(aggregates, list) or not any(
                    isinstance(item, dict) and item.get("column") and item.get("op") for item in aggregates
                ):
                    errors.append(f"{op} missing aggregates on node {node_id}")
            if op == "join":
                inc = incoming.get(node_id, [])
                if len(inc) < 2:
                    errors.append(f"join requires two inputs on node {node_id}")
                allow_cross = bool(metadata.get("allowCrossJoin") or metadata.get("allow_cross_join") or False)
                left_keys = metadata.get("leftKeys") or metadata.get("left_keys") or []
                right_keys = metadata.get("rightKeys") or metadata.get("right_keys") or []
                if not allow_cross:
                    if not left_keys or not right_keys:
                        errors.append(
                            "join requires leftKeys/rightKeys (or allowCrossJoin=true) on node "
                            f"{node_id}"
                        )
                    elif len(left_keys) != len(right_keys):
                        errors.append(f"join leftKeys/rightKeys length mismatch on node {node_id}")
                if allow_cross:
                    warnings.append(f"join allowCrossJoin=true on node {node_id}")
            if op == "union":
                if len(incoming.get(node_id, [])) < 2:
                    errors.append(f"union requires two inputs on node {node_id}")
    return errors, warnings


def add_edge(
    plan: Dict[str, Any],
    *,
    from_node_id: str,
    to_node_id: str,
) -> PlanMutation:
    """
    Add a graph edge (idempotent).

    Note: incoming edge order can affect join semantics (LEFT/RIGHT).
    """
    definition = _definition(plan)
    existing = _node_ids(definition)
    src = _ensure_str(from_node_id, name="from_node_id")
    dst = _ensure_str(to_node_id, name="to_node_id")
    if src not in existing:
        raise PipelinePlanBuilderError(f"from_node_id missing node: {src}")
    if dst not in existing:
        raise PipelinePlanBuilderError(f"to_node_id missing node: {dst}")
    edges = _ensure_list(definition.get("edges"), name="definition_json.edges")
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        if str(edge.get("from") or "").strip() == src and str(edge.get("to") or "").strip() == dst:
            return PlanMutation(plan=plan)
    edges.append({"from": src, "to": dst})
    definition["edges"] = edges
    return PlanMutation(plan=plan)


def delete_edge(
    plan: Dict[str, Any],
    *,
    from_node_id: str,
    to_node_id: str,
) -> PlanMutation:
    """Delete all matching edges from->to (no-op if not found)."""
    definition = _definition(plan)
    src = _ensure_str(from_node_id, name="from_node_id")
    dst = _ensure_str(to_node_id, name="to_node_id")
    edges = _ensure_list(definition.get("edges"), name="definition_json.edges")
    filtered: List[Any] = []
    removed = 0
    for edge in edges:
        if not isinstance(edge, dict):
            filtered.append(edge)
            continue
        if str(edge.get("from") or "").strip() == src and str(edge.get("to") or "").strip() == dst:
            removed += 1
            continue
        filtered.append(edge)
    definition["edges"] = filtered
    warnings: Tuple[str, ...] = ()
    if removed == 0:
        warnings = (f"edge not found: {src}->{dst}",)
    return PlanMutation(plan=plan, warnings=warnings)


def set_node_inputs(
    plan: Dict[str, Any],
    *,
    node_id: str,
    input_node_ids: List[str],
) -> PlanMutation:
    """
    Replace all incoming edges to node_id with input_node_ids (in order).

    This is the safest way to repair join input order (LEFT then RIGHT).
    """
    definition = _definition(plan)
    existing = _node_ids(definition)
    target = _ensure_str(node_id, name="node_id")
    if target not in existing:
        raise PipelinePlanBuilderError(f"node_id missing node: {target}")
    inputs = [str(item).strip() for item in (input_node_ids or []) if str(item).strip()]
    if not inputs:
        raise PipelinePlanBuilderError("input_node_ids is required")
    missing = sorted([src for src in inputs if src not in existing])
    if missing:
        raise PipelinePlanBuilderError(f"input_node_ids missing nodes: {missing}")

    edges = _ensure_list(definition.get("edges"), name="definition_json.edges")
    kept: List[Any] = []
    for edge in edges:
        if not isinstance(edge, dict):
            kept.append(edge)
            continue
        if str(edge.get("to") or "").strip() == target:
            continue
        kept.append(edge)
    for src in inputs:
        kept.append({"from": src, "to": target})
    definition["edges"] = kept
    return PlanMutation(plan=plan, node_id=target)


def update_node_metadata(
    plan: Dict[str, Any],
    *,
    node_id: str,
    set_fields: Optional[Dict[str, Any]] = None,
    unset_fields: Optional[List[str]] = None,
    replace: bool = False,
) -> PlanMutation:
    """Patch node.metadata (merge by default, replace if requested)."""
    definition = _definition(plan)
    target = _ensure_str(node_id, name="node_id")
    node = _find_node(definition, target)
    if not node:
        raise PipelinePlanBuilderError(f"node_id missing node: {target}")

    existing_meta = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
    patch = _ensure_dict(set_fields or {}, name="set_fields") if (set_fields is not None) else {}
    next_meta = dict(patch) if replace else dict(existing_meta)
    next_meta.update(patch)
    for key in _ensure_list(unset_fields, name="unset_fields"):
        k = str(key or "").strip()
        if k:
            next_meta.pop(k, None)

    # Preserve the same transform operation guardrails as add_transform/add_join.
    node_type = str(node.get("type") or "").strip().lower()
    if node_type == "transform":
        operation = str(next_meta.get("operation") or "").strip()
        if operation and operation not in SUPPORTED_TRANSFORMS:
            raise PipelinePlanBuilderError(f"Unsupported operation: {operation}")
        if operation == "join":
            join_type = str(next_meta.get("joinType") or "inner").strip().lower()
            if join_type == "cross":
                raise PipelinePlanBuilderError("cross join is not allowed (joinType=cross)")
            allow_cross = bool(next_meta.get("allowCrossJoin") or next_meta.get("allow_cross_join") or False)
            if allow_cross:
                raise PipelinePlanBuilderError("cross join is not allowed (allowCrossJoin=true)")

    if str(node.get("type") or "").strip().lower() == "output":
        output_name = next_meta.get("outputName")
        if output_name is not None and not str(output_name or "").strip():
            raise PipelinePlanBuilderError("output.metadata.outputName cannot be empty")

    node["metadata"] = next_meta
    return PlanMutation(plan=plan, node_id=target)


def delete_node(plan: Dict[str, Any], *, node_id: str) -> PlanMutation:
    """Delete a node and any incident edges; also removes outputs[] entry for output nodes."""
    definition = _definition(plan)
    target = _ensure_str(node_id, name="node_id")
    nodes = _ensure_list(definition.get("nodes"), name="definition_json.nodes")
    node = _find_node(definition, target)
    if not node:
        raise PipelinePlanBuilderError(f"node_id missing node: {target}")

    definition["nodes"] = [n for n in nodes if not (isinstance(n, dict) and str(n.get("id") or "").strip() == target)]

    edges = _ensure_list(definition.get("edges"), name="definition_json.edges")
    definition["edges"] = [
        e
        for e in edges
        if not (
            isinstance(e, dict)
            and (str(e.get("from") or "").strip() == target or str(e.get("to") or "").strip() == target)
        )
    ]

    if str(node.get("type") or "").strip().lower() == "output":
        metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
        output_name = str(metadata.get("outputName") or "").strip()
        if output_name:
            outputs = _ensure_list(plan.get("outputs"), name="outputs")
            plan["outputs"] = [
                item
                for item in outputs
                if not (isinstance(item, dict) and str(item.get("output_name") or "").strip() == output_name)
            ]
    return PlanMutation(plan=plan, node_id=target)


def update_output(
    plan: Dict[str, Any],
    *,
    output_name: str,
    set_fields: Optional[Dict[str, Any]] = None,
    unset_fields: Optional[List[str]] = None,
    replace: bool = False,
) -> PlanMutation:
    """
    Patch an outputs[] entry by output_name; keeps output node metadata.outputName in sync if renamed.
    """
    name = _ensure_str(output_name, name="output_name")
    outputs = _ensure_list(plan.get("outputs"), name="outputs")
    idx = None
    for i, item in enumerate(outputs):
        if not isinstance(item, dict):
            continue
        if str(item.get("output_name") or "").strip() == name:
            idx = i
            break
    if idx is None:
        raise PipelinePlanBuilderError(f"outputs entry not found: {name}")

    patch = _ensure_dict(set_fields or {}, name="set_fields") if (set_fields is not None) else {}
    next_item = dict(patch) if replace else dict(outputs[idx])
    next_item.update(patch)
    for key in _ensure_list(unset_fields, name="unset_fields"):
        k = str(key or "").strip()
        if k:
            next_item.pop(k, None)
    if not str(next_item.get("output_name") or "").strip():
        raise PipelinePlanBuilderError("outputs[].output_name cannot be empty")
    outputs[idx] = next_item
    plan["outputs"] = outputs

    new_name = str(next_item.get("output_name") or "").strip()
    if new_name != name:
        definition = _definition(plan)
        for node in _ensure_list(definition.get("nodes"), name="definition_json.nodes"):
            if not isinstance(node, dict):
                continue
            if str(node.get("type") or "").strip().lower() != "output":
                continue
            metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
            if str(metadata.get("outputName") or "").strip() != name:
                continue
            updated_meta = dict(metadata)
            updated_meta["outputName"] = new_name
            node["metadata"] = updated_meta
    return PlanMutation(plan=plan)
