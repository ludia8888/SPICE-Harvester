"""Pipeline Builder definition augmentation helpers.

These helpers mutate pipeline definition JSON graphs by injecting additional
transforms used by the canonical contract (sys columns + FK checks) and by
improving runtime behavior (automatic casts after inputs).

They are shared across BFF and pipeline execution services to avoid duplicated
graph mutation logic.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from shared.services.pipeline.pipeline_dataset_utils import normalize_dataset_selection, resolve_dataset_version
from shared.services.pipeline.pipeline_definition_utils import split_expectation_columns
from shared.services.pipeline.pipeline_graph_utils import normalize_edges, unique_node_id
from shared.services.pipeline.pipeline_schema_casts import extract_schema_casts
from shared.services.pipeline.pipeline_transform_spec import normalize_operation
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.utils.time_utils import utcnow

_CANONICAL_OUTPUT_PREFIX = "canonical_"
_SYS_COLUMN_SPECS = [
    {"name": "_sys_source", "type": "xsd:string"},
    {"name": "_sys_ingested_at", "type": "xsd:dateTime"},
    {"name": "_sys_record_hash", "type": "xsd:string"},
    {"name": "_sys_is_deleted", "type": "xsd:boolean"},
    {"name": "_sys_valid_from", "type": "xsd:dateTime"},
    {"name": "_sys_valid_to", "type": "xsd:dateTime"},
]
_SYS_NOT_NULL_COLUMNS = {
    "_sys_source",
    "_sys_ingested_at",
    "_sys_record_hash",
    "_sys_is_deleted",
    "_sys_valid_from",
}


def _clone_definition_graph(
    definition_json: Dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]], set[str]]:
    nodes_raw = definition_json.get("nodes")
    edges_raw = definition_json.get("edges")
    if not isinstance(edges_raw, list):
        edges_raw = []
    nodes = [dict(node) for node in nodes_raw if isinstance(node, dict)] if isinstance(nodes_raw, list) else []
    edges = normalize_edges(edges_raw)
    node_by_id = {str(node.get("id")): node for node in nodes if node.get("id")}
    existing_ids = set(node_by_id.keys())
    return nodes, edges, node_by_id, existing_ids


def _output_name_for_node(node_id: str, node: Dict[str, Any]) -> str:
    metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
    return str(metadata.get("outputName") or metadata.get("datasetName") or node.get("title") or node_id or "").strip()


def _is_canonical_output(
    node_id: str,
    node: Dict[str, Any],
    canonical_output_names: Optional[set[str]] = None,
) -> bool:
    name = _output_name_for_node(node_id, node)
    if not name:
        return False
    if canonical_output_names and name in canonical_output_names:
        return True
    return name.startswith(_CANONICAL_OUTPUT_PREFIX)


def _sql_ident(name: str) -> str:
    return f"`{str(name).replace('`', '``')}`"


def _concat_expr(columns: List[str], *, null_if_any: bool = False) -> str:
    col_exprs = [_sql_ident(col) for col in columns]
    joined = f"concat_ws('||', {', '.join(col_exprs)})"
    if not null_if_any or not col_exprs:
        return joined
    null_checks = " OR ".join(f"{expr} IS NULL" for expr in col_exprs)
    return f"case when {null_checks} then null else {joined} end"


def _normalize_fk_columns(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return split_expectation_columns(str(value or ""))


def _merge_schema_checks(
    existing: Any,
    additions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in existing if isinstance(existing, list) else []:
        if not isinstance(item, dict):
            continue
        rule = str(item.get("rule") or "").strip().lower()
        column = str(item.get("column") or "").strip()
        if rule and column:
            seen.add((rule, column))
        output.append(item)
    for item in additions:
        rule = str(item.get("rule") or "").strip().lower()
        column = str(item.get("column") or "").strip()
        if rule and column and (rule, column) in seen:
            continue
        if rule and column:
            seen.add((rule, column))
        output.append(item)
    return output


def _sys_compute_expressions(output_name: str) -> List[Dict[str, str]]:
    safe_name = str(output_name or "").replace("'", "''")
    now_value = utcnow().strftime("%Y-%m-%d %H:%M:%S")
    valid_to_value = "9999-12-31 23:59:59"
    return [
        {"column": "_sys_source", "expr": f"'{safe_name}'"},
        {"column": "_sys_ingested_at", "expr": f"to_timestamp('{now_value}')"},
        {"column": "_sys_is_deleted", "expr": "false"},
        {"column": "_sys_valid_from", "expr": f"to_timestamp('{now_value}')"},
        {"column": "_sys_valid_to", "expr": f"to_timestamp('{valid_to_value}')"},
        {"column": "_sys_record_hash", "expr": "sha2(to_json(struct(*)), 256)"},
    ]


def _refresh_sys_compute_nodes(
    *,
    node_by_id: Dict[str, Dict[str, Any]],
    output_id: str,
    output_name: str,
) -> bool:
    updated = False
    for spec in _sys_compute_expressions(output_name):
        node_id = f"sys_{spec['column'].lstrip('_')}_{output_id}"
        node = node_by_id.get(node_id)
        if not node:
            continue
        metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
        expression = f"{spec['column']} = {spec['expr']}"
        if normalize_operation(metadata.get("operation")) != "compute" or metadata.get("expression") != expression:
            metadata = dict(metadata)
            metadata["operation"] = "compute"
            metadata["expression"] = expression
            node["metadata"] = metadata
            node_by_id[node_id] = node
            updated = True
    return updated


def _inject_sys_columns_chain(
    *,
    edges: List[Dict[str, Any]],
    nodes: List[Dict[str, Any]],
    node_by_id: Dict[str, Dict[str, Any]],
    existing_ids: set[str],
    source_id: str,
    output_id: str,
    output_name: str,
) -> Optional[str]:
    if not source_id:
        return None
    edges[:] = [edge for edge in edges if edge.get("to") != output_id]
    prev_id = source_id
    for spec in _sys_compute_expressions(output_name):
        node_id = unique_node_id(f"sys_{spec['column'].lstrip('_')}_{output_id}", existing_ids, start_index=1)
        existing_ids.add(node_id)
        node = {
            "id": node_id,
            "type": "transform",
            "metadata": {
                "operation": "compute",
                "expression": f"{spec['column']} = {spec['expr']}",
            },
        }
        nodes.append(node)
        node_by_id[node_id] = node
        edges.append({"from": prev_id, "to": node_id})
        prev_id = node_id
    edges.append({"from": prev_id, "to": output_id})
    return prev_id


def _inject_fk_join_check(
    *,
    edges: List[Dict[str, Any]],
    nodes: List[Dict[str, Any]],
    node_by_id: Dict[str, Dict[str, Any]],
    existing_ids: set[str],
    left_node_id: str,
    output_id: str,
    fk_index: int,
    fk_spec: Dict[str, Any],
    default_branch: Optional[str],
) -> Optional[str]:
    columns = _normalize_fk_columns(fk_spec.get("columns") or fk_spec.get("column"))
    reference = fk_spec.get("reference") if isinstance(fk_spec.get("reference"), dict) else {}
    ref_columns = _normalize_fk_columns(
        reference.get("columns")
        or reference.get("column")
        or fk_spec.get("ref_columns")
        or fk_spec.get("ref_column")
    )
    if not ref_columns and columns:
        ref_columns = list(columns)
    if not columns or not ref_columns:
        return None
    if len(columns) != len(ref_columns):
        return None
    allow_nulls = fk_spec.get("allow_nulls") if "allow_nulls" in fk_spec else fk_spec.get("allowNulls")
    allow_nulls = True if allow_nulls is None else bool(allow_nulls)
    ref_branch = reference.get("branch") or fk_spec.get("branch") or default_branch or "main"
    dataset_id = (
        reference.get("datasetId")
        or reference.get("dataset_id")
        or fk_spec.get("datasetId")
        or fk_spec.get("dataset_id")
    )
    dataset_name = (
        reference.get("datasetName")
        or reference.get("dataset_name")
        or fk_spec.get("datasetName")
        or fk_spec.get("dataset_name")
    )

    left_key = columns[0]
    left_key_synthetic = False
    current_left = left_node_id
    if len(columns) > 1:
        left_key = f"__fk_key_{fk_index}"
        left_key_synthetic = True
        expr = _concat_expr(columns, null_if_any=True)
        left_compute_id = unique_node_id(f"fk_key_left_{fk_index}", existing_ids, start_index=1)
        existing_ids.add(left_compute_id)
        left_compute = {
            "id": left_compute_id,
            "type": "transform",
            "metadata": {
                "operation": "compute",
                "expression": f"{left_key} = {expr}",
            },
        }
        nodes.append(left_compute)
        node_by_id[left_compute_id] = left_compute
        edges.append({"from": current_left, "to": left_compute_id})
        current_left = left_compute_id

    ref_input_id = unique_node_id(f"fk_ref_input_{fk_index}", existing_ids, start_index=1)
    existing_ids.add(ref_input_id)
    ref_meta: Dict[str, Any] = {}
    if dataset_id:
        ref_meta["datasetId"] = dataset_id
    if dataset_name:
        ref_meta["datasetName"] = dataset_name
    if ref_branch:
        ref_meta["datasetBranch"] = ref_branch
    ref_input = {"id": ref_input_id, "type": "input", "metadata": ref_meta}
    nodes.append(ref_input)
    node_by_id[ref_input_id] = ref_input

    ref_key = f"__fk_ref_key_{fk_index}"
    ref_current = ref_input_id
    if len(ref_columns) > 1:
        expr = _concat_expr(ref_columns, null_if_any=True)
        ref_compute_id = unique_node_id(f"fk_ref_key_{fk_index}", existing_ids, start_index=1)
        existing_ids.add(ref_compute_id)
        ref_compute = {
            "id": ref_compute_id,
            "type": "transform",
            "metadata": {
                "operation": "compute",
                "expression": f"{ref_key} = {expr}",
            },
        }
        nodes.append(ref_compute)
        node_by_id[ref_compute_id] = ref_compute
        edges.append({"from": ref_current, "to": ref_compute_id})
        ref_current = ref_compute_id
        ref_select_id = unique_node_id(f"fk_ref_select_{fk_index}", existing_ids, start_index=1)
        existing_ids.add(ref_select_id)
        ref_select = {
            "id": ref_select_id,
            "type": "transform",
            "metadata": {"operation": "select", "columns": [ref_key]},
        }
        nodes.append(ref_select)
        node_by_id[ref_select_id] = ref_select
        edges.append({"from": ref_current, "to": ref_select_id})
        ref_current = ref_select_id
    else:
        ref_select_id = unique_node_id(f"fk_ref_select_{fk_index}", existing_ids, start_index=1)
        existing_ids.add(ref_select_id)
        ref_select = {
            "id": ref_select_id,
            "type": "transform",
            "metadata": {"operation": "select", "columns": [ref_columns[0]]},
        }
        nodes.append(ref_select)
        node_by_id[ref_select_id] = ref_select
        edges.append({"from": ref_current, "to": ref_select_id})
        ref_current = ref_select_id
        ref_rename_id = unique_node_id(f"fk_ref_rename_{fk_index}", existing_ids, start_index=1)
        existing_ids.add(ref_rename_id)
        ref_rename = {
            "id": ref_rename_id,
            "type": "transform",
            "metadata": {"operation": "rename", "rename": {ref_columns[0]: ref_key}},
        }
        nodes.append(ref_rename)
        node_by_id[ref_rename_id] = ref_rename
        edges.append({"from": ref_current, "to": ref_rename_id})
        ref_current = ref_rename_id

    ref_dedupe_id = unique_node_id(f"fk_ref_dedupe_{fk_index}", existing_ids, start_index=1)
    existing_ids.add(ref_dedupe_id)
    ref_dedupe = {
        "id": ref_dedupe_id,
        "type": "transform",
        "metadata": {"operation": "dedupe", "columns": [ref_key]},
    }
    nodes.append(ref_dedupe)
    node_by_id[ref_dedupe_id] = ref_dedupe
    edges.append({"from": ref_current, "to": ref_dedupe_id})
    ref_current = ref_dedupe_id

    marker_col = f"__fk_ref_hit_{fk_index}"
    ref_marker_id = unique_node_id(f"fk_ref_marker_{fk_index}", existing_ids, start_index=1)
    existing_ids.add(ref_marker_id)
    ref_marker = {
        "id": ref_marker_id,
        "type": "transform",
        "metadata": {
            "operation": "compute",
            "expression": f"{marker_col} = 1",
        },
    }
    nodes.append(ref_marker)
    node_by_id[ref_marker_id] = ref_marker
    edges.append({"from": ref_current, "to": ref_marker_id})
    ref_current = ref_marker_id

    edges[:] = [edge for edge in edges if edge.get("to") != output_id]
    join_id = unique_node_id(f"fk_join_{fk_index}", existing_ids, start_index=1)
    existing_ids.add(join_id)
    join_node = {
        "id": join_id,
        "type": "transform",
        "metadata": {
            "operation": "join",
            "joinType": "left",
            "leftKey": left_key,
            "rightKey": ref_key,
        },
    }
    nodes.append(join_node)
    node_by_id[join_id] = join_node
    edges.append({"from": current_left, "to": join_id})
    edges.append({"from": ref_current, "to": join_id})

    missing_col = f"__fk_missing_{fk_index}"
    left_expr = _sql_ident(left_key)
    marker_expr = _sql_ident(marker_col)
    if allow_nulls:
        missing_expr = (
            f"{missing_col} = case when {left_expr} is null then 0 "
            f"when {marker_expr} is null then 1 else 0 end"
        )
    else:
        missing_expr = f"{missing_col} = case when {marker_expr} is null then 1 else 0 end"
    missing_id = unique_node_id(f"fk_missing_{fk_index}", existing_ids, start_index=1)
    existing_ids.add(missing_id)
    missing_node = {
        "id": missing_id,
        "type": "transform",
        "metadata": {
            "operation": "compute",
            "expression": missing_expr,
            "schemaChecks": [{"rule": "max", "column": missing_col, "value": 0}],
        },
    }
    nodes.append(missing_node)
    node_by_id[missing_id] = missing_node
    edges.append({"from": join_id, "to": missing_id})

    drop_columns = [missing_col, marker_col, ref_key]
    if left_key_synthetic:
        drop_columns.append(left_key)
    drop_id = unique_node_id(f"fk_drop_{fk_index}", existing_ids, start_index=1)
    existing_ids.add(drop_id)
    drop_node = {
        "id": drop_id,
        "type": "transform",
        "metadata": {"operation": "drop", "columns": drop_columns},
    }
    nodes.append(drop_node)
    node_by_id[drop_id] = drop_node
    edges.append({"from": missing_id, "to": drop_id})
    edges.append({"from": drop_id, "to": output_id})
    return drop_id


def augment_definition_with_canonical_contract(
    *,
    definition_json: Dict[str, Any],
    branch: Optional[str],
    canonical_output_names: Optional[set[str]] = None,
) -> Dict[str, Any]:
    nodes_raw = definition_json.get("nodes")
    if not isinstance(nodes_raw, list) or not nodes_raw:
        return definition_json

    nodes, edges, node_by_id, existing_ids = _clone_definition_graph(definition_json)
    updated = False

    for node_id, node in list(node_by_id.items()):
        if node.get("type") != "output":
            continue
        if not _is_canonical_output(node_id, node, canonical_output_names):
            continue
        metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
        already_applied = bool(metadata.get("_canonical_contract_applied"))
        output_name = _output_name_for_node(node_id, node)
        if not already_applied and not metadata.get("pkSemantics") and not metadata.get("pk_semantics"):
            metadata = dict(metadata)
            metadata["pkSemantics"] = "snapshot"
        checks: List[Dict[str, Any]] = []
        if not already_applied:
            for spec in _SYS_COLUMN_SPECS:
                checks.append({"rule": "required", "column": spec["name"]})
                checks.append({"rule": "type", "column": spec["name"], "value": spec["type"]})
                if spec["name"] in _SYS_NOT_NULL_COLUMNS:
                    checks.append({"rule": "not_null", "column": spec["name"]})
            metadata["schemaChecks"] = _merge_schema_checks(metadata.get("schemaChecks"), checks)
            metadata["_canonical_contract_applied"] = True
            node["metadata"] = metadata
            node_by_id[node_id] = node

        incoming_edges = [edge for edge in edges if edge.get("to") == node_id]
        refreshed = _refresh_sys_compute_nodes(
            node_by_id=node_by_id,
            output_id=node_id,
            output_name=output_name,
        )
        if refreshed:
            updated = True
        if not incoming_edges:
            if refreshed:
                updated = True
            continue
        source_id = str(incoming_edges[0].get("from") or "").strip()
        if not source_id:
            continue
        sys_ids = {f"sys_{spec['name'].lstrip('_')}_{node_id}" for spec in _SYS_COLUMN_SPECS}
        missing_sys = any(sys_id not in node_by_id for sys_id in sys_ids)
        if already_applied and not missing_sys:
            if refreshed:
                updated = True
            continue
        tail_id = _inject_sys_columns_chain(
            edges=edges,
            nodes=nodes,
            node_by_id=node_by_id,
            existing_ids=existing_ids,
            source_id=source_id,
            output_id=node_id,
            output_name=output_name,
        )
        if tail_id is None:
            continue
        foreign_keys = metadata.get("foreignKeys") or metadata.get("foreign_keys") or []
        if isinstance(foreign_keys, list):
            for idx, fk_spec in enumerate(foreign_keys):
                if not isinstance(fk_spec, dict):
                    continue
                injected = _inject_fk_join_check(
                    edges=edges,
                    nodes=nodes,
                    node_by_id=node_by_id,
                    existing_ids=existing_ids,
                    left_node_id=tail_id,
                    output_id=node_id,
                    fk_index=idx,
                    fk_spec=fk_spec,
                    default_branch=branch,
                )
                if injected:
                    tail_id = injected
        updated = True

    if not updated:
        return definition_json
    updated_definition = dict(definition_json)
    updated_definition["nodes"] = nodes
    updated_definition["edges"] = edges
    return updated_definition


def _is_cast_transform(node: Dict[str, Any]) -> bool:
    if not isinstance(node, dict):
        return False
    if node.get("type") != "transform":
        return False
    metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
    if normalize_operation(metadata.get("operation")) != "cast":
        return False
    casts = metadata.get("casts")
    return isinstance(casts, list) and len(casts) > 0


async def augment_definition_with_casts(
    *,
    definition_json: Dict[str, Any],
    db_name: str,
    branch: Optional[str],
    dataset_registry: DatasetRegistry,
) -> Dict[str, Any]:
    nodes_raw = definition_json.get("nodes")
    if not isinstance(nodes_raw, list) or not nodes_raw:
        return definition_json

    nodes, edges, node_by_id, existing_ids = _clone_definition_graph(definition_json)

    def outgoing_targets(node_id: str) -> List[str]:
        targets: List[str] = []
        for edge in edges:
            source = edge.get("from") or edge.get("source")
            target = edge.get("to") or edge.get("target")
            if source == node_id and target:
                targets.append(str(target))
        return targets

    updated = False
    for node_id, node in list(node_by_id.items()):
        if node.get("type") != "input":
            continue
        targets = outgoing_targets(node_id)
        if not targets:
            continue
        if all(_is_cast_transform(node_by_id.get(target_id, {})) for target_id in targets):
            continue

        selection = normalize_dataset_selection(node.get("metadata") or {}, default_branch=branch or "main")
        try:
            resolution = await resolve_dataset_version(
                dataset_registry,
                db_name=db_name,
                selection=selection,
            )
        except Exception:
            continue
        dataset = resolution.dataset
        version = resolution.version
        schema_json = getattr(dataset, "schema_json", None) if dataset else None
        if not schema_json and version is not None:
            schema_json = getattr(version, "sample_json", None)
        casts = extract_schema_casts(schema_json)
        if not casts:
            continue

        cast_node_id = unique_node_id(f"cast_{node_id}", existing_ids, start_index=1)
        existing_ids.add(cast_node_id)
        cast_node = {
            "id": cast_node_id,
            "type": "transform",
            "metadata": {
                "operation": "cast",
                "casts": casts,
            },
        }
        nodes.append(cast_node)
        node_by_id[cast_node_id] = cast_node
        rewired_edges: List[Dict[str, Any]] = []
        for edge in edges:
            source = edge.get("from") or edge.get("source")
            if source == node_id:
                updated_edge = dict(edge)
                updated_edge["from"] = cast_node_id
                updated_edge.pop("source", None)
                rewired_edges.append(updated_edge)
            else:
                rewired_edges.append(edge)
        edges = rewired_edges
        edges.append({"from": node_id, "to": cast_node_id})
        updated = True

    if not updated:
        return definition_json
    updated_definition = dict(definition_json)
    updated_definition["nodes"] = nodes
    updated_definition["edges"] = edges
    return updated_definition
