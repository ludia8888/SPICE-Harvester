"""
Claim-based refuter for pipeline plans.

This module implements a *refutation gate*:
- It never "proves" a plan/claim correct.
- It only produces HARD failures when it can provide a concrete counterexample ("witness").

The refuter is intentionally domain-agnostic and relies only on:
- plan structure (nodes/edges/metadata), and
- deterministic sample tables produced by PipelineExecutor (or provided run_tables).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from shared.models.pipeline_plan import PipelinePlan
from shared.services.dataset_registry import DatasetRegistry
from shared.services.pipeline_executor import PipelineExecutor, PipelineTable, _cast_value_with_status  # type: ignore
from shared.services.pipeline_graph_utils import build_incoming, normalize_edges, normalize_nodes
from shared.services.pipeline_transform_spec import normalize_operation, resolve_join_spec
from shared.utils.llm_safety import mask_pii

logger = logging.getLogger(__name__)


def _normalize_string_list(value: Any) -> List[str]:
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    out: List[str] = []
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        parts = [part.strip() for part in text.replace("+", ",").split(",") if part.strip()]
        out.extend(parts if parts else [text])
    return [item for item in out if item]


def _normalize_claim_kind(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    key = raw.replace("-", "_").upper()
    if key in {"PK", "PRIMARY_KEY", "PRIMARYKEY"}:
        return "PK"
    if key in {"FK", "FOREIGN_KEY", "FOREIGNKEY"}:
        return "FK"
    if key in {"CAST_SUCCESS", "CAST_ALL_PARSE", "CAST_PARSE_ALL"}:
        return "CAST_SUCCESS"
    if key in {"CAST_LOSSLESS", "CAST_ROUNDTRIP", "CAST_LOSSLESS_ROUNDTRIP"}:
        return "CAST_LOSSLESS"
    if key in {"JOIN_ASSUMES_RIGHT_PK", "JOIN_RIGHT_PK"}:
        return "JOIN_ASSUMES_RIGHT_PK"
    if key in {"JOIN_FUNCTIONAL_RIGHT", "JOIN_N_TO_1", "JOIN_N1"}:
        return "JOIN_FUNCTIONAL_RIGHT"
    if key in {"ROW_PRESERVE_LEFT", "JOIN_ROW_PRESERVE_LEFT"}:
        return "ROW_PRESERVE_LEFT"
    return key


def _normalize_claim_severity(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"SOFT", "WARN", "WARNING"}:
        return "SOFT"
    if raw in {"HARD", "FAIL", "BLOCK"}:
        return "HARD"
    return "HARD"


def _claim_spec(claim: Dict[str, Any]) -> Dict[str, Any]:
    spec = claim.get("spec")
    return spec if isinstance(spec, dict) else {}


def _claim_target_node_id(claim: Dict[str, Any]) -> Optional[str]:
    target = claim.get("target")
    if isinstance(target, dict):
        for key in ("node_id", "nodeId", "table_node_id", "tableNodeId"):
            value = target.get(key)
            if value:
                return str(value).strip() or None
    return None


def _row_ref(
    row: Dict[str, Any],
    *,
    row_index: int,
    include_columns: List[str],
) -> Dict[str, Any]:
    values = {col: row.get(col) for col in include_columns}
    return {"row_index": row_index, "values": values}


def _refute_pk(
    table: PipelineTable,
    *,
    key_cols: List[str],
    require_not_null: bool,
) -> Optional[Dict[str, Any]]:
    if not key_cols:
        return {
            "description": "PK claim missing key_cols",
            "row_refs": [],
            "values": {"key_cols": []},
            "unverifiable": True,
        }
    missing_cols = [col for col in key_cols if col not in table.columns]
    if missing_cols:
        return {
            "description": "PK key column(s) missing from table",
            "row_refs": [],
            "values": {"missing_columns": missing_cols, "key_cols": key_cols},
            "unverifiable": True,
        }

    seen: Dict[Tuple[Any, ...], int] = {}
    for idx, row in enumerate(table.rows):
        key = tuple(row.get(col) for col in key_cols)
        if require_not_null and any(value is None for value in key):
            return {
                "description": "PK has NULL in key",
                "row_refs": [_row_ref(row, row_index=idx, include_columns=key_cols)],
                "values": {"key_cols": key_cols, "key": list(key)},
            }
        if key in seen:
            other_idx = seen[key]
            other = table.rows[other_idx] if 0 <= other_idx < len(table.rows) else {}
            return {
                "description": "Duplicate PK key found",
                "row_refs": [
                    _row_ref(other, row_index=other_idx, include_columns=key_cols),
                    _row_ref(row, row_index=idx, include_columns=key_cols),
                ],
                "values": {"key_cols": key_cols, "key": list(key)},
            }
        seen[key] = idx
    return None


def _refute_join_functional_right(
    left: PipelineTable,
    right: PipelineTable,
    *,
    left_keys: List[str],
    right_keys: List[str],
) -> Optional[Dict[str, Any]]:
    if not left_keys or not right_keys or len(left_keys) != len(right_keys):
        return {
            "description": "JOIN_FUNCTIONAL_RIGHT missing or mismatched join keys",
            "row_refs": [],
            "values": {"left_keys": left_keys, "right_keys": right_keys},
            "unverifiable": True,
        }
    missing_left = [col for col in left_keys if col not in left.columns]
    missing_right = [col for col in right_keys if col not in right.columns]
    if missing_left or missing_right:
        return {
            "description": "JOIN_FUNCTIONAL_RIGHT join key column(s) missing",
            "row_refs": [],
            "values": {"missing_left": missing_left, "missing_right": missing_right},
            "unverifiable": True,
        }

    left_index: Dict[Tuple[Any, ...], int] = {}
    for li, row in enumerate(left.rows):
        k = tuple(row.get(col) for col in left_keys)
        if any(v is None for v in k):
            continue
        # Store first witness row for this key.
        left_index.setdefault(k, li)

    first_right: Dict[Tuple[Any, ...], int] = {}
    for ri, row in enumerate(right.rows):
        k = tuple(row.get(col) for col in right_keys)
        if any(v is None for v in k):
            continue
        if k in first_right:
            li = left_index.get(k)
            if li is None:
                continue
            r1 = first_right[k]
            left_row = left.rows[li] if 0 <= li < len(left.rows) else {}
            right_row_1 = right.rows[r1] if 0 <= r1 < len(right.rows) else {}
            return {
                "description": "Non-functional join: left key matches multiple right rows",
                "row_refs": [
                    {"side": "left", **_row_ref(left_row, row_index=li, include_columns=left_keys)},
                    {"side": "right", **_row_ref(right_row_1, row_index=r1, include_columns=right_keys)},
                    {"side": "right", **_row_ref(row, row_index=ri, include_columns=right_keys)},
                ],
                "values": {"join_key": list(k), "left_keys": left_keys, "right_keys": right_keys},
            }
        first_right[k] = ri
    return None


def _refute_cast_success(
    table: PipelineTable,
    casts: List[Dict[str, Any]],
    *,
    only_columns: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    requested = set(_normalize_string_list(only_columns)) if only_columns else None
    cast_specs: List[Tuple[str, str]] = []
    for item in casts or []:
        if not isinstance(item, dict):
            continue
        col = str(item.get("column") or "").strip()
        target = str(item.get("type") or "").strip()
        if not col or not target:
            continue
        if requested is not None and col not in requested:
            continue
        cast_specs.append((col, target))

    if not cast_specs:
        return {
            "description": "CAST_SUCCESS has no casts to check",
            "row_refs": [],
            "values": {"casts": casts},
            "unverifiable": True,
        }

    for col, target in cast_specs:
        if col not in table.columns:
            return {
                "description": "CAST_SUCCESS column missing from table",
                "row_refs": [],
                "values": {"column": col, "target_type": target},
                "unverifiable": True,
            }

    for idx, row in enumerate(table.rows):
        for col, target in cast_specs:
            value = row.get(col)
            _, attempted, failed = _cast_value_with_status(value, target, cast_mode="SAFE_NULL")
            if attempted and failed:
                return {
                    "description": "Cast parse failed",
                    "row_refs": [_row_ref(row, row_index=idx, include_columns=[col])],
                    "values": {"column": col, "target_type": target, "value": value},
                }
    return None


def _extract_claims(definition_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    nodes = definition_json.get("nodes")
    if not isinstance(nodes, list):
        return []
    claims: List[Dict[str, Any]] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("id") or "").strip()
        if not node_id:
            continue
        meta = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
        raw_claims = meta.get("claims")
        if not isinstance(raw_claims, list):
            continue
        for idx, item in enumerate(raw_claims):
            if not isinstance(item, dict):
                continue
            claim = dict(item)
            claim_id = str(claim.get("id") or "").strip()
            kind = _normalize_claim_kind(claim.get("kind") or claim.get("type") or "")
            if not claim_id:
                base = kind or "claim"
                claim_id = f"{node_id}:{base}:{idx + 1}"
            claim["id"] = claim_id
            claim["_node_id"] = node_id
            claim["_kind"] = kind
            claim["_severity"] = _normalize_claim_severity(claim.get("severity"))
            claims.append(claim)
    claims.sort(key=lambda c: (str(c.get("_node_id") or ""), str(c.get("id") or "")))
    return claims


async def refute_pipeline_plan_claims(
    *,
    plan: PipelinePlan,
    dataset_registry: Optional[DatasetRegistry] = None,
    run_tables: Optional[Dict[str, Any]] = None,
    sample_limit: int = 400,
    max_output_rows: int = 20000,
    max_hard_failures: int = 5,
    max_soft_warnings: int = 20,
) -> Dict[str, Any]:
    """
    Returns:
      {
        status: "success" | "invalid",
        gate: "PASS_NOT_REFUTED" | "BLOCK",
        hard_failures: [Violation...],
        soft_warnings: [Violation...],
        errors: string[],
        warnings: string[],
        stats: {...}
      }
    """
    definition_json = dict(plan.definition_json or {})
    nodes = normalize_nodes(definition_json.get("nodes"))
    edges = normalize_edges(definition_json.get("edges"))
    incoming = build_incoming(edges)

    claims = _extract_claims(definition_json)
    warnings: List[str] = []
    if not claims:
        return {
            "status": "success",
            "gate": "PASS_NOT_REFUTED",
            "hard_failures": [],
            "soft_warnings": [],
            "errors": [],
            # No claims is normal; the refuter is opt-in via claims.
            "warnings": [],
            "stats": {"claims_total": 0},
        }

    tables: Dict[str, PipelineTable] = {}
    if run_tables is not None:
        # run_tables format is compatible with PipelineExecutor preview payload:
        # { node_id: { columns: [...], rows: [...] } }
        for node_id, payload in (run_tables or {}).items():
            if not isinstance(node_id, str) or not node_id.strip():
                continue
            if not isinstance(payload, dict):
                continue
            cols_raw = payload.get("columns")
            rows_raw = payload.get("rows")
            if not isinstance(cols_raw, list) or not isinstance(rows_raw, list):
                continue
            cols: List[str] = []
            for col in cols_raw:
                if isinstance(col, dict):
                    name = str(col.get("name") or "").strip()
                else:
                    name = str(col or "").strip()
                if name:
                    cols.append(name)
            rows: List[Dict[str, Any]] = [dict(row) for row in rows_raw if isinstance(row, dict)]
            tables[str(node_id)] = PipelineTable(columns=cols, rows=rows)
    else:
        if not dataset_registry:
            return {
                "status": "invalid",
                "gate": "BLOCK",
                "hard_failures": [],
                "soft_warnings": [],
                "errors": ["dataset_registry is required when run_tables is not provided"],
                "warnings": [],
                "stats": {"claims_total": len(claims)},
            }
        db_name = str(plan.data_scope.db_name or "").strip()
        if not db_name:
            return {
                "status": "invalid",
                "gate": "BLOCK",
                "hard_failures": [],
                "soft_warnings": [],
                "errors": ["plan.data_scope.db_name is required"],
                "warnings": [],
                "stats": {"claims_total": len(claims)},
            }
        executor = PipelineExecutor(dataset_registry)
        definition_for_run = dict(definition_json)
        preview_meta = dict(definition_for_run.get("__preview_meta__") or {})
        preview_meta.setdefault("branch", str(plan.data_scope.branch or "") or "main")
        try:
            preview_meta["sample_limit"] = max(1, min(int(sample_limit or 0), 1200))
        except (TypeError, ValueError):
            preview_meta["sample_limit"] = 400
        try:
            preview_meta["max_output_rows"] = max(1, min(int(max_output_rows or 0), 250000))
        except (TypeError, ValueError):
            preview_meta["max_output_rows"] = 20000
        definition_for_run["__preview_meta__"] = preview_meta
        try:
            run_result = await executor.run(definition=definition_for_run, db_name=db_name)
        except Exception as exc:
            return {
                "status": "invalid",
                "gate": "BLOCK",
                "hard_failures": [],
                "soft_warnings": [],
                "errors": [f"preview run failed: {exc}"],
                "warnings": [],
                "stats": {"claims_total": len(claims)},
            }
        tables = dict(run_result.tables or {})

    hard_failures: List[Dict[str, Any]] = []
    soft_warnings: List[Dict[str, Any]] = []
    checked = 0

    for claim in claims:
        if len(hard_failures) >= max(0, int(max_hard_failures or 0)):
            warnings.append("hard failure limit reached; remaining claims not checked")
            break

        claim_id = str(claim.get("id") or "").strip() or "claim"
        kind = str(claim.get("_kind") or "")
        severity = str(claim.get("_severity") or "HARD")
        node_id = _claim_target_node_id(claim) or str(claim.get("_node_id") or "").strip()
        spec = _claim_spec(claim)

        checked += 1
        witness: Optional[Dict[str, Any]] = None
        unsound_hard = False

        if kind == "PK":
            key_cols = _normalize_string_list(spec.get("key_cols") or spec.get("keyCols") or spec.get("columns"))
            require_not_null = True if "require_not_null" not in spec and "requireNotNull" not in spec else bool(
                spec.get("require_not_null", spec.get("requireNotNull"))
            )
            table = tables.get(node_id)
            if not table:
                witness = {
                    "description": "Target table not available for PK check",
                    "row_refs": [],
                    "values": {"node_id": node_id},
                    "unverifiable": True,
                }
            else:
                witness = _refute_pk(table, key_cols=key_cols, require_not_null=require_not_null)

        elif kind in {"JOIN_ASSUMES_RIGHT_PK", "JOIN_FUNCTIONAL_RIGHT", "ROW_PRESERVE_LEFT"}:
            node = nodes.get(node_id) if node_id else None
            meta = node.get("metadata") if isinstance(node, dict) else {}
            operation = normalize_operation(meta.get("operation"))
            if operation != "join":
                witness = {
                    "description": f"{kind} applied to non-join node",
                    "row_refs": [],
                    "values": {"node_id": node_id, "operation": operation},
                    "unverifiable": True,
                }
            else:
                join_spec = resolve_join_spec(meta or {})
                inputs = incoming.get(node_id, [])
                left_id = inputs[0] if len(inputs) >= 1 else None
                right_id = inputs[1] if len(inputs) >= 2 else None
                if not left_id or not right_id:
                    witness = {
                        "description": "Join node missing inputs",
                        "row_refs": [],
                        "values": {"node_id": node_id, "inputs": inputs},
                        "unverifiable": True,
                    }
                else:
                    left_table = tables.get(left_id)
                    right_table = tables.get(right_id)
                    if not left_table or not right_table:
                        witness = {
                            "description": "Join input table(s) not available",
                            "row_refs": [],
                            "values": {"left_id": left_id, "right_id": right_id},
                            "unverifiable": True,
                        }
                    else:
                        left_keys = list(join_spec.left_keys or [])
                        right_keys = list(join_spec.right_keys or [])
                        if kind == "JOIN_ASSUMES_RIGHT_PK":
                            require_not_null = True if "require_not_null" not in spec and "requireNotNull" not in spec else bool(
                                spec.get("require_not_null", spec.get("requireNotNull"))
                            )
                            witness = _refute_pk(right_table, key_cols=right_keys, require_not_null=require_not_null)
                        elif kind == "JOIN_FUNCTIONAL_RIGHT":
                            witness = _refute_join_functional_right(
                                left_table,
                                right_table,
                                left_keys=left_keys,
                                right_keys=right_keys,
                            )
                        else:
                            # ROW_PRESERVE_LEFT is tricky under sampling + max_output_rows truncation.
                            # Only provide SOFT guidance unless the agent explicitly sets severity=SOFT.
                            unsound_hard = True
                            witness = {
                                "description": "ROW_PRESERVE_LEFT is not hard-refutable under sampling/truncation; treated as soft",
                                "row_refs": [],
                                "values": {"node_id": node_id},
                                "unverifiable": True,
                            }

        elif kind in {"CAST_SUCCESS", "CAST_LOSSLESS"}:
            node = nodes.get(node_id) if node_id else None
            meta = node.get("metadata") if isinstance(node, dict) else {}
            operation = normalize_operation(meta.get("operation"))
            if operation != "cast":
                witness = {
                    "description": f"{kind} applied to non-cast node",
                    "row_refs": [],
                    "values": {"node_id": node_id, "operation": operation},
                    "unverifiable": True,
                }
            else:
                inputs = incoming.get(node_id, [])
                src_id = inputs[0] if inputs else None
                src_table = tables.get(src_id) if src_id else None
                if not src_table:
                    witness = {
                        "description": "Cast input table not available",
                        "row_refs": [],
                        "values": {"node_id": node_id, "input": src_id},
                        "unverifiable": True,
                    }
                else:
                    casts = meta.get("casts") if isinstance(meta.get("casts"), list) else []
                    only_columns = _normalize_string_list(spec.get("columns") or spec.get("cols")) if spec else None
                    if kind == "CAST_SUCCESS":
                        witness = _refute_cast_success(src_table, casts, only_columns=only_columns)
                    else:
                        # CAST_LOSSLESS needs a user-provided canonicalization spec to be meaningful.
                        unsound_hard = True
                        witness = {
                            "description": "CAST_LOSSLESS is not supported without explicit canonicalization spec; treated as soft",
                            "row_refs": [],
                            "values": {"node_id": node_id},
                            "unverifiable": True,
                        }

        elif kind == "FK":
            # FK "orphan" checks are not sound under head-sampling. We can emit SOFT hints only.
            unsound_hard = True
            witness = {
                "description": "FK refutation is sample-based and not hard-gated (cannot prove absence in parent set)",
                "row_refs": [],
                "values": {"note": "treated as soft unless a full-scan membership oracle exists"},
                "unverifiable": True,
            }

        else:
            unsound_hard = True
            witness = {
                "description": "Unknown/unsupported claim kind",
                "row_refs": [],
                "values": {"kind": kind},
                "unverifiable": True,
            }

        if witness is None:
            continue

        if isinstance(witness, dict) and witness.get("unverifiable"):
            unsound_hard = True

        violation = {
            "claim_id": claim_id,
            "kind": kind,
            "severity": "HARD_FAIL" if (severity == "HARD" and not unsound_hard) else "SOFT_WARN",
            "witness": witness,
            "hint": claim.get("hint") if isinstance(claim.get("hint"), dict) else None,
        }

        if severity == "HARD" and not unsound_hard:
            hard_failures.append(violation)
        else:
            soft_warnings.append(violation)
            if len(soft_warnings) >= max(0, int(max_soft_warnings or 0)):
                warnings.append("soft warning limit reached; remaining soft issues not reported")
                break

    gate = "BLOCK" if hard_failures else "PASS_NOT_REFUTED"
    status = "invalid" if hard_failures else "success"
    errors = [f"{v.get('claim_id')}: {((v.get('witness') or {}).get('description') or '')}" for v in hard_failures][:20]
    payload = {
        "status": status,
        "gate": gate,
        "hard_failures": hard_failures,
        "soft_warnings": soft_warnings,
        "errors": errors,
        "warnings": warnings,
        "stats": {
            "claims_total": len(claims),
            "claims_checked": checked,
            "hard_failures": len(hard_failures),
            "soft_warnings": len(soft_warnings),
        },
    }
    return mask_pii(payload)
