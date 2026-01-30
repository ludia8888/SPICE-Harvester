from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Tuple

from shared.services.pipeline.pipeline_definition_utils import resolve_execution_semantics
from shared.services.pipeline.pipeline_transform_spec import normalize_operation

PreviewPolicyLevel = Literal["allow", "warn", "require_spark", "deny"]
PreviewIssueSeverity = Literal["warn", "require_spark", "deny"]


@dataclass(frozen=True)
class PreviewPolicyIssue:
    severity: PreviewIssueSeverity
    code: str
    message: str
    node_id: Optional[str] = None
    operation: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
        }
        if self.node_id:
            payload["node_id"] = self.node_id
        if self.operation:
            payload["operation"] = self.operation
        return payload


_POLICY_SEVERITY_ORDER: Dict[PreviewPolicyLevel, int] = {
    "allow": 0,
    "warn": 1,
    "require_spark": 2,
    "deny": 3,
}


def _level_from_issues(issues: List[PreviewPolicyIssue]) -> PreviewPolicyLevel:
    level: PreviewPolicyLevel = "allow"
    for issue in issues:
        candidate: PreviewPolicyLevel
        if issue.severity == "warn":
            candidate = "warn"
        elif issue.severity == "require_spark":
            candidate = "require_spark"
        else:
            candidate = "deny"
        if _POLICY_SEVERITY_ORDER[candidate] > _POLICY_SEVERITY_ORDER[level]:
            level = candidate
    return level


_COMPARISON_OP_RE = re.compile(r">=|<=|!=|==|>|<|(?<![<>=!])=(?![=])")
_FILTER_COMPLEX_TOKEN_RE = re.compile(
    r"\b(and|or|not|in|like|rlike|between|is|null|case|when|then|else|end)\b",
    re.IGNORECASE,
)


def _count_comparison_ops(expression: str) -> int:
    return len(_COMPARISON_OP_RE.findall(expression))


def _is_simple_filter_expression(expression: str) -> bool:
    expr = str(expression or "").strip()
    if not expr:
        return True
    if "(" in expr or ")" in expr:
        return False
    if _FILTER_COMPLEX_TOKEN_RE.search(expr):
        return False
    if _count_comparison_ops(expr) != 1:
        return False
    return True


_SAFE_AST_NODES: Tuple[type, ...] = (
    ast.Expression,
    ast.BinOp,
    ast.UnaryOp,
    ast.Name,
    ast.Constant,
    ast.Load,
    ast.Compare,
    ast.BoolOp,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.Mod,
    ast.Pow,
    ast.USub,
    ast.UAdd,
    ast.And,
    ast.Or,
    ast.Eq,
    ast.NotEq,
    ast.Gt,
    ast.GtE,
    ast.Lt,
    ast.LtE,
)


def _is_safe_python_ast(tree: ast.AST) -> bool:
    for child in ast.walk(tree):
        if isinstance(child, _SAFE_AST_NODES):
            continue
        return False
    return True


_NUMERIC_LITERAL_RE = re.compile(r"^[+-]?\d+(\.\d+)?$")


def _is_timestamp_literal_arg(text: str) -> bool:
    trimmed = str(text or "").strip()
    if not trimmed:
        return False
    if _NUMERIC_LITERAL_RE.match(trimmed):
        return True
    if (trimmed.startswith("'") and trimmed.endswith("'")) or (trimmed.startswith('"') and trimmed.endswith('"')):
        return True
    return False


def _is_preview_safe_compute_expression(expression: str) -> bool:
    expr = str(expression or "").strip()
    if not expr:
        return True
    # Mirror the preview executor behavior: interpret Spark-style '=' comparisons as Python '=='.
    expr = re.sub(r"(?<![<>=!])=(?![=])", "==", expr)
    lower = expr.lower()
    if lower.startswith("to_timestamp(") and expr.endswith(")"):
        inner = expr[len("to_timestamp(") : -1].strip()
        return _is_timestamp_literal_arg(inner)
    if lower.startswith("timestamp(") and expr.endswith(")"):
        inner = expr[len("timestamp(") : -1].strip()
        return _is_timestamp_literal_arg(inner)
    try:
        tree = ast.parse(expr, mode="eval")
    except Exception:
        return False
    return _is_safe_python_ast(tree)


_SELECT_EXPR_SUSPICIOUS_RE = re.compile(r"[()`,;+*/%]|\\b(case|when|then|else|end|cast|try_cast|over)\\b", re.IGNORECASE)


def _select_expr_item_requires_spark(expr: str) -> bool:
    text = str(expr or "").strip()
    if not text:
        return False
    if _SELECT_EXPR_SUSPICIOUS_RE.search(text):
        return True
    lowered = text.lower()
    if " as " in lowered:
        left, right = [part.strip() for part in text.rsplit(" as ", 1)]
        if not left or not right:
            return True
        # The Python preview engine only supports selecting existing columns (optionally aliased).
        if _SELECT_EXPR_SUSPICIOUS_RE.search(left) or _SELECT_EXPR_SUSPICIOUS_RE.search(right):
            return True
        if " " in left or " " in right:
            return True
        return False
    # Non-aliased selectExpr should be a simple column reference (no whitespace).
    if " " in text:
        return True
    return False


def evaluate_preview_policy(definition_json: Dict[str, Any]) -> Dict[str, Any]:
    """Enterprise hard-gating for plan_preview.

    The Python preview executor is intentionally lightweight and cannot reproduce Spark semantics.
    This policy is conservative: if the preview could be misleading, require Spark preview instead.
    """

    issues: List[PreviewPolicyIssue] = []

    try:
        semantics = resolve_execution_semantics(definition_json)
    except Exception:
        semantics = "snapshot"
    if semantics in {"incremental", "streaming"}:
        issues.append(
            PreviewPolicyIssue(
                severity="require_spark",
                code="EXECUTION_SEMANTICS_REQUIRE_SPARK",
                message=f"execution_semantics={semantics} requires Spark-backed preview to validate watermarks/incremental behavior.",
            )
        )

    nodes = definition_json.get("nodes")
    if not isinstance(nodes, list):
        nodes = []

    for raw in nodes:
        if not isinstance(raw, dict):
            continue
        node_id = str(raw.get("id") or "").strip() or None
        node_type = str(raw.get("type") or "").strip().lower()
        if node_type != "transform":
            continue
        metadata = raw.get("metadata")
        metadata = metadata if isinstance(metadata, dict) else {}
        operation = normalize_operation(metadata.get("operation")).strip()
        op_lower = operation.lower()
        if not op_lower:
            continue

        if op_lower == "udf":
            issues.append(
                PreviewPolicyIssue(
                    severity="deny",
                    code="UDF_NOT_SUPPORTED_IN_SPARK_EXECUTION",
                    message="udf transform is not supported by Spark execution in this environment; preview would diverge from build/deploy.",
                    node_id=node_id,
                    operation=operation,
                )
            )
            continue

        if op_lower == "filter":
            expr = str(metadata.get("expression") or "")
            if not _is_simple_filter_expression(expr):
                issues.append(
                    PreviewPolicyIssue(
                        severity="require_spark",
                        code="FILTER_EXPRESSION_REQUIRES_SPARK",
                        message="filter expression is too complex for plan_preview (risk of no-op or wrong results); use Spark preview.",
                        node_id=node_id,
                        operation=operation,
                    )
                )
            continue

        if op_lower == "compute":
            # Structured compute is preferred; check all formulas.
            assignments = (
                metadata.get("assignments")
                or metadata.get("computedColumns")
                or metadata.get("computed_columns")
            )
            formulas: List[str] = []
            if isinstance(assignments, list) and assignments:
                for item in assignments:
                    if not isinstance(item, dict):
                        continue
                    formula = item.get("expression") or item.get("expr") or item.get("formula")
                    if isinstance(formula, str) and formula.strip():
                        formulas.append(formula)
            else:
                formula = metadata.get("formula") or metadata.get("expr")
                if isinstance(formula, str) and formula.strip():
                    formulas.append(formula)
                expr = metadata.get("expression")
                if isinstance(expr, str) and expr.strip():
                    formulas.append(expr)
            for formula in formulas:
                if not _is_preview_safe_compute_expression(formula):
                    issues.append(
                        PreviewPolicyIssue(
                            severity="require_spark",
                            code="COMPUTE_EXPRESSION_REQUIRES_SPARK",
                            message="compute expression uses Spark SQL features not supported by plan_preview; use Spark preview.",
                            node_id=node_id,
                            operation=operation,
                        )
                    )
                    break
            continue

        if op_lower in {"select"}:
            expressions = metadata.get("expressions") or metadata.get("selectExpr") or metadata.get("select_expr")
            if isinstance(expressions, list) and expressions:
                if any(_select_expr_item_requires_spark(item) for item in expressions):
                    issues.append(
                        PreviewPolicyIssue(
                            severity="require_spark",
                            code="SELECT_EXPR_REQUIRES_SPARK",
                            message="selectExpr contains expressions that plan_preview cannot evaluate; use Spark preview.",
                            node_id=node_id,
                            operation=operation,
                        )
                    )
            continue

        if op_lower in {"groupby", "aggregate"}:
            expr_items = (
                metadata.get("aggregateExpressions")
                or metadata.get("aggExpressions")
                or metadata.get("aggregate_expressions")
            )
            if isinstance(expr_items, list) and expr_items:
                issues.append(
                    PreviewPolicyIssue(
                        severity="require_spark",
                        code="AGGREGATE_EXPRESSIONS_REQUIRE_SPARK",
                        message="aggregateExpressions/aggExpressions require Spark preview for accurate results.",
                        node_id=node_id,
                        operation=operation,
                    )
                )
            # Even with structured aggregates, global ops can deviate due to sampling.
            issues.append(
                PreviewPolicyIssue(
                    severity="warn",
                    code="GLOBAL_AGGREGATION_PREVIEW_MAY_DIVERGE",
                    message="groupBy/aggregate results may diverge between preview sampling and full execution.",
                    node_id=node_id,
                    operation=operation,
                )
            )
            continue

        if op_lower == "join":
            # Joins are especially sensitive to independent sampling.
            issues.append(
                PreviewPolicyIssue(
                    severity="warn",
                    code="JOIN_PREVIEW_MAY_DIVERGE",
                    message="join preview may diverge due to sampling and null/duplicate key semantics; validate with Spark preview/build.",
                    node_id=node_id,
                    operation=operation,
                )
            )
            continue

        if op_lower == "pivot":
            pivot_meta = metadata.get("pivot") if isinstance(metadata.get("pivot"), dict) else {}
            agg = str(pivot_meta.get("agg") or "sum").strip().lower()
            if agg in {"min", "max"}:
                issues.append(
                    PreviewPolicyIssue(
                        severity="require_spark",
                        code="PIVOT_AGG_REQUIRES_SPARK",
                        message=f"pivot agg={agg} requires Spark preview (plan_preview only supports sum/count/avg).",
                        node_id=node_id,
                        operation=operation,
                    )
                )
            issues.append(
                PreviewPolicyIssue(
                    severity="warn",
                    code="PIVOT_PREVIEW_MAY_DIVERGE",
                    message="pivot results may diverge between preview sampling and full execution.",
                    node_id=node_id,
                    operation=operation,
                )
            )
            continue

        if op_lower == "window":
            window_meta = metadata.get("window") if isinstance(metadata.get("window"), dict) else {}
            expressions = window_meta.get("expressions") if isinstance(window_meta.get("expressions"), list) else None
            if expressions:
                issues.append(
                    PreviewPolicyIssue(
                        severity="require_spark",
                        code="WINDOW_EXPRESSIONS_REQUIRE_SPARK",
                        message="window expressions require Spark preview.",
                        node_id=node_id,
                        operation=operation,
                    )
                )
            output_col = str(window_meta.get("outputColumn") or "row_number").strip()
            if output_col and output_col != "row_number":
                issues.append(
                    PreviewPolicyIssue(
                        severity="require_spark",
                        code="WINDOW_OUTPUT_COLUMN_REQUIRES_SPARK",
                        message="window outputColumn differs from plan_preview default 'row_number'; use Spark preview.",
                        node_id=node_id,
                        operation=operation,
                    )
                )
            issues.append(
                PreviewPolicyIssue(
                    severity="warn",
                    code="WINDOW_PREVIEW_MAY_DIVERGE",
                    message="window results may diverge between preview sampling and full execution.",
                    node_id=node_id,
                    operation=operation,
                )
            continue

        if op_lower in {"dedupe", "sort", "union"}:
            issues.append(
                PreviewPolicyIssue(
                    severity="warn",
                    code="GLOBAL_OPERATION_PREVIEW_MAY_DIVERGE",
                    message=f"{op_lower} results may diverge between preview sampling and full execution.",
                    node_id=node_id,
                    operation=operation,
                )
            )
            continue

        if op_lower in {"cast", "regexreplace"}:
            issues.append(
                PreviewPolicyIssue(
                    severity="warn",
                    code="ENGINE_DIFFERENCES_PREVIEW_MAY_DIVERGE",
                    message=f"{op_lower} behavior may differ between Python preview and Spark execution; validate with Spark preview/build.",
                    node_id=node_id,
                    operation=operation,
                )
            )
            continue

    level = _level_from_issues(issues)
    suggested_tool = "plan_preview"
    if level in {"require_spark", "deny"}:
        suggested_tool = "pipeline_preview_wait"

    return {
        "level": level,
        "suggested_tool": suggested_tool,
        "issues": [issue.to_dict() for issue in issues],
    }

