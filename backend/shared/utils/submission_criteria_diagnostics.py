from __future__ import annotations

import ast
from typing import Any, Dict, List, Set


def infer_submission_criteria_failure_reason(expression: str) -> Dict[str, Any]:
    """
    Best-effort heuristics for classifying `submission_criteria` failures.

    This is designed for agent branching:
    - `missing_role`: criteria references `user.role`
    - `state_mismatch`: criteria references target state (target/targets or other non-user identifiers)
    - `mixed`: both categories are present
    - `unknown`: cannot infer
    """

    expr = str(expression or "").strip()
    if not expr:
        return {"reason": "unknown", "reasons": ["unknown"], "identifiers": []}

    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError:
        return {"reason": "unknown", "reasons": ["unknown"], "identifiers": []}

    identifiers: Set[str] = set()
    mentions_user_role = False

    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            identifiers.add(node.id)
            continue

        if isinstance(node, ast.Attribute):
            if node.attr == "role" and isinstance(node.value, ast.Name) and node.value.id == "user":
                mentions_user_role = True

            root = node.value
            while isinstance(root, (ast.Attribute, ast.Subscript)):
                root = root.value  # type: ignore[assignment]
            if isinstance(root, ast.Name):
                identifiers.add(root.id)
            continue

        if isinstance(node, ast.Subscript):
            if isinstance(node.value, ast.Name) and node.value.id == "user":
                slice_node = node.slice
                if isinstance(slice_node, ast.Constant) and slice_node.value == "role":
                    mentions_user_role = True

            root = node.value
            while isinstance(root, (ast.Attribute, ast.Subscript)):
                root = root.value  # type: ignore[assignment]
            if isinstance(root, ast.Name):
                identifiers.add(root.id)

    allowed_non_state = {"user", "input", "db_name", "base_branch", "targets_meta"}
    mentions_state = any(name not in allowed_non_state for name in identifiers)

    reasons: List[str] = []
    if mentions_user_role:
        reasons.append("missing_role")
    if mentions_state:
        reasons.append("state_mismatch")
    if not reasons:
        reasons = ["unknown"]

    reason = reasons[0] if len(reasons) == 1 else "mixed"
    return {"reason": reason, "reasons": reasons, "identifiers": sorted(identifiers)}

