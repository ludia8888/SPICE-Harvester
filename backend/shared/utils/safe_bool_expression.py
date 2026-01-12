from __future__ import annotations

import ast
from typing import Any, Mapping


class BoolExpressionError(ValueError):
    pass


class UnsafeBoolExpressionError(BoolExpressionError):
    pass


class BoolExpressionEvaluationError(BoolExpressionError):
    pass


_ALLOWED_NODES: tuple[type[ast.AST], ...] = (
    ast.Expression,
    ast.BoolOp,
    ast.UnaryOp,
    ast.Compare,
    ast.Name,
    ast.Load,
    ast.Constant,
    ast.Attribute,
    ast.Subscript,
    ast.List,
    ast.Tuple,
    ast.Set,
    ast.Dict,
    ast.And,
    ast.Or,
    ast.Not,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.In,
    ast.NotIn,
    ast.Is,
    ast.IsNot,
)


def safe_eval_bool_expression(
    expression: str,
    *,
    variables: Mapping[str, Any],
    max_nodes: int = 200,
) -> bool:
    expr = str(expression or "").strip()
    if not expr:
        return True

    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise UnsafeBoolExpressionError(f"invalid expression syntax: {exc}") from exc

    _validate_bool_expression_ast(tree, max_nodes=max_nodes)
    value = _eval_bool_expression_node(tree.body, variables=variables)
    if not isinstance(value, bool):
        raise BoolExpressionEvaluationError("expression must evaluate to a boolean")
    return value


def validate_bool_expression_syntax(expression: str, *, max_nodes: int = 200) -> None:
    """
    Validate a boolean expression for safety and syntax without evaluating it.

    This is intended for ontology validation at ActionType definition time.
    """
    expr = str(expression or "").strip()
    if not expr:
        return

    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise UnsafeBoolExpressionError(f"invalid expression syntax: {exc}") from exc

    _validate_bool_expression_ast(tree, max_nodes=max_nodes)


def _validate_bool_expression_ast(tree: ast.AST, *, max_nodes: int) -> None:
    nodes_seen = 0
    for node in ast.walk(tree):
        nodes_seen += 1
        if nodes_seen > max_nodes:
            raise UnsafeBoolExpressionError("expression too complex")

        if not isinstance(node, _ALLOWED_NODES):
            raise UnsafeBoolExpressionError(f"unsupported expression node: {type(node).__name__}")

        if isinstance(node, ast.Name) and node.id.startswith("_"):
            raise UnsafeBoolExpressionError("private identifiers are not allowed")

        if isinstance(node, ast.Attribute):
            if node.attr.startswith("_"):
                raise UnsafeBoolExpressionError("private attribute access is not allowed")

        if isinstance(node, ast.Subscript):
            slice_node = node.slice
            if isinstance(slice_node, ast.Constant):
                if not isinstance(slice_node.value, (int, str)):
                    raise UnsafeBoolExpressionError("subscript index must be int or str constant")
                continue
            raise UnsafeBoolExpressionError("only constant subscripts are supported")


def _eval_bool_expression_node(node: ast.AST, *, variables: Mapping[str, Any]) -> Any:
    if isinstance(node, ast.Constant):
        return node.value

    if isinstance(node, ast.Name):
        if node.id in variables:
            return variables[node.id]
        raise BoolExpressionEvaluationError(f"unknown identifier: {node.id}")

    if isinstance(node, ast.Attribute):
        base = _eval_bool_expression_node(node.value, variables=variables)
        if not isinstance(base, dict):
            raise BoolExpressionEvaluationError("attribute access is only supported on objects")
        if node.attr not in base:
            raise BoolExpressionEvaluationError(f"missing attribute: {node.attr}")
        return base[node.attr]

    if isinstance(node, ast.Subscript):
        base = _eval_bool_expression_node(node.value, variables=variables)
        index = node.slice.value if isinstance(node.slice, ast.Constant) else None
        if isinstance(base, dict):
            if index not in base:
                raise BoolExpressionEvaluationError(f"missing key: {index}")
            return base[index]
        if isinstance(base, (list, tuple)):
            if not isinstance(index, int):
                raise BoolExpressionEvaluationError("list indices must be integers")
            try:
                return base[index]
            except IndexError as exc:
                raise BoolExpressionEvaluationError("index out of range") from exc
        raise BoolExpressionEvaluationError("subscript is only supported on list/tuple/dict")

    if isinstance(node, ast.List):
        return [_eval_bool_expression_node(item, variables=variables) for item in node.elts]

    if isinstance(node, ast.Tuple):
        return tuple(_eval_bool_expression_node(item, variables=variables) for item in node.elts)

    if isinstance(node, ast.Set):
        return {_eval_bool_expression_node(item, variables=variables) for item in node.elts}

    if isinstance(node, ast.Dict):
        return {
            _eval_bool_expression_node(k, variables=variables): _eval_bool_expression_node(v, variables=variables)
            for k, v in zip(node.keys, node.values)
        }

    if isinstance(node, ast.UnaryOp):
        if isinstance(node.op, ast.Not):
            operand = _eval_bool_expression_node(node.operand, variables=variables)
            if not isinstance(operand, bool):
                raise BoolExpressionEvaluationError("not operand must be boolean")
            return not operand
        raise UnsafeBoolExpressionError(f"unsupported unary operator: {type(node.op).__name__}")

    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            for item in node.values:
                value = _eval_bool_expression_node(item, variables=variables)
                if not isinstance(value, bool):
                    raise BoolExpressionEvaluationError("and operands must be boolean")
                if not value:
                    return False
            return True
        if isinstance(node.op, ast.Or):
            for item in node.values:
                value = _eval_bool_expression_node(item, variables=variables)
                if not isinstance(value, bool):
                    raise BoolExpressionEvaluationError("or operands must be boolean")
                if value:
                    return True
            return False
        raise UnsafeBoolExpressionError(f"unsupported boolean operator: {type(node.op).__name__}")

    if isinstance(node, ast.Compare):
        left = _eval_bool_expression_node(node.left, variables=variables)
        for op, comparator in zip(node.ops, node.comparators):
            right = _eval_bool_expression_node(comparator, variables=variables)
            if not _apply_compare(left=left, op=op, right=right):
                return False
            left = right
        return True

    raise UnsafeBoolExpressionError(f"unsupported expression node: {type(node).__name__}")


def _apply_compare(*, left: Any, op: ast.AST, right: Any) -> bool:
    try:
        if isinstance(op, ast.Eq):
            return left == right
        if isinstance(op, ast.NotEq):
            return left != right
        if isinstance(op, ast.Lt):
            return left < right
        if isinstance(op, ast.LtE):
            return left <= right
        if isinstance(op, ast.Gt):
            return left > right
        if isinstance(op, ast.GtE):
            return left >= right
        if isinstance(op, ast.In):
            return left in right
        if isinstance(op, ast.NotIn):
            return left not in right
        if isinstance(op, ast.Is):
            return left is right
        if isinstance(op, ast.IsNot):
            return left is not right
    except TypeError as exc:
        raise BoolExpressionEvaluationError(str(exc)) from exc
    raise UnsafeBoolExpressionError(f"unsupported comparison operator: {type(op).__name__}")
