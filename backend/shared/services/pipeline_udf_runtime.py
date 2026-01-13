from __future__ import annotations

import ast
from typing import Any, Callable, Dict


class PipelineUdfError(ValueError):
    pass


_SAFE_BUILTINS: Dict[str, Any] = {
    "abs": abs,
    "bool": bool,
    "dict": dict,
    "enumerate": enumerate,
    "float": float,
    "int": int,
    "len": len,
    "list": list,
    "max": max,
    "min": min,
    "range": range,
    "round": round,
    "str": str,
    "sum": sum,
}


_DISALLOWED_AST_NODES = (
    ast.Import,
    ast.ImportFrom,
    ast.AsyncFunctionDef,
    ast.ClassDef,
    ast.With,
    ast.AsyncWith,
    ast.Try,
    ast.Raise,
    ast.Global,
    ast.Nonlocal,
    ast.While,
    ast.For,
    ast.AsyncFor,
    ast.Lambda,
    ast.Await,
    ast.Yield,
    ast.YieldFrom,
)

_DISALLOWED_AST_NODE_MESSAGES = {
    ast.Import: "UDF code cannot import modules",
    ast.ImportFrom: "UDF code cannot import modules",
    ast.AsyncFunctionDef: "UDF code cannot define async functions",
    ast.ClassDef: "UDF code cannot define classes",
    ast.With: "UDF code cannot use 'with' statements",
    ast.AsyncWith: "UDF code cannot use 'with' statements",
    ast.Try: "UDF code cannot use try/except",
    ast.Raise: "UDF code cannot raise exceptions",
    ast.Global: "UDF code cannot declare global variables",
    ast.Nonlocal: "UDF code cannot declare nonlocal variables",
    ast.While: "UDF code cannot use while loops",
    ast.For: "UDF code cannot use for loops",
    ast.AsyncFor: "UDF code cannot use for loops",
    ast.Lambda: "UDF code cannot define lambdas",
    ast.Await: "UDF code cannot use await",
    ast.Yield: "UDF code cannot use yield",
    ast.YieldFrom: "UDF code cannot use yield from",
}


class _UdfAstValidator(ast.NodeVisitor):
    def generic_visit(self, node: ast.AST) -> None:
        if isinstance(node, _DISALLOWED_AST_NODES):
            message = _DISALLOWED_AST_NODE_MESSAGES.get(type(node))
            raise PipelineUdfError(message or f"UDF code uses disallowed syntax: {type(node).__name__}")
        super().generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        # Prevent common sandbox escapes via dunder and frame/coroutine attributes
        # like __class__/__mro__/__subclasses__/gi_frame/f_globals/etc.
        if "_" in node.attr:
            raise PipelineUdfError("UDF code cannot access private attributes")
        self.generic_visit(node)


def _validate_udf_ast(tree: ast.AST) -> None:
    if not isinstance(tree, ast.Module):  # pragma: no cover
        raise PipelineUdfError("Invalid UDF AST")

    # Disallow any top-level statements besides function definitions and an optional module docstring.
    has_transform = False
    for stmt in tree.body:
        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant) and isinstance(stmt.value.value, str):
            continue
        if isinstance(stmt, (ast.Import, ast.ImportFrom)):
            raise PipelineUdfError("UDF code cannot import modules")
        if isinstance(stmt, ast.FunctionDef):
            if stmt.name == "transform":
                has_transform = True
            continue
        raise PipelineUdfError("UDF code may only contain function definitions (no top-level statements)")

    if not has_transform:
        raise PipelineUdfError("UDF must define callable transform(row)")

    _UdfAstValidator().visit(tree)


def compile_row_udf(code: str) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    """
    Compile a Python UDF for row-level transforms.

    Contract:
    - user must define `transform(row)` where row is a dict and return a dict
    - imports are not allowed
    """

    code = (code or "").strip()
    if not code:
        raise PipelineUdfError("UDF code is empty")

    try:
        tree = ast.parse(code, mode="exec")
    except SyntaxError as exc:  # pragma: no cover
        raise PipelineUdfError(f"Invalid UDF syntax: {exc}") from exc

    _validate_udf_ast(tree)

    globals_dict: Dict[str, Any] = {"__builtins__": dict(_SAFE_BUILTINS)}
    locals_dict: Dict[str, Any] = {}
    exec(compile(tree, filename="<pipeline_udf>", mode="exec"), globals_dict, locals_dict)

    fn = locals_dict.get("transform") or globals_dict.get("transform")
    if not callable(fn):
        raise PipelineUdfError("UDF must define callable transform(row)")

    def _wrapped(row: Dict[str, Any]) -> Dict[str, Any]:
        out = fn(dict(row))
        if not isinstance(out, dict):
            raise PipelineUdfError("UDF transform(row) must return a dict")
        return out

    return _wrapped
