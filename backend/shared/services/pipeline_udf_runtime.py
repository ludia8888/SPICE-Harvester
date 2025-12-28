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

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            raise PipelineUdfError("UDF code cannot import modules")

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

