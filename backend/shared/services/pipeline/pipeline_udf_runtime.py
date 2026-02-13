from __future__ import annotations

import ast
import hashlib
from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, MutableMapping, Optional, Protocol


class PipelineUdfError(ValueError):
    pass


class PipelineUdfRegistry(Protocol):
    async def get_udf_latest_version(self, *, udf_id: str) -> Any: ...

    async def get_udf_version(self, *, udf_id: str, version: int) -> Any: ...


@dataclass(frozen=True)
class ResolvedPipelineUdf:
    udf_id: Optional[str]
    version: Optional[int]
    code: str
    cache_key: str


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


def _code_hash(code: str) -> str:
    return hashlib.sha256(str(code).encode("utf-8")).hexdigest()[:12]


def build_udf_cache_key(*, udf_id: Optional[str], version: Optional[int], code: str) -> str:
    return f"{udf_id or 'inline'}:{version if version is not None else 'inline'}:{_code_hash(code)}"


def _parse_udf_version(value: Any) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = int(text)
    except Exception as exc:
        raise PipelineUdfError(f"Invalid udfVersion: {value}") from exc
    if parsed <= 0:
        raise PipelineUdfError(f"Invalid udfVersion: {value}")
    return parsed


async def resolve_udf_reference(
    *,
    metadata: Mapping[str, Any],
    pipeline_registry: Optional[PipelineUdfRegistry],
    require_reference: bool,
    code_cache: Optional[MutableMapping[str, str]] = None,
) -> ResolvedPipelineUdf:
    _ = require_reference
    udf_id = str(metadata.get("udfId") or metadata.get("udf_id") or "").strip() or None
    udf_code = str(metadata.get("udfCode") or metadata.get("udf_code") or "").strip() or None
    udf_version = _parse_udf_version(metadata.get("udfVersion") or metadata.get("udf_version"))

    if udf_code:
        raise PipelineUdfError("udfCode is not allowed; use udfId (+udfVersion)")
    if not udf_id:
        raise PipelineUdfError("udf requires udfId")

    if udf_id:
        if not pipeline_registry:
            raise PipelineUdfError("udf requires pipeline_registry to resolve udfId")

        resolved_version = udf_version
        if resolved_version is None:
            latest = await pipeline_registry.get_udf_latest_version(udf_id=udf_id)
            if not latest:
                raise PipelineUdfError("udf not found")
            resolved_version = int(getattr(latest, "version", 0) or 0)
            if resolved_version <= 0:
                raise PipelineUdfError("udf version not found")

        lookup_key = f"{udf_id}:{resolved_version}"
        resolved_code = (code_cache or {}).get(lookup_key)
        if not resolved_code:
            version_row = await pipeline_registry.get_udf_version(udf_id=udf_id, version=resolved_version)
            if not version_row:
                raise PipelineUdfError("udf version not found")
            resolved_code = str(getattr(version_row, "code", "") or "").strip()
            if not resolved_code:
                raise PipelineUdfError("udf version code is empty")
            if code_cache is not None:
                code_cache[lookup_key] = resolved_code

        return ResolvedPipelineUdf(
            udf_id=udf_id,
            version=resolved_version,
            code=resolved_code,
            cache_key=build_udf_cache_key(udf_id=udf_id, version=resolved_version, code=resolved_code),
        )
    raise PipelineUdfError("udf requires udfId")


def validate_udf_output_schema(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise PipelineUdfError("UDF transform(row) must return a dict")
    normalized: Dict[str, Any] = {}
    for key, value in payload.items():
        name = str(key or "").strip()
        if not name:
            raise PipelineUdfError("UDF output keys must be non-empty strings")
        normalized[name] = value
    return normalized


def compile_udf(code: str) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    return compile_row_udf(code)


def _normalize_udf_source(code: str) -> str:
    normalized = (code or "").strip()
    if not normalized:
        return normalized
    # Backward compatibility: some call paths still pass escaped newlines.
    if "\\n" in normalized and "\n" not in normalized:
        try:
            decoded = normalized.encode("utf-8").decode("unicode_escape")
        except UnicodeDecodeError:
            return normalized
        if decoded.strip():
            return decoded.strip()
    return normalized


def compile_row_udf(code: str) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    """
    Compile a Python UDF for row-level transforms.

    Contract:
    - user must define `transform(row)` where row is a dict and return a dict
    - imports are not allowed
    """

    code = _normalize_udf_source(code)
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
        return validate_udf_output_schema(out)

    return _wrapped
