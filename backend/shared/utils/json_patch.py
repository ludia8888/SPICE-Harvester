"""
Minimal JSON Patch (RFC6902-ish) applier for control-plane artifacts.

We intentionally support only a small subset of operations required by
server-generated patch proposals:
- add
- replace
- remove

This avoids introducing a new dependency and keeps the surface auditable.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Iterable, Mapping


class JsonPatchError(ValueError):
    pass


def _decode_pointer_token(token: str) -> str:
    return token.replace("~1", "/").replace("~0", "~")


def _iter_pointer(path: str) -> list[str]:
    if not isinstance(path, str) or not path.startswith("/"):
        raise JsonPatchError("path must be a JSON Pointer starting with '/'")
    if path == "/":
        return []
    return [_decode_pointer_token(p) for p in path.lstrip("/").split("/")]


def _resolve_parent(doc: Any, tokens: list[str]) -> tuple[Any, str]:
    if not tokens:
        raise JsonPatchError("path must not be empty")
    current = doc
    for token in tokens[:-1]:
        if isinstance(current, list):
            if token == "-":
                raise JsonPatchError("'-' token is only valid for the final path segment")
            try:
                idx = int(token)
            except ValueError as exc:
                raise JsonPatchError(f"invalid list index: {token}") from exc
            try:
                current = current[idx]
            except IndexError as exc:
                raise JsonPatchError(f"list index out of range: {idx}") from exc
            continue
        if isinstance(current, dict):
            if token not in current:
                raise JsonPatchError(f"missing object key: {token}")
            current = current[token]
            continue
        raise JsonPatchError(f"cannot traverse into {type(current).__name__}")
    return current, tokens[-1]


def apply_json_patch(document: Any, operations: Iterable[Mapping[str, Any]]) -> Any:
    doc = deepcopy(document)
    for op in operations:
        operation = str(op.get("op") or "").strip().lower()
        if operation not in {"add", "replace", "remove"}:
            raise JsonPatchError(f"unsupported op: {operation}")
        path = op.get("path")
        tokens = _iter_pointer(str(path))
        parent, key = _resolve_parent(doc, tokens)

        if isinstance(parent, list):
            if key == "-":
                idx = len(parent)
            else:
                try:
                    idx = int(key)
                except ValueError as exc:
                    raise JsonPatchError(f"invalid list index: {key}") from exc
            if operation == "add":
                value = op.get("value")
                if idx < 0 or idx > len(parent):
                    raise JsonPatchError(f"list index out of range for add: {idx}")
                parent.insert(idx, value)
                continue
            if operation == "replace":
                value = op.get("value")
                try:
                    parent[idx] = value
                except IndexError as exc:
                    raise JsonPatchError(f"list index out of range for replace: {idx}") from exc
                continue
            if operation == "remove":
                try:
                    parent.pop(idx)
                except IndexError as exc:
                    raise JsonPatchError(f"list index out of range for remove: {idx}") from exc
                continue

        if isinstance(parent, dict):
            if operation == "add":
                parent[key] = op.get("value")
                continue
            if operation == "replace":
                if key not in parent:
                    raise JsonPatchError(f"missing object key for replace: {key}")
                parent[key] = op.get("value")
                continue
            if operation == "remove":
                if key not in parent:
                    raise JsonPatchError(f"missing object key for remove: {key}")
                del parent[key]
                continue

        raise JsonPatchError(f"cannot apply op at parent type {type(parent).__name__}")

    return doc

