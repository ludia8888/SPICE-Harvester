from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

_CODE_KEYS = {"code", "error", "error_code", "api_code", "external_code"}
_CODE_LIKE = re.compile(r"^[A-Z][A-Z0-9_]+$")


def _extract_enum_values(path: Path, *, class_name: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    values: set[str] = set()

    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name != class_name:
            continue
        for stmt in node.body:
            if not isinstance(stmt, ast.Assign) or len(stmt.targets) != 1:
                continue
            target = stmt.targets[0]
            if not isinstance(target, ast.Name):
                continue
            value = stmt.value
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                values.add(value.value)

    return values


def _extract_dict_literal_keys(path: Path, *, var_name: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    keys: set[str] = set()

    for node in tree.body:
        dict_node = None

        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == var_name:
            dict_node = node.value
        elif isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == var_name for t in node.targets
        ):
            dict_node = node.value

        if not isinstance(dict_node, ast.Dict):
            continue

        for key in dict_node.keys:
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                keys.add(key.value)

    return keys


def _collect_code_like_literals(*, backend_dir: Path) -> dict[str, list[str]]:
    occurrences: dict[str, list[str]] = {}

    for path in backend_dir.rglob("*.py"):
        if "tests" in path.parts:
            continue

        try:
            source = path.read_text(encoding="utf-8")
        except Exception:
            continue

        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            for raw_key, raw_value in zip(node.keys, node.values):
                if not (isinstance(raw_key, ast.Constant) and raw_key.value in _CODE_KEYS):
                    continue
                if not (isinstance(raw_value, ast.Constant) and isinstance(raw_value.value, str)):
                    continue
                value = raw_value.value
                if not _CODE_LIKE.match(value):
                    continue
                occurrences.setdefault(value, []).append(f"{path}:{node.lineno}")

    return occurrences


@pytest.mark.unit
def test_error_taxonomy_covers_all_code_like_literals() -> None:
    backend_dir = Path(__file__).resolve().parents[3]

    error_code_values = _extract_enum_values(
        backend_dir / "shared" / "errors" / "error_types.py",
        class_name="ErrorCode",
    )
    external_enum_values = _extract_enum_values(
        backend_dir / "shared" / "errors" / "external_codes.py",
        class_name="ExternalErrorCode",
    )
    from shared.errors import enterprise_catalog as catalog_module

    external_codes = {
        key.value if hasattr(key, "value") else str(key) for key in catalog_module._EXTERNAL_CODE_SPECS.keys()
    }
    objectify_codes = set(catalog_module._OBJECTIFY_ERROR_SPECS.keys())
    allowed = error_code_values | external_enum_values | external_codes | objectify_codes

    occurrences = _collect_code_like_literals(backend_dir=backend_dir)
    missing = {code: refs for code, refs in occurrences.items() if code not in allowed}

    if not missing:
        return

    lines = ["Unmapped code-like error literals found (add to enterprise_catalog or use ErrorCode):"]
    for code in sorted(missing):
        refs = missing[code]
        sample = ", ".join(refs[:3])
        suffix = " ..." if len(refs) > 3 else ""
        lines.append(f"- {code}: {sample}{suffix}")
    raise AssertionError("\n".join(lines))
