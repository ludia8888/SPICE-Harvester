from __future__ import annotations

import ast
from pathlib import Path

import pytest


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


@pytest.mark.unit
def test_legacy_error_code_enum_is_registered_in_catalog() -> None:
    backend_dir = Path(__file__).resolve().parents[3]
    enum_values = _extract_enum_values(
        backend_dir / "shared" / "errors" / "legacy_codes.py",
        class_name="LegacyErrorCode",
    )
    legacy_codes = _extract_dict_literal_keys(
        backend_dir / "shared" / "errors" / "enterprise_catalog.py",
        var_name="_LEGACY_CODE_SPECS",
    )
    external_codes = _extract_dict_literal_keys(
        backend_dir / "shared" / "errors" / "enterprise_catalog.py",
        var_name="_EXTERNAL_CODE_SPECS",
    )
    objectify_codes = _extract_dict_literal_keys(
        backend_dir / "shared" / "errors" / "enterprise_catalog.py",
        var_name="_OBJECTIFY_ERROR_SPECS",
    )
    catalog_values = legacy_codes | external_codes | objectify_codes

    missing = sorted(enum_values - catalog_values)
    if not missing:
        return

    lines = ["LegacyErrorCode values missing from enterprise catalog code maps:"]
    lines.extend(f"- {code}" for code in missing)
    raise AssertionError("\n".join(lines))
