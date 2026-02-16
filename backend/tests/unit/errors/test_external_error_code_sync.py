from __future__ import annotations

from pathlib import Path

import pytest


def _extract_enum_values(path: Path, *, class_name: str) -> set[str]:
    import ast

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


@pytest.mark.unit
def test_external_error_code_enum_is_registered_in_catalog() -> None:
    backend_dir = Path(__file__).resolve().parents[3]
    enum_values = _extract_enum_values(
        backend_dir / "shared" / "errors" / "external_codes.py",
        class_name="ExternalErrorCode",
    )
    from shared.errors import enterprise_catalog as catalog_module

    catalog_external_codes = {
        key.value if hasattr(key, "value") else str(key) for key in catalog_module._EXTERNAL_CODE_SPECS.keys()
    }
    catalog_objectify_codes = set(catalog_module._OBJECTIFY_ERROR_SPECS.keys())
    catalog_values = catalog_external_codes | catalog_objectify_codes

    missing = sorted(enum_values - catalog_values)
    if not missing:
        return

    lines = ["ExternalErrorCode values missing from enterprise catalog code maps:"]
    lines.extend(f"- {code}" for code in missing)
    raise AssertionError("\n".join(lines))
