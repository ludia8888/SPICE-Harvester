from __future__ import annotations

from pathlib import Path

from bff.main import app as bff_app
from oms.main import app as oms_app


def _iter_branch_defaults(openapi: dict) -> list[tuple[str, str, str, str]]:
    violations: list[tuple[str, str, str, str]] = []
    for path, methods in (openapi.get("paths") or {}).items():
        for method, operation in (methods or {}).items():
            params = operation.get("parameters") or []
            for param in params:
                name = str(param.get("name") or "")
                if "branch" not in name.lower():
                    continue
                schema = param.get("schema") or {}
                default = schema.get("default")
                if isinstance(default, str) and default.strip().lower() == "master":
                    violations.append((method.upper(), path, name, default))
    return violations


def test_openapi_branch_defaults_do_not_use_master() -> None:
    violations = _iter_branch_defaults(bff_app.openapi()) + _iter_branch_defaults(oms_app.openapi())
    assert not violations, f"legacy master default detected in OpenAPI: {violations}"


def test_runtime_code_has_no_branch_master_default_literals() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    backend_root = repo_root / "backend"
    violations: list[str] = []

    for path in backend_root.rglob("*.py"):
        relative = path.relative_to(repo_root).as_posix()
        if relative.startswith("backend/tests/"):
            continue
        if "/migrations/" in relative:
            continue
        if relative == "backend/scripts/migrate_branch_master_to_main.py":
            continue
        text = path.read_text(encoding="utf-8")
        if 'Query("master"' in text or "Query('master'" in text:
            violations.append(relative)
        if 'default="master"' in text or "default='master'" in text:
            violations.append(relative)
        if 'or "master"' in text or "or 'master'" in text:
            violations.append(relative)

    assert not violations, f"legacy runtime branch literal detected: {sorted(set(violations))}"
