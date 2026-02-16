from __future__ import annotations

import ast
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.services.core.service_factory import ServiceInfo, create_fastapi_service


_STATUS_KEY = "status"
_STATUS_ERROR = "error"
_ALLOWLIST = {
    Path("shared") / "errors" / "error_envelope.py",
}


def _collect_status_error_literals(*, backend_dir: Path) -> dict[str, list[str]]:
    occurrences: dict[str, list[str]] = {}

    for path in backend_dir.rglob("*.py"):
        if "tests" in path.parts:
            continue

        rel_path = path.relative_to(backend_dir)
        if rel_path in _ALLOWLIST:
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
                if not (isinstance(raw_key, ast.Constant) and raw_key.value == _STATUS_KEY):
                    continue
                if not (isinstance(raw_value, ast.Constant) and raw_value.value == _STATUS_ERROR):
                    continue
                occurrences.setdefault(str(rel_path), []).append(f"{path}:{node.lineno}")

    return occurrences


@pytest.mark.unit
def test_no_direct_status_error_dicts() -> None:
    backend_dir = Path(__file__).resolve().parents[3]
    occurrences = _collect_status_error_literals(backend_dir=backend_dir)
    if not occurrences:
        return

    lines = [
        "Direct status='error' dict literals found. Use build_error_envelope() instead:",
    ]
    for rel_path in sorted(occurrences):
        refs = ", ".join(occurrences[rel_path][:3])
        suffix = " ..." if len(occurrences[rel_path]) > 3 else ""
        lines.append(f"- {rel_path}: {refs}{suffix}")
    raise AssertionError("\n".join(lines))


@pytest.mark.unit
def test_error_response_contains_enterprise_metadata() -> None:
    service_info = ServiceInfo(
        name="bff",
        title="Test Service",
        description="Test Service for enterprise error envelope",
    )
    app: FastAPI = create_fastapi_service(
        service_info=service_info,
        include_health_check=False,
        include_logging_middleware=False,
    )

    @app.get("/boom")
    def boom() -> None:
        raise ValueError("kaboom")

    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/boom")
    payload = resp.json()

    assert resp.status_code == 500
    assert payload["status"] == "error"
    assert payload["code"] == "INTERNAL_ERROR"
    assert payload["enterprise"]["code"].startswith("SHV-")
    assert payload["diagnostics"]["schema"] == "error_diagnostics.v1"
    assert payload["diagnostics"]["lookup"]["enterprise_code"] == payload["enterprise"]["code"]
    assert payload["diagnostics"]["group_fingerprint"].startswith("sha256:")
    assert payload["diagnostics"]["instance_fingerprint"].startswith("sha256:")


@pytest.mark.unit
def test_classified_http_exception_preserves_enterprise_and_external_codes() -> None:
    service_info = ServiceInfo(
        name="bff",
        title="Test Service",
        description="Test Service for external code propagation",
    )
    app: FastAPI = create_fastapi_service(
        service_info=service_info,
        include_health_check=False,
        include_logging_middleware=False,
    )

    @app.get("/mapping-error")
    def mapping_error() -> None:
        raise classified_http_exception(
            400,
            "mapping invalid",
            code=ErrorCode.OBJECTIFY_MAPPING_ERROR,
            external_code="MAPPING_SPEC_SOURCE_MISSING",
            extra={"missing_sources": ["missing"]},
        )

    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/mapping-error")
    payload = resp.json()

    assert resp.status_code == 400
    assert payload["code"] == "OBJECTIFY_MAPPING_ERROR"
    assert payload["enterprise"]["external_code"] == "MAPPING_SPEC_SOURCE_MISSING"
    assert payload["message"] == "mapping invalid"
    detail = payload["detail"] if isinstance(payload.get("detail"), dict) else {}
    assert detail.get("missing_sources") == ["missing"]
