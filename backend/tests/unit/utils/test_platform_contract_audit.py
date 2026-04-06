from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from scripts import platform_contract_audit as audit_module


@pytest.mark.unit
def test_platform_contract_audit_guard() -> None:
    backend_dir = Path(__file__).resolve().parents[3]
    repo_root = backend_dir.parent
    script_path = backend_dir / "scripts" / "platform_contract_audit.py"
    result = subprocess.run(
        [sys.executable, str(script_path), "--repo-root", str(repo_root)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        return

    output = (result.stdout or "") + ("\n" if result.stdout and result.stderr else "") + (result.stderr or "")
    raise AssertionError(f"Platform contract audit failed:\n{output}")


@pytest.mark.unit
def test_runtime_vocabulary_audit_flags_minified_nginx_health_payload(tmp_path: Path) -> None:
    backend_dir = tmp_path / "backend"
    backend_dir.mkdir()
    (backend_dir / "nginx.conf").write_text(
        'location /health { return 200 \'{"status":"healthy"}\'; }\n',
        encoding="utf-8",
    )
    (tmp_path / "docs").mkdir()

    violations = audit_module.audit_runtime_vocabulary(tmp_path)

    assert any("backend/nginx.conf" in violation for violation in violations)


@pytest.mark.unit
def test_runtime_vocabulary_audit_scans_docs_examples(tmp_path: Path) -> None:
    backend_dir = tmp_path / "backend"
    backend_dir.mkdir()
    (backend_dir / "placeholder.py").write_text("STATUS = 'ready'\n", encoding="utf-8")
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    (docs_dir / "example.md").write_text('{"status": "healthy"}\n', encoding="utf-8")

    violations = audit_module.audit_runtime_vocabulary(tmp_path)

    assert any("docs/example.md" in violation for violation in violations)


@pytest.mark.unit
def test_platform_contract_audit_flags_reintroduced_legacy_health_module(tmp_path: Path) -> None:
    legacy_file = tmp_path / "backend" / "shared" / "services" / "core" / "health_check.py"
    legacy_file.parent.mkdir(parents=True)
    legacy_file.write_text("class HealthCheckAggregator: ...\n", encoding="utf-8")

    violations = audit_module.audit_absent_runtime_files(tmp_path)

    assert any("health_check.py" in violation for violation in violations)


@pytest.mark.unit
def test_platform_contract_audit_flags_duplicate_surface_markers(tmp_path: Path) -> None:
    router_path = tmp_path / "backend" / "funnel" / "routers" / "type_inference_router.py"
    router_path.parent.mkdir(parents=True)
    router_path.write_text('@router.get("/health")\n', encoding="utf-8")

    lineage_router_path = tmp_path / "backend" / "bff" / "routers" / "lineage.py"
    lineage_router_path.parent.mkdir(parents=True)
    lineage_router_path.write_text("def _freshness_status(*args, **kwargs):\n    return None\n", encoding="utf-8")

    pipeline_execution_path = tmp_path / "backend" / "bff" / "services" / "pipeline_execution_service.py"
    pipeline_execution_path.parent.mkdir(parents=True)
    pipeline_execution_path.write_text("def _build_ontology_ref(branch):\n    return branch\n", encoding="utf-8")

    connector_path = tmp_path / "backend" / "shared" / "services" / "core" / "connector_ingest_service.py"
    connector_path.parent.mkdir(parents=True)
    connector_path.write_text("def _dataset_artifact_prefix(**kwargs):\n    return 'x'\n", encoding="utf-8")

    datasets_router_path = tmp_path / "backend" / "bff" / "routers" / "foundry_datasets_v2.py"
    datasets_router_path.parent.mkdir(parents=True, exist_ok=True)
    datasets_router_path.write_text("def _dataset_id_from_rid(value):\n    return value\n", encoding="utf-8")

    foundry_query_path = tmp_path / "backend" / "oms" / "routers" / "query.py"
    foundry_query_path.parent.mkdir(parents=True)
    foundry_query_path.write_text("def _foundry_error(*args, **kwargs):\n    return None\n", encoding="utf-8")

    registry_catalog_path = tmp_path / "backend" / "shared" / "services" / "registries" / "dataset_registry_catalog.py"
    registry_catalog_path.parent.mkdir(parents=True)
    registry_catalog_path.write_text("def _require_pool(registry):\n    return registry\n", encoding="utf-8")

    violations = audit_module.audit_duplicate_surfaces(tmp_path)

    assert any("type_inference_router.py" in violation for violation in violations)
    assert any("lineage.py" in violation for violation in violations)
    assert any("pipeline_execution_service.py" in violation for violation in violations)
    assert any("connector_ingest_service.py" in violation for violation in violations)
    assert any("foundry_datasets_v2.py" in violation for violation in violations)
    assert any("query.py" in violation for violation in violations)
    assert any("dataset_registry_catalog.py" in violation for violation in violations)


@pytest.mark.unit
def test_platform_contract_audit_flags_apiresponse_requests_import_surface(tmp_path: Path) -> None:
    service_file = tmp_path / "backend" / "bff" / "routers" / "example.py"
    service_file.parent.mkdir(parents=True)
    service_file.write_text(
        "from shared.models.requests import ApiResponse\n",
        encoding="utf-8",
    )

    violations = audit_module.audit_apiresponse_import_surface(tmp_path)

    assert any("example.py imports ApiResponse via requests.py" in violation for violation in violations)
