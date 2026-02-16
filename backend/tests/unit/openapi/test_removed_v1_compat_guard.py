from pathlib import Path

import pytest
from fastapi import FastAPI

from bff.routers import link_types, object_types, query


REMOVED_V1_OPERATIONS: tuple[tuple[str, str], ...] = (
    ("get", "/api/v1/databases/{db_name}/ontology/object-types"),
    ("get", "/api/v1/databases/{db_name}/ontology/object-types/{class_id}"),
    ("get", "/api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types"),
    (
        "get",
        "/api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types/{link_type_api_name}",
    ),
    ("post", "/api/v1/databases/{db_name}/query"),
)


def _build_schema() -> dict:
    app = FastAPI()
    app.include_router(object_types.router, prefix="/api/v1")
    app.include_router(link_types.router, prefix="/api/v1")
    app.include_router(query.router, prefix="/api/v1")
    return app.openapi()


@pytest.mark.unit
def test_removed_v1_compat_operations_absent_from_openapi() -> None:
    paths = _build_schema().get("paths", {})

    for method, path in REMOVED_V1_OPERATIONS:
        assert method not in (paths.get(path) or {})

    # Write operations that still exist on the same object-type paths must stay.
    assert "post" in (paths.get("/api/v1/databases/{db_name}/ontology/object-types") or {})
    assert "put" in (paths.get("/api/v1/databases/{db_name}/ontology/object-types/{class_id}") or {})
    assert "get" in (paths.get("/api/v1/databases/{db_name}/query/builder") or {})


@pytest.mark.unit
def test_removed_v1_query_path_not_allowlisted_for_agent_tools() -> None:
    allowlist_path = Path(__file__).resolve().parents[3] / "shared" / "policies" / "agent_tool_allowlist.json"
    allowlist_text = allowlist_path.read_text(encoding="utf-8")
    assert "/api/v1/databases/{db_name}/query" not in allowlist_text


@pytest.mark.unit
def test_migration_docs_synced_to_code_deleted_status() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    migration_text = (repo_root / "docs" / "FOUNDRY_V1_TO_V2_MIGRATION.md").read_text(encoding="utf-8").lower()
    checklist_text = (repo_root / "docs" / "FOUNDRY_ALIGNMENT_CHECKLIST.md").read_text(encoding="utf-8").lower()

    assert "code deleted" in migration_text
    assert "code-deleted" in checklist_text
