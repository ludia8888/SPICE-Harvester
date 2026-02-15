from __future__ import annotations

from bff.main import app


def test_removed_legacy_routes_not_exposed_in_openapi() -> None:
    paths = set((app.openapi() or {}).get("paths", {}).keys())
    removed_legacy_paths = {
        "/api/v1/databases/{db_name}/branches",
        "/api/v1/databases/{db_name}/branches/{branch_name}",
        "/api/v1/databases/{db_name}/versions",
        "/api/v1/databases/{db_name}/ontology/branches",
        "/api/v1/databases/{db_name}/merge/simulate",
        "/api/v1/databases/{db_name}/merge/resolve",
    }
    for path in removed_legacy_paths:
        assert path not in paths
