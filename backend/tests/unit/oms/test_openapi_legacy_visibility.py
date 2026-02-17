from __future__ import annotations

from oms.main import app


def test_oms_legacy_routes_removed_from_openapi() -> None:
    paths = set((app.openapi() or {}).get("paths", {}).keys())
    foundry_database_paths = {
        "/api/v1/database/list",
        "/api/v1/database/create",
        "/api/v1/database/exists/{db_name}",
    }
    removed_legacy_paths = {
        "/api/v1/branch/{db_name}/list",
        "/api/v1/version/{db_name}/head",
        "/api/v1/version/{db_name}/history",
        "/api/v1/version/{db_name}/diff",
        "/api/v1/database/{db_name}/ontology/branches",
        "/api/v1/actions/{db_name}/async/{action_type_id}/submit",
        "/api/v1/actions/{db_name}/async/{action_type_id}/submit-batch",
        "/api/v1/actions/{db_name}/async/{action_type_id}/simulate",
        "/api/v1/actions/{db_name}/async/logs/{action_log_id}/undo",
    }

    for path in removed_legacy_paths:
        assert path not in paths

    for path in foundry_database_paths:
        assert path in paths
