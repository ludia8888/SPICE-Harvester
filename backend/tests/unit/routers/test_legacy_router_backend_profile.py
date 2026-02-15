from __future__ import annotations

from bff.main import app


def test_removed_legacy_ontology_merge_routes_not_mounted() -> None:
    paths = set((app.openapi() or {}).get("paths", {}).keys())
    assert "/api/v1/databases/{db_name}/ontology/branches" not in paths
    assert "/api/v1/databases/{db_name}/merge/simulate" not in paths
    assert "/api/v1/databases/{db_name}/merge/resolve" not in paths
