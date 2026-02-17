from fastapi.testclient import TestClient

from bff.main import app


LEGACY_REMOVED_PATHS = (
    "/api/v1/databases/testdb/suggest-schema-from-data",
    "/api/v1/databases/testdb/suggest-mappings",
    "/api/v1/databases/testdb/suggest-mappings-from-google-sheets",
    "/api/v1/databases/testdb/suggest-mappings-from-excel",
    "/api/v1/databases/testdb/suggest-schema-from-google-sheets",
    "/api/v1/databases/testdb/suggest-schema-from-excel",
    "/api/v1/databases/testdb/import-from-google-sheets/dry-run",
    "/api/v1/databases/testdb/import-from-google-sheets/commit",
    "/api/v1/databases/testdb/import-from-excel/dry-run",
    "/api/v1/databases/testdb/import-from-excel/commit",
)


def test_legacy_suggest_and_import_routes_not_registered() -> None:
    client = TestClient(app)
    for path in LEGACY_REMOVED_PATHS:
        response = client.post(path, json={})
        assert response.status_code == 404
