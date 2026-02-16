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

_BROKEN_OFFICIAL_DOC_URLS: tuple[str, ...] = (
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-objects/list-linked-objects",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-objects/get-linked-object",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-queries/search-json-query",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-objects/search-json-query",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontologies/get-full-metadata",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-metadata/list-interface-types",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-metadata/list-value-types",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/interface-types/list-interface-types",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/interface-types/get-interface-type",
)

_CURRENT_OFFICIAL_DOC_URLS: tuple[str, ...] = (
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/linked-objects/list-linked-objects",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/linked-objects/get-linked-object",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-objects/search-objects",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontologies/get-ontology-full-metadata",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-interfaces/list-interface-types",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-interfaces/get-interface-type",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/list-action-types",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/get-action-type",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/get-action-type-by-rid",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/query-types/list-query-types",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/query-types/get-query-type",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-value-types/list-ontology-value-types",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-value-types/get-ontology-value-type",
)


def _build_schema() -> dict:
    app = FastAPI()
    app.include_router(object_types.router, prefix="/api/v1")
    app.include_router(link_types.router, prefix="/api/v1")
    app.include_router(query.router, prefix="/api/v1")
    return app.openapi()


def _param_names(paths: dict, *, path: str, method: str = "get") -> set[str]:
    operation = (paths.get(path) or {}).get(method) or {}
    parameters = operation.get("parameters") if isinstance(operation, dict) else None
    if not isinstance(parameters, list):
        return set()
    names: set[str] = set()
    for parameter in parameters:
        if not isinstance(parameter, dict):
            continue
        name = str(parameter.get("name") or "").strip()
        if name:
            names.add(name)
    return names


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
def test_removed_v1_compat_operations_absent_from_full_bff_openapi() -> None:
    from bff.main import app as bff_app

    paths = (bff_app.openapi() or {}).get("paths", {})
    for method, path in REMOVED_V1_OPERATIONS:
        assert method not in (paths.get(path) or {})

    assert "post" in (paths.get("/api/v1/databases/{db_name}/ontology/object-types") or {})
    assert "put" in (paths.get("/api/v1/databases/{db_name}/ontology/object-types/{class_id}") or {})
    assert "get" in (paths.get("/api/v1/databases/{db_name}/query/builder") or {})


@pytest.mark.unit
def test_foundry_v2_openapi_parameter_surface_matches_official_docs_contract() -> None:
    from bff.main import app as bff_app

    paths = (bff_app.openapi() or {}).get("paths", {})

    full_metadata_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontology}/fullMetadata",
    )
    assert {"branch"} <= full_metadata_params
    assert "preview" not in full_metadata_params
    assert "sdkPackageRid" not in full_metadata_params
    assert "sdkVersion" not in full_metadata_params

    action_types_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontology}/actionTypes",
    )
    assert {"pageSize", "pageToken", "branch"} <= action_types_params
    assert "preview" not in action_types_params
    assert "sdkPackageRid" not in action_types_params
    assert "sdkVersion" not in action_types_params

    action_type_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontology}/actionTypes/{actionType}",
    )
    assert {"branch"} <= action_type_params
    assert "pageSize" not in action_type_params
    assert "pageToken" not in action_type_params
    assert "preview" not in action_type_params
    assert "sdkPackageRid" not in action_type_params
    assert "sdkVersion" not in action_type_params

    action_type_by_rid_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontology}/actionTypes/byRid/{actionTypeRid}",
    )
    assert {"branch"} <= action_type_by_rid_params
    assert "pageSize" not in action_type_by_rid_params
    assert "pageToken" not in action_type_by_rid_params
    assert "preview" not in action_type_by_rid_params
    assert "sdkPackageRid" not in action_type_by_rid_params
    assert "sdkVersion" not in action_type_by_rid_params

    interface_types_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontology}/interfaceTypes",
    )
    assert {"preview", "pageSize", "pageToken", "branch"} <= interface_types_params

    interface_type_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontology}/interfaceTypes/{interfaceType}",
    )
    assert {"preview", "branch", "sdkPackageRid", "sdkVersion"} <= interface_type_params

    value_types_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontology}/valueTypes",
    )
    assert "preview" in value_types_params
    assert "pageSize" not in value_types_params
    assert "pageToken" not in value_types_params
    assert "branch" not in value_types_params

    query_types_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontology}/queryTypes",
    )
    assert {"pageSize", "pageToken"} <= query_types_params
    assert "branch" not in query_types_params
    assert "preview" not in query_types_params
    assert "sdkPackageRid" not in query_types_params
    assert "sdkVersion" not in query_types_params

    query_type_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontology}/queryTypes/{queryApiName}",
    )
    assert {"version", "sdkPackageRid", "sdkVersion"} <= query_type_params

    object_type_full_metadata_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontology}/objectTypes/{objectType}/fullMetadata",
    )
    assert {"branch", "preview", "sdkPackageRid", "sdkVersion"} <= object_type_full_metadata_params


@pytest.mark.unit
def test_removed_v1_query_path_not_allowlisted_for_agent_tools() -> None:
    allowlist_path = Path(__file__).resolve().parents[3] / "shared" / "policies" / "agent_tool_allowlist.json"
    allowlist_text = allowlist_path.read_text(encoding="utf-8")
    assert "/api/v1/databases/{db_name}/query" not in allowlist_text


@pytest.mark.unit
def test_removed_v1_compat_path_literals_absent_from_runtime_code() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    runtime_roots = [backend_root / "bff", backend_root / "oms"]
    removed_path_literals = [
        "/api/v1/databases/{db_name}/ontology/object-types",
        "/api/v1/databases/{db_name}/ontology/object-types/{class_id}",
        "/api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types",
        "/api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types/{link_type_api_name}",
        "/api/v1/databases/{db_name}/query",
    ]

    hits: list[tuple[str, str]] = []
    for root in runtime_roots:
        for path in root.rglob("*.py"):
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            for needle in removed_path_literals:
                if needle in text:
                    hits.append((str(path.resolve()), needle))

    assert not hits


@pytest.mark.unit
def test_removed_bff_legacy_branch_and_merge_path_literals_absent_from_runtime_code() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    runtime_roots = [backend_root / "bff", backend_root / "oms"]
    removed_path_literals = [
        "/api/v1/databases/{db_name}/branches",
        "/api/v1/databases/{db_name}/branches/{branch_name}",
        "/api/v1/databases/{db_name}/ontology/branches",
        "/api/v1/databases/{db_name}/merge/simulate",
        "/api/v1/databases/{db_name}/merge/resolve",
    ]

    hits: list[tuple[str, str]] = []
    for root in runtime_roots:
        for path in root.rglob("*.py"):
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            for needle in removed_path_literals:
                if needle in text:
                    hits.append((str(path.resolve()), needle))

    assert not hits


@pytest.mark.unit
def test_migration_docs_synced_to_code_deleted_status() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    migration_text = (repo_root / "docs" / "FOUNDRY_V1_TO_V2_MIGRATION.md").read_text(encoding="utf-8").lower()
    checklist_text = (repo_root / "docs" / "FOUNDRY_ALIGNMENT_CHECKLIST.md").read_text(encoding="utf-8").lower()

    assert "code deleted" in migration_text
    assert "code-deleted" in checklist_text


@pytest.mark.unit
def test_foundry_reference_docs_use_current_urls() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    migration_text = (repo_root / "docs" / "FOUNDRY_V1_TO_V2_MIGRATION.md").read_text(encoding="utf-8")
    checklist_text = (repo_root / "docs" / "FOUNDRY_ALIGNMENT_CHECKLIST.md").read_text(encoding="utf-8")
    merged = f"{migration_text}\n{checklist_text}"

    for url in _BROKEN_OFFICIAL_DOC_URLS:
        assert url not in merged

    for url in _CURRENT_OFFICIAL_DOC_URLS:
        assert url in merged


@pytest.mark.unit
def test_legacy_version_head_endpoint_literal_is_only_in_visibility_guard() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    expected_only_file = backend_root / "tests" / "unit" / "oms" / "test_openapi_legacy_visibility.py"
    this_file = Path(__file__).resolve()
    needle = "/api/v1/version/{db_name}/head"

    hit_files: list[Path] = []
    for path in backend_root.rglob("*.py"):
        if path.resolve() == this_file:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if needle in text:
            hit_files.append(path.resolve())

    assert hit_files == [expected_only_file.resolve()]


@pytest.mark.unit
def test_legacy_branch_endpoint_literals_are_only_in_visibility_guard() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    expected_only_file = backend_root / "tests" / "unit" / "oms" / "test_openapi_legacy_visibility.py"
    this_file = Path(__file__).resolve()
    needle = "/api/v1/branch/"

    hit_files: list[Path] = []
    for path in backend_root.rglob("*.py"):
        if path.resolve() == this_file:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if needle in text:
            hit_files.append(path.resolve())

    assert hit_files == [expected_only_file.resolve()]


@pytest.mark.unit
@pytest.mark.parametrize(
    "needle",
    [
        "/api/v1/version/{db_name}/history",
        "/api/v1/version/{db_name}/diff",
    ],
)
def test_legacy_version_history_diff_literals_are_only_in_visibility_guard(needle: str) -> None:
    backend_root = Path(__file__).resolve().parents[3]
    expected_files = {
        (backend_root / "tests" / "unit" / "oms" / "test_openapi_legacy_visibility.py").resolve(),
        (backend_root / "tests" / "test_oms_smoke.py").resolve(),
    }
    this_file = Path(__file__).resolve()

    hit_files: list[Path] = []
    for path in backend_root.rglob("*.py"):
        resolved = path.resolve()
        if resolved == this_file:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if needle in text:
            hit_files.append(resolved)

    assert set(hit_files) == expected_files
