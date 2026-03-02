import re
from pathlib import Path

import pytest
from fastapi import FastAPI

from bff.routers import ontology_extensions


REMOVED_V1_OPERATIONS: tuple[tuple[str, str], ...] = (
    ("get", "/api/v1/databases/{db_name}/ontology/object-types"),
    ("get", "/api/v1/databases/{db_name}/ontology/object-types/{class_id}"),
    ("get", "/api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types"),
    (
        "get",
        "/api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types/{link_type_api_name}",
    ),
    ("post", "/api/v1/databases/{db_name}/ontology/object-types"),
    ("put", "/api/v1/databases/{db_name}/ontology/object-types/{class_id}"),
    ("post", "/api/v1/databases/{db_name}/ontology/validate"),
    ("get", "/api/v1/databases/{db_name}/ontology/{class_id}/schema"),
    ("post", "/api/v1/databases/{db_name}/ontology/{class_id}/mapping-metadata"),
    ("get", "/api/v1/databases/{db_name}/ontology/proposals"),
    ("post", "/api/v1/databases/{db_name}/ontology/proposals"),
    ("post", "/api/v1/databases/{db_name}/ontology/proposals/{proposal_id}/approve"),
    ("post", "/api/v1/databases/{db_name}/ontology/deploy"),
    ("get", "/api/v1/databases/{db_name}/ontology/health"),
    ("post", "/api/v1/databases/{db_name}/query"),
    ("get", "/api/v1/databases/{db_name}/query/builder"),
    ("post", "/api/v1/databases/{db_name}/actions/{action_type_id}/simulate"),
    ("post", "/api/v1/databases/{db_name}/actions/{action_type_id}/submit"),
    ("post", "/api/v1/databases/{db_name}/actions/{action_type_id}/submit-batch"),
    ("post", "/api/v1/databases/{db_name}/actions/logs/{action_log_id}/undo"),
    ("get", "/api/v1/databases/{db_name}/actions/logs"),
    ("get", "/api/v1/databases/{db_name}/actions/logs/{action_log_id}"),
    ("get", "/api/v1/databases/{db_name}/actions/simulations"),
    ("get", "/api/v1/databases/{db_name}/actions/simulations/{simulation_id}"),
    ("get", "/api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions"),
    ("get", "/api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions/{version}"),
    ("get", "/api/v1/databases/{db_name}/ontology/link-types"),
    ("get", "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}"),
    ("post", "/api/v1/databases/{db_name}/ontology/link-types"),
    ("put", "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}"),
    ("get", "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits"),
    ("post", "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits"),
    ("post", "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/reindex"),
    ("get", "/api/v1/databases/{db_name}/ontology/action-types"),
    ("get", "/api/v1/databases/{db_name}/ontology/action-types/{resource_id}"),
    ("post", "/api/v1/databases/{db_name}/ontology/action-types"),
    ("put", "/api/v1/databases/{db_name}/ontology/action-types/{resource_id}"),
    ("delete", "/api/v1/databases/{db_name}/ontology/action-types/{resource_id}"),
    ("get", "/api/v1/databases/{db_name}/ontology/functions"),
    ("get", "/api/v1/databases/{db_name}/ontology/functions/{resource_id}"),
    ("post", "/api/v1/databases/{db_name}/ontology/functions"),
    ("put", "/api/v1/databases/{db_name}/ontology/functions/{resource_id}"),
    ("delete", "/api/v1/databases/{db_name}/ontology/functions/{resource_id}"),
    ("get", "/api/v1/databases/{db_name}/ontology/groups"),
    ("post", "/api/v1/databases/{db_name}/ontology/groups"),
    ("get", "/api/v1/databases/{db_name}/ontology/groups/{resource_id}"),
    ("put", "/api/v1/databases/{db_name}/ontology/groups/{resource_id}"),
    ("delete", "/api/v1/databases/{db_name}/ontology/groups/{resource_id}"),
    ("get", "/api/v1/databases/{db_name}/ontology/interfaces"),
    ("get", "/api/v1/databases/{db_name}/ontology/interfaces/{resource_id}"),
    ("post", "/api/v1/databases/{db_name}/ontology/interfaces"),
    ("put", "/api/v1/databases/{db_name}/ontology/interfaces/{resource_id}"),
    ("delete", "/api/v1/databases/{db_name}/ontology/interfaces/{resource_id}"),
    ("get", "/api/v1/databases/{db_name}/ontology/shared-properties"),
    ("get", "/api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}"),
    ("post", "/api/v1/databases/{db_name}/ontology/shared-properties"),
    ("put", "/api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}"),
    ("delete", "/api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}"),
    ("get", "/api/v1/databases/{db_name}/ontology/value-types"),
    ("get", "/api/v1/databases/{db_name}/ontology/value-types/{resource_id}"),
    ("post", "/api/v1/databases/{db_name}/ontology/value-types"),
    ("put", "/api/v1/databases/{db_name}/ontology/value-types/{resource_id}"),
    ("delete", "/api/v1/databases/{db_name}/ontology/value-types/{resource_id}"),
    ("get", "/api/v1/databases/{db_name}/classes"),
    ("get", "/api/v1/databases/{db_name}/classes/{class_id}"),
    ("get", "/api/v1/databases/{db_name}/class/{class_id}/instances"),
    ("get", "/api/v1/databases/{db_name}/class/{class_id}/instance/{instance_id}"),
    ("post", "/api/v1/databases/{db_name}/classes"),
    ("get", "/api/v1/databases/{db_name}/class/{class_id}/sample-values"),
    ("post", "/api/v1/pipeline-plans/compile"),
    ("get", "/api/v1/pipeline-plans/{plan_id}"),
    ("post", "/api/v1/pipeline-plans/{plan_id}/preview"),
    ("post", "/api/v1/pipeline-plans/{plan_id}/inspect-preview"),
    ("post", "/api/v1/pipeline-plans/{plan_id}/evaluate-joins"),
    ("post", "/api/v1/pipelines/simulate-definition"),
    ("get", "/api/v1/pipelines/branches"),
    ("post", "/api/v1/pipelines/branches/{branch}/archive"),
    ("post", "/api/v1/pipelines/branches/{branch}/restore"),
    ("post", "/api/v1/pipelines/{pipeline_id}/branches"),
    ("post", "/api/v1/pipelines/datasets/{dataset_id}/versions/{version_id}/funnel-analysis"),
    ("post", "/api/v1/databases/{db_name}/suggest-schema-from-data"),
    ("post", "/api/v1/databases/{db_name}/suggest-mappings"),
    ("post", "/api/v1/databases/{db_name}/suggest-mappings-from-google-sheets"),
    ("post", "/api/v1/databases/{db_name}/suggest-mappings-from-excel"),
    ("post", "/api/v1/databases/{db_name}/suggest-schema-from-google-sheets"),
    ("post", "/api/v1/databases/{db_name}/suggest-schema-from-excel"),
    ("post", "/api/v1/databases/{db_name}/import-from-google-sheets/dry-run"),
    ("post", "/api/v1/databases/{db_name}/import-from-google-sheets/commit"),
    ("post", "/api/v1/databases/{db_name}/import-from-excel/dry-run"),
    ("post", "/api/v1/databases/{db_name}/import-from-excel/commit"),
    ("post", "/api/v1/data-connectors/google-sheets/grid"),
    ("post", "/api/v1/data-connectors/google-sheets/preview"),
    ("post", "/api/v1/data-connectors/google-sheets/register"),
    ("get", "/api/v1/data-connectors/google-sheets/registered"),
    ("get", "/api/v1/data-connectors/google-sheets/{sheet_id}/preview"),
    ("delete", "/api/v1/data-connectors/google-sheets/{sheet_id}"),
    ("post", "/api/v1/data-connectors/google-sheets/{sheet_id}/start-pipelining"),
    ("post", "/api/v1/data-connectors/google-sheets/oauth/start"),
    ("get", "/api/v1/data-connectors/google-sheets/oauth/callback"),
    ("get", "/api/v1/data-connectors/google-sheets/drive/spreadsheets"),
    ("get", "/api/v1/data-connectors/google-sheets/spreadsheets/{sheet_id}/worksheets"),
    ("delete", "/api/v1/data-connectors/google-sheets/connections/{connection_id}"),
    ("post", "/api/v1/backing-datasources"),
    ("get", "/api/v1/backing-datasources"),
    ("get", "/api/v1/backing-datasources/{backing_id}"),
    ("post", "/api/v1/backing-datasources/{backing_id}/versions"),
    ("get", "/api/v1/backing-datasources/{backing_id}/versions"),
    ("get", "/api/v1/backing-datasource-versions/{version_id}"),
    # Phase 1 v2 migration: dataset endpoints hidden (replaced by /api/v2/datasets/*)
    ("post", "/api/v1/pipelines/datasets"),
    ("post", "/api/v1/pipelines/datasets/{dataset_id}/versions"),
    ("post", "/api/v1/pipelines/datasets/csv-upload"),
    ("post", "/api/v1/pipelines/datasets/excel-upload"),
    ("post", "/api/v1/pipelines/datasets/media-upload"),
    ("get", "/api/v1/pipelines/datasets/ingest-requests/{ingest_request_id}"),
    ("post", "/api/v1/pipelines/datasets/ingest-requests/{ingest_request_id}/schema/approve"),
    # Phase 1 v2 migration: ontology create hidden (replaced by /api/v2/ontologies/*/objectTypes)
    ("post", "/api/v1/databases/{db_name}/ontology"),
    # Phase 2 v2 migration: pipeline execution deleted (replaced by /api/v2/orchestration/builds/*)
    ("post", "/api/v1/pipelines/{pipeline_id}/preview"),
    ("post", "/api/v1/pipelines/{pipeline_id}/build"),
    ("post", "/api/v1/pipelines/{pipeline_id}/deploy"),
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
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/object-types/list-outgoing-link-types",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/object-types/get-outgoing-link-type",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/load-object-set-objects",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/load-object-set-multiple-object-types",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/load-object-set-objects-or-interfaces",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/aggregate-object-set",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-object-sets/create-temporary-object-set/",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/list-action-types",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/get-action-type",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/get-action-type-by-rid",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/actions/action-basics/",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/actions/apply-action",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/actions/apply-action-batch",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/query-types/list-query-types",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/query-types/get-query-type",
    "https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/queries/execute-query",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-value-types/list-ontology-value-types",
    "https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-value-types/get-ontology-value-type",
)


def _build_schema() -> dict:
    app = FastAPI()
    app.include_router(ontology_extensions.router, prefix="/api/v1")
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


@pytest.mark.unit
def test_removed_v1_compat_operations_absent_from_full_bff_openapi() -> None:
    from bff.main import app as bff_app

    paths = (bff_app.openapi() or {}).get("paths", {})
    for method, path in REMOVED_V1_OPERATIONS:
        assert method not in (paths.get(path) or {})

    assert "post" not in (paths.get("/api/v1/databases/{db_name}/ontology/object-types") or {})
    assert "put" not in (paths.get("/api/v1/databases/{db_name}/ontology/object-types/{class_id}") or {})
    assert "post" not in (paths.get("/api/v1/databases/{db_name}/classes") or {})


@pytest.mark.unit
def test_foundry_v2_openapi_parameter_surface_matches_official_docs_contract() -> None:
    from bff.main import app as bff_app

    paths = (bff_app.openapi() or {}).get("paths", {})

    full_metadata_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/fullMetadata",
    )
    assert {"branch", "preview"} <= full_metadata_params
    assert "sdkPackageRid" not in full_metadata_params
    assert "sdkVersion" not in full_metadata_params

    action_types_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/actionTypes",
    )
    assert {"pageSize", "pageToken", "branch"} <= action_types_params
    assert "preview" not in action_types_params
    assert "sdkPackageRid" not in action_types_params
    assert "sdkVersion" not in action_types_params

    action_type_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/actionTypes/{actionTypeApiName}",
    )
    assert {"branch"} <= action_type_params
    assert "pageSize" not in action_type_params
    assert "pageToken" not in action_type_params
    assert "preview" not in action_type_params
    assert "sdkPackageRid" not in action_type_params
    assert "sdkVersion" not in action_type_params

    action_type_by_rid_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/actionTypes/byRid/{actionTypeRid}",
    )
    assert {"branch"} <= action_type_by_rid_params
    assert "pageSize" not in action_type_by_rid_params
    assert "pageToken" not in action_type_by_rid_params
    assert "preview" not in action_type_by_rid_params
    assert "sdkPackageRid" not in action_type_by_rid_params
    assert "sdkVersion" not in action_type_by_rid_params

    apply_action_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/actions/{actionApiName}/apply",
        method="post",
    )
    assert {"branch", "sdkPackageRid", "sdkVersion", "transactionId"} <= apply_action_params
    assert "preview" not in apply_action_params
    assert "validate" not in apply_action_params

    apply_action_batch_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/actions/{actionApiName}/applyBatch",
        method="post",
    )
    assert {"branch", "sdkPackageRid", "sdkVersion"} <= apply_action_batch_params
    assert "validate" not in apply_action_batch_params
    assert "preview" not in apply_action_batch_params

    # Foundry 공식 공개 surface 기준: action logs/simulations dedicated routes are not exposed.
    assert "post" not in (paths.get("/api/v2/ontologies/{ontologyRid}/actions/logs/{actionLogId}/undo") or {})
    assert "get" not in (paths.get("/api/v2/ontologies/{ontologyRid}/actions/logs") or {})
    assert "get" not in (paths.get("/api/v2/ontologies/{ontologyRid}/actions/logs/{actionLogId}") or {})
    assert "get" not in (paths.get("/api/v2/ontologies/{ontologyRid}/actions/simulations") or {})
    assert "get" not in (paths.get("/api/v2/ontologies/{ontologyRid}/actions/simulations/{simulationId}") or {})
    assert "get" not in (paths.get("/api/v2/ontologies/{ontologyRid}/actions/simulations/{simulationId}/versions") or {})
    assert "get" not in (
        paths.get("/api/v2/ontologies/{ontologyRid}/actions/simulations/{simulationId}/versions/{version}") or {}
    )

    interface_types_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/interfaceTypes",
    )
    assert {"preview", "pageSize", "pageToken", "branch"} <= interface_types_params

    interface_type_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/interfaceTypes/{interfaceTypeApiName}",
    )
    assert {"preview", "branch", "sdkPackageRid", "sdkVersion"} <= interface_type_params

    value_types_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/valueTypes",
    )
    assert "preview" in value_types_params
    assert "pageSize" not in value_types_params
    assert "pageToken" not in value_types_params
    assert "branch" not in value_types_params

    query_types_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/queryTypes",
    )
    assert {"pageSize", "pageToken"} <= query_types_params
    assert "branch" not in query_types_params
    assert "preview" not in query_types_params
    assert "sdkPackageRid" not in query_types_params
    assert "sdkVersion" not in query_types_params

    query_type_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/queryTypes/{queryApiName}",
    )
    assert {"version", "sdkPackageRid", "sdkVersion"} <= query_type_params

    query_execute_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/queries/{queryApiName}/execute",
        method="post",
    )
    assert {"version", "sdkPackageRid", "sdkVersion", "transactionId"} <= query_execute_params
    assert "branch" not in query_execute_params
    assert "preview" not in query_execute_params

    object_type_full_metadata_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/objectTypes/{objectTypeApiName}/fullMetadata",
    )
    assert {"branch", "preview", "sdkPackageRid", "sdkVersion"} <= object_type_full_metadata_params

    object_set_load_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/objectSets/loadObjects",
        method="post",
    )
    assert {"branch", "transactionId", "sdkPackageRid", "sdkVersion"} <= object_set_load_params
    assert "preview" not in object_set_load_params
    assert "pageSize" not in object_set_load_params
    assert "pageToken" not in object_set_load_params

    object_set_load_links_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/objectSets/loadLinks",
        method="post",
    )
    assert {"branch", "preview", "sdkPackageRid", "sdkVersion"} <= object_set_load_links_params
    assert "transactionId" not in object_set_load_links_params

    object_set_load_multiple_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/objectSets/loadObjectsMultipleObjectTypes",
        method="post",
    )
    assert {"branch", "preview", "transactionId", "sdkPackageRid", "sdkVersion"} <= object_set_load_multiple_params

    object_set_load_or_interfaces_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/objectSets/loadObjectsOrInterfaces",
        method="post",
    )
    assert {"branch", "preview", "sdkPackageRid", "sdkVersion"} <= object_set_load_or_interfaces_params
    assert "transactionId" not in object_set_load_or_interfaces_params

    object_set_aggregate_params = _param_names(
        paths,
        path="/api/v2/ontologies/{ontologyRid}/objectSets/aggregate",
        method="post",
    )
    assert {"branch", "transactionId", "sdkPackageRid", "sdkVersion"} <= object_set_aggregate_params


@pytest.mark.unit
def test_removed_v1_query_path_not_allowlisted_for_agent_tools() -> None:
    allowlist_path = Path(__file__).resolve().parents[3] / "shared" / "policies" / "agent_tool_allowlist.json"
    allowlist_text = allowlist_path.read_text(encoding="utf-8")
    assert "/api/v1/databases/{db_name}/ontology/object-types" not in allowlist_text
    assert "/api/v1/databases/{db_name}/ontology/object-types/{class_id}" not in allowlist_text
    assert "/api/v1/databases/{db_name}/query" not in allowlist_text
    assert "/api/v1/databases/{db_name}/actions/{action_type_id}/simulate" not in allowlist_text
    assert "/api/v1/databases/{db_name}/actions/{action_type_id}/submit-batch" not in allowlist_text
    assert "/api/v1/databases/{db_name}/actions/logs" not in allowlist_text
    assert "/api/v1/databases/{db_name}/actions/logs/{action_log_id}" not in allowlist_text
    assert "/api/v1/databases/{db_name}/ontology/link-types" not in allowlist_text
    assert "/api/v1/databases/{db_name}/ontology/action-types" not in allowlist_text
    assert "/api/v1/databases/{db_name}/ontology/functions" not in allowlist_text
    assert "/api/v1/databases/{db_name}/ontology/groups" not in allowlist_text
    assert "/api/v1/databases/{db_name}/ontology/interfaces" not in allowlist_text
    assert "/api/v1/databases/{db_name}/ontology/shared-properties" not in allowlist_text
    assert "/api/v1/databases/{db_name}/ontology/value-types" not in allowlist_text
    assert "/api/v1/databases/{db_name}/ontology/{class_id}/schema" not in allowlist_text
    assert "/api/v1/databases/{db_name}/ontology/{class_id}/mapping-metadata" not in allowlist_text
    assert "/api/v1/databases/{db_name}/ontology/validate" not in allowlist_text
    assert "/api/v1/databases/{db_name}/ontology/proposals" not in allowlist_text
    assert "/api/v1/databases/{db_name}/ontology/proposals/{proposal_id}/approve" not in allowlist_text
    assert "/api/v1/databases/{db_name}/ontology/deploy" not in allowlist_text
    assert "/api/v1/databases/{db_name}/ontology/health" not in allowlist_text
    assert "/api/v1/databases/{db_name}/classes" not in allowlist_text
    assert "/api/v1/databases/{db_name}/class/{class_id}/sample-values" not in allowlist_text
    assert "/api/v1/pipeline-plans/compile" not in allowlist_text
    assert "/api/v1/pipeline-plans/{plan_id}" not in allowlist_text
    assert "/api/v1/pipeline-plans/{plan_id}/preview" not in allowlist_text
    assert "/api/v1/pipeline-plans/{plan_id}/inspect-preview" not in allowlist_text
    assert "/api/v1/pipeline-plans/{plan_id}/evaluate-joins" not in allowlist_text
    assert "/api/v1/pipelines/simulate-definition" not in allowlist_text
    assert "/api/v1/pipelines/branches" not in allowlist_text
    assert "/api/v1/pipelines/branches/{branch}/archive" not in allowlist_text
    assert "/api/v1/pipelines/branches/{branch}/restore" not in allowlist_text
    assert "/api/v1/pipelines/{pipeline_id}/branches" not in allowlist_text
    assert "/api/v1/pipelines/datasets/{dataset_id}/versions/{version_id}/funnel-analysis" not in allowlist_text
    assert "/api/v1/databases/{db_name}/suggest-schema-from-data" not in allowlist_text
    assert "/api/v1/databases/{db_name}/suggest-mappings" not in allowlist_text
    assert "/api/v1/databases/{db_name}/suggest-mappings-from-google-sheets" not in allowlist_text
    assert "/api/v1/databases/{db_name}/suggest-mappings-from-excel" not in allowlist_text
    assert "/api/v1/databases/{db_name}/suggest-schema-from-google-sheets" not in allowlist_text
    assert "/api/v1/databases/{db_name}/suggest-schema-from-excel" not in allowlist_text
    assert "/api/v1/databases/{db_name}/import-from-google-sheets/dry-run" not in allowlist_text
    assert "/api/v1/databases/{db_name}/import-from-google-sheets/commit" not in allowlist_text
    assert "/api/v1/databases/{db_name}/import-from-excel/dry-run" not in allowlist_text
    assert "/api/v1/databases/{db_name}/import-from-excel/commit" not in allowlist_text


@pytest.mark.unit
def test_removed_v1_query_path_not_present_in_seed_allowlist_script() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    script_path = repo_root / "scripts" / "seed_agent_tool_allowlist.py"
    script_text = script_path.read_text(encoding="utf-8")
    assert "/api/v1/databases/{db_name}/query" not in script_text


@pytest.mark.unit
def test_agent_runtime_and_migrated_e2e_scripts_do_not_reference_v1_pipelines_prefix() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    targets = [
        repo_root / "backend" / "agent" / "services" / "agent_runtime.py",
        repo_root / "scripts" / "run_pipeline_artifact_e2e.sh",
        repo_root / "scripts" / "e2e_agent_pipeline_demo.sh",
    ]
    forbidden_prefixes = [
        "/api/v1/pipelines",
        "/pipelines/datasets/csv-upload",
    ]

    hits: list[tuple[str, str]] = []
    for path in targets:
        text = path.read_text(encoding="utf-8")
        for needle in forbidden_prefixes:
            if needle in text:
                hits.append((str(path.resolve()), needle))

    assert not hits


@pytest.mark.unit
def test_pipeline_e2e_suites_route_legacy_v1_calls_through_v2_adapter() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    targets = [
        repo_root / "backend" / "tests" / "test_pipeline_execution_semantics_e2e.py",
        repo_root / "backend" / "tests" / "test_pipeline_transform_cleansing_e2e.py",
        repo_root / "backend" / "tests" / "test_pipeline_streaming_semantics_e2e.py",
        repo_root / "backend" / "tests" / "test_pipeline_type_mismatch_guard_e2e.py",
        repo_root / "backend" / "tests" / "test_pipeline_objectify_es_e2e.py",
        repo_root / "backend" / "tests" / "test_foundry_e2e_qa.py",
        repo_root / "backend" / "tests" / "test_financial_investigation_workflow_e2e.py",
    ]

    missing_adapter: list[str] = []
    raw_async_client_contexts: list[str] = []
    for path in targets:
        text = path.read_text(encoding="utf-8")
        if "/api/v1/pipelines" not in text:
            continue
        if "PipelinesV2AdapterClient" not in text:
            missing_adapter.append(str(path.resolve()))
        if re.search(r"async with httpx\.AsyncClient\([^\n]*\) as client:", text):
            raw_async_client_contexts.append(str(path.resolve()))

    assert not missing_adapter
    assert not raw_async_client_contexts


@pytest.mark.unit
def test_removed_v1_compat_path_literals_absent_from_runtime_code() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    runtime_roots = [backend_root / "bff", backend_root / "oms"]
    removed_path_literals = [
        "/api/v1/databases/{db_name}/ontology/object-types",
        "/api/v1/databases/{db_name}/ontology/object-types/{class_id}",
        "/api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types",
        "/api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types/{link_type_api_name}",
        "/api/v1/databases/{db_name}/ontology/validate",
        "/api/v1/databases/{db_name}/query",
        "/api/v1/databases/{db_name}/query/builder",
        "/api/v1/databases/{db_name}/actions/{action_type_id}/simulate",
        "/api/v1/databases/{db_name}/actions/{action_type_id}/submit",
        "/api/v1/databases/{db_name}/actions/{action_type_id}/submit-batch",
        "/api/v1/databases/{db_name}/actions/logs/{action_log_id}/undo",
        "/api/v1/databases/{db_name}/actions/logs",
        "/api/v1/databases/{db_name}/actions/logs/{action_log_id}",
        "/api/v1/databases/{db_name}/actions/simulations",
        "/api/v1/databases/{db_name}/actions/simulations/{simulation_id}",
        "/api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions",
        "/api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions/{version}",
        "/api/v2/ontologies/{ontologyRid}/actions/logs/{actionLogId}/undo",
        "/api/v2/ontologies/{ontologyRid}/actions/logs",
        "/api/v2/ontologies/{ontologyRid}/actions/logs/{actionLogId}",
        "/api/v2/ontologies/{ontologyRid}/actions/simulations",
        "/api/v2/ontologies/{ontologyRid}/actions/simulations/{simulationId}",
        "/api/v2/ontologies/{ontologyRid}/actions/simulations/{simulationId}/versions",
        "/api/v2/ontologies/{ontologyRid}/actions/simulations/{simulationId}/versions/{version}",
        "/api/v1/databases/{db_name}/ontology/link-types",
        "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}",
        "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits",
        "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/reindex",
        "/api/v1/databases/{db_name}/ontology/action-types",
        "/api/v1/databases/{db_name}/ontology/action-types/{resource_id}",
        "/api/v1/databases/{db_name}/ontology/functions",
        "/api/v1/databases/{db_name}/ontology/functions/{resource_id}",
        "/api/v1/databases/{db_name}/ontology/groups",
        "/api/v1/databases/{db_name}/ontology/groups/{resource_id}",
        "/api/v1/databases/{db_name}/ontology/interfaces",
        "/api/v1/databases/{db_name}/ontology/interfaces/{resource_id}",
        "/api/v1/databases/{db_name}/ontology/shared-properties",
        "/api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}",
        "/api/v1/databases/{db_name}/ontology/value-types",
        "/api/v1/databases/{db_name}/ontology/value-types/{resource_id}",
        "/api/v1/databases/{db_name}/ontology/{class_id}/schema",
        "/api/v1/databases/{db_name}/ontology/{class_id}/mapping-metadata",
        "/api/v1/databases/{db_name}/ontology/proposals",
        "/api/v1/databases/{db_name}/ontology/proposals/{proposal_id}/approve",
        "/api/v1/databases/{db_name}/ontology/deploy",
        "/api/v1/databases/{db_name}/ontology/health",
        "/api/v1/database/{db_name}/ontology/proposals",
        "/api/v1/database/{db_name}/ontology/proposals/{proposal_id}/approve",
        "/api/v1/database/{db_name}/ontology/deploy",
        "/api/v1/database/{db_name}/ontology/health",
        "/api/v1/database/{db_name}/pull-requests",
        "/api/v1/database/{db_name}/pull-requests/{pr_id}",
        "/api/v1/database/{db_name}/pull-requests/{pr_id}/merge",
        "/api/v1/database/{db_name}/pull-requests/{pr_id}/close",
        "/api/v1/database/{db_name}/pull-requests/{pr_id}/diff",
        "/api/v1/databases/{db_name}/classes",
        "/api/v1/databases/{db_name}/classes/{class_id}",
        "/api/v1/databases/{db_name}/class/{class_id}/instances",
        "/api/v1/databases/{db_name}/class/{class_id}/instance/{instance_id}",
        "/api/v1/databases/{db_name}/class/{class_id}/sample-values",
        "/api/v1/pipeline-plans/compile",
        "/api/v1/pipeline-plans/{plan_id}",
        "/api/v1/pipeline-plans/{plan_id}/preview",
        "/api/v1/pipeline-plans/{plan_id}/inspect-preview",
        "/api/v1/pipeline-plans/{plan_id}/evaluate-joins",
        "/api/v1/pipelines/simulate-definition",
        "/api/v1/pipelines/branches",
        "/api/v1/pipelines/branches/{branch}/archive",
        "/api/v1/pipelines/branches/{branch}/restore",
        "/api/v1/pipelines/{pipeline_id}/branches",
        "/api/v1/pipelines/datasets/{dataset_id}/versions/{version_id}/funnel-analysis",
        "/api/v1/databases/{db_name}/suggest-schema-from-data",
        "/api/v1/databases/{db_name}/suggest-mappings",
        "/api/v1/databases/{db_name}/suggest-mappings-from-google-sheets",
        "/api/v1/databases/{db_name}/suggest-mappings-from-excel",
        "/api/v1/databases/{db_name}/suggest-schema-from-google-sheets",
        "/api/v1/databases/{db_name}/suggest-schema-from-excel",
        "/api/v1/databases/{db_name}/import-from-google-sheets/dry-run",
        "/api/v1/databases/{db_name}/import-from-google-sheets/commit",
        "/api/v1/databases/{db_name}/import-from-excel/dry-run",
        "/api/v1/databases/{db_name}/import-from-excel/commit",
        "/api/v1/data-connectors/google-sheets/grid",
        "/api/v1/data-connectors/google-sheets/preview",
        "/api/v1/data-connectors/google-sheets/register",
        "/api/v1/data-connectors/google-sheets/registered",
        "/api/v1/data-connectors/google-sheets/{sheet_id}/preview",
        "/api/v1/data-connectors/google-sheets/{sheet_id}/start-pipelining",
        "/api/v1/data-connectors/google-sheets/{sheet_id}",
        "/api/v1/data-connectors/google-sheets/connections/{connection_id}",
        "/api/v1/data-connectors/google-sheets/oauth/start",
        "/api/v1/data-connectors/google-sheets/oauth/callback",
        "/api/v1/data-connectors/google-sheets/drive/spreadsheets",
        "/api/v1/data-connectors/google-sheets/spreadsheets/{sheet_id}/worksheets",
    ]
    forbidden_path_fragments = [
        "/actions/logs",
        "/actions/simulations",
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
            for needle in forbidden_path_fragments:
                if needle in text:
                    hits.append((str(path.resolve()), needle))

    assert not hits


@pytest.mark.unit
def test_architecture_guard_covers_legacy_action_and_funnel_path_literals() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    guard_text = (repo_root / "scripts" / "architecture_guard.py").read_text(encoding="utf-8")
    required_needles = [
        "/actions/logs",
        "/actions/simulations",
        "/api/v1/actions/",
        "/api/v1/data-connectors/google-sheets/grid",
        "/api/v1/data-connectors/google-sheets/preview",
        "/api/v1/data-connectors/google-sheets/register",
        "/api/v1/data-connectors/google-sheets/registered",
        "/api/v1/data-connectors/google-sheets/{sheet_id}/preview",
        "/api/v1/data-connectors/google-sheets/{sheet_id}/start-pipelining",
        "/api/v1/data-connectors/google-sheets/{sheet_id}",
        "/api/v1/data-connectors/google-sheets/connections/{connection_id}",
        "/api/v1/data-connectors/google-sheets/oauth/start",
        "/api/v1/data-connectors/google-sheets/oauth/callback",
        "/api/v1/data-connectors/google-sheets/drive/spreadsheets",
        "/api/v1/data-connectors/google-sheets/spreadsheets/{sheet_id}/worksheets",
        "/api/v1/funnel/",
        "/api/v1/version/",
        "/api/v1/branch/",
    ]

    for needle in required_needles:
        assert needle in guard_text


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
def test_funnel_runtime_does_not_call_bff_google_sheets_v1_paths() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    funnel_root = backend_root / "funnel"
    forbidden_literals = [
        "/api/v1/data-connectors/google-sheets/preview",
        "/api/v1/data-connectors/google-sheets/grid",
    ]

    hits: list[tuple[str, str]] = []
    for path in funnel_root.rglob("*.py"):
        if "tests" in path.parts:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for needle in forbidden_literals:
            if needle in text:
                hits.append((str(path.resolve()), needle))

    assert not hits


@pytest.mark.unit
def test_action_worker_direct_undo_literal_absent_from_runtime() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    runtime_files = [
        backend_root / "action_worker" / "main.py",
        backend_root / "oms" / "routers" / "action_async.py",
    ]

    hits: list[tuple[str, str]] = []
    for path in runtime_files:
        text = path.read_text(encoding="utf-8")
        for needle in ("direct_undo", "undo_of_action_log_id", "_execute_direct_undo"):
            if needle in text:
                hits.append((str(path.resolve()), needle))

    assert not hits


@pytest.mark.unit
def test_oms_action_apply_runtime_not_coupled_to_internal_simulation_registry() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    action_router = backend_root / "oms" / "routers" / "action_async.py"
    text = action_router.read_text(encoding="utf-8")
    assert "ActionSimulationRegistry" not in text
    assert "create_simulation(" not in text
    assert "create_version(" not in text
    assert "simulation_id" not in text


@pytest.mark.unit
def test_migration_docs_synced_to_code_deleted_status() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    migration_text = (repo_root / "docs" / "FOUNDRY_V1_TO_V2_MIGRATION.md").read_text(encoding="utf-8").lower()
    checklist_text = (repo_root / "docs" / "FOUNDRY_ALIGNMENT_CHECKLIST.md").read_text(encoding="utf-8").lower()

    assert "code deleted" in migration_text
    assert "code-deleted" in checklist_text


@pytest.mark.unit
def test_legacy_link_types_router_composition_shim_deleted() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    legacy_router_path = backend_root / "bff" / "routers" / "link_types.py"
    assert not legacy_router_path.exists()

    routers_init = (backend_root / "bff" / "routers" / "__init__.py").read_text(encoding="utf-8")
    assert "\"link_types\"" not in routers_init


@pytest.mark.unit
def test_legacy_link_type_and_objectify_helper_shims_deleted() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    legacy_paths = [
        backend_root / "bff" / "routers" / "link_types_ops.py",
        backend_root / "bff" / "routers" / "objectify_ops.py",
    ]
    for path in legacy_paths:
        assert not path.exists()


@pytest.mark.unit
def test_legacy_actions_and_query_router_shims_deleted() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    legacy_paths = [
        backend_root / "bff" / "routers" / "actions.py",
        backend_root / "bff" / "routers" / "query.py",
    ]
    for path in legacy_paths:
        assert not path.exists()

    routers_init = (backend_root / "bff" / "routers" / "__init__.py").read_text(encoding="utf-8")
    assert "\"actions\"" not in routers_init
    assert "\"query\"" not in routers_init

    main_text = (backend_root / "bff" / "main.py").read_text(encoding="utf-8")
    assert "actions.router" not in main_text
    assert "query.router" not in main_text


@pytest.mark.unit
def test_legacy_data_connector_sheet_tools_router_deleted() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    legacy_path = backend_root / "bff" / "routers" / "data_connector_sheet_tools.py"
    assert not legacy_path.exists()
    data_connector_router = backend_root / "bff" / "routers" / "data_connector.py"
    assert not data_connector_router.exists()

    main_text = (backend_root / "bff" / "main.py").read_text(encoding="utf-8")
    assert "data_connector.router" not in main_text


@pytest.mark.unit
def test_legacy_ontology_suggest_and_import_router_shims_deleted() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    legacy_paths = [
        backend_root / "bff" / "routers" / "ontology_suggestions.py",
        backend_root / "bff" / "routers" / "ontology_imports.py",
        backend_root / "bff" / "services" / "ontology_suggestions_service.py",
        backend_root / "bff" / "services" / "ontology_imports_service.py",
    ]
    for path in legacy_paths:
        assert not path.exists()

    ontology_module = (backend_root / "bff" / "routers" / "ontology.py").read_text(encoding="utf-8")
    assert "ontology_suggestions" not in ontology_module
    assert "ontology_imports" not in ontology_module


@pytest.mark.unit
def test_legacy_ontology_metadata_router_shim_deleted() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    legacy_path = backend_root / "bff" / "routers" / "ontology_metadata.py"
    assert not legacy_path.exists()

    ontology_module = (backend_root / "bff" / "routers" / "ontology.py").read_text(encoding="utf-8")
    assert "ontology_metadata" not in ontology_module


@pytest.mark.unit
def test_legacy_instances_sample_values_router_deleted() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    legacy_path = backend_root / "bff" / "routers" / "instances.py"
    assert not legacy_path.exists()

    routers_init = (backend_root / "bff" / "routers" / "__init__.py").read_text(encoding="utf-8")
    assert "\"instances\"" not in routers_init

    main_text = (backend_root / "bff" / "main.py").read_text(encoding="utf-8")
    assert "instances.router" not in main_text


@pytest.mark.unit
def test_legacy_pipeline_compile_and_simulation_router_shims_deleted() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    legacy_paths = [
        backend_root / "bff" / "routers" / "pipeline_branches.py",
        backend_root / "bff" / "routers" / "pipeline_plans.py",
        backend_root / "bff" / "routers" / "pipeline_plans_compile.py",
        backend_root / "bff" / "routers" / "pipeline_plans_read.py",
        backend_root / "bff" / "routers" / "pipeline_plans_preview.py",
        backend_root / "bff" / "routers" / "pipeline_simulation.py",
    ]
    for path in legacy_paths:
        assert not path.exists()

    pipeline_module = (backend_root / "bff" / "routers" / "pipeline.py").read_text(encoding="utf-8")
    assert "pipeline_branches" not in pipeline_module
    assert "pipeline_simulation" not in pipeline_module

    routers_init = (backend_root / "bff" / "routers" / "__init__.py").read_text(encoding="utf-8")
    assert "\"pipeline_plans\"" not in routers_init

    main_text = (backend_root / "bff" / "main.py").read_text(encoding="utf-8")
    assert "pipeline_plans.router" not in main_text


@pytest.mark.unit
def test_external_funnel_service_artifacts_deleted() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    removed_paths = [
        backend_root / "start_funnel.sh",
        backend_root / "funnel" / "Dockerfile",
        backend_root / "funnel" / "requirements.txt",
    ]
    for path in removed_paths:
        assert not path.exists()

    verify_script = (backend_root / "verify_new_structure.sh").read_text(encoding="utf-8")
    assert "start_funnel.sh" not in verify_script

    audit_script = (backend_root / "scripts" / "single_source_of_truth_audit.py").read_text(encoding="utf-8")
    assert "'funnel'" not in audit_script

    funnel_main = (backend_root / "funnel" / "main.py").read_text(encoding="utf-8")
    assert "run_service(app, service_info, \"funnel.main:app\")" not in funnel_main
    assert "/api/v1/funnel/" not in funnel_main

    funnel_client = (backend_root / "bff" / "services" / "funnel_client.py").read_text(encoding="utf-8")
    assert "/api/v1/funnel/" not in funnel_client


@pytest.mark.unit
def test_legacy_oms_pull_request_modules_deleted() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    legacy_paths = [
        backend_root / "oms" / "routers" / "pull_request.py",
        backend_root / "oms" / "services" / "pull_request_service.py",
    ]
    for path in legacy_paths:
        assert not path.exists()

    oms_main = (backend_root / "oms" / "main.py").read_text(encoding="utf-8")
    assert "from oms.routers import pull_request" not in oms_main
    assert "pull_request.router" not in oms_main
    assert "enable_pull_requests" not in oms_main


@pytest.mark.unit
def test_legacy_action_simulation_registry_module_deleted() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    legacy_path = backend_root / "shared" / "services" / "registries" / "action_simulation_registry.py"
    assert not legacy_path.exists()


@pytest.mark.unit
def test_migration_docs_do_not_reintroduce_fullmetadata_preview_contradiction() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    migration_text = (repo_root / "docs" / "FOUNDRY_V1_TO_V2_MIGRATION.md").read_text(encoding="utf-8")
    forbidden = "`GET /api/v2/ontologies/{ontologyRid}/fullMetadata`는 `preview` 파라미터를 사용하지 않음"
    assert forbidden not in migration_text


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


@pytest.mark.unit
def test_removed_v1_paths_absent_from_generated_api_docs() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    generated_docs = [
        repo_root / "docs" / "API_REFERENCE.md",
        repo_root / "docs-portal" / "docs" / "api" / "auto-v1-reference.mdx",
        repo_root / "docs-portal" / "docs" / "api" / "auto-v2-reference.mdx",
        repo_root / "docs-portal" / "static" / "generated" / "bff-openapi.json",
    ]
    removed_literals = [
        "/api/v1/data-connectors/google-sheets/grid",
        "/api/v1/data-connectors/google-sheets/preview",
        "/api/v1/data-connectors/google-sheets/register",
        "/api/v1/data-connectors/google-sheets/registered",
        "/api/v1/data-connectors/google-sheets/{sheet_id}/preview",
        "/api/v1/data-connectors/google-sheets/{sheet_id}/start-pipelining",
        "/api/v1/data-connectors/google-sheets/{sheet_id}",
        "/api/v1/data-connectors/google-sheets/connections/{connection_id}",
        "/api/v1/data-connectors/google-sheets/oauth/start",
        "/api/v1/data-connectors/google-sheets/oauth/callback",
        "/api/v1/data-connectors/google-sheets/drive/spreadsheets",
        "/api/v1/data-connectors/google-sheets/spreadsheets/{sheet_id}/worksheets",
        "/api/v1/databases/{db_name}/actions/logs/{action_log_id}/undo",
        "/api/v1/databases/{db_name}/actions/logs",
        "/api/v1/databases/{db_name}/actions/logs/{action_log_id}",
        "/api/v1/databases/{db_name}/actions/simulations",
        "/api/v1/databases/{db_name}/actions/simulations/{simulation_id}",
        "/api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions",
        "/api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions/{version}",
        "/api/v2/ontologies/{ontologyRid}/actions/logs/{actionLogId}/undo",
        "/api/v2/ontologies/{ontologyRid}/actions/logs",
        "/api/v2/ontologies/{ontologyRid}/actions/logs/{actionLogId}",
        "/api/v2/ontologies/{ontologyRid}/actions/simulations",
        "/api/v2/ontologies/{ontologyRid}/actions/simulations/{simulationId}",
        "/api/v2/ontologies/{ontologyRid}/actions/simulations/{simulationId}/versions",
        "/api/v2/ontologies/{ontologyRid}/actions/simulations/{simulationId}/versions/{version}",
        "/api/v1/pipelines/datasets/{dataset_id}/versions/{version_id}/funnel-analysis",
        "/api/v1/databases/{db_name}/suggest-schema-from-data",
        "/api/v1/databases/{db_name}/suggest-mappings",
        "/api/v1/databases/{db_name}/suggest-mappings-from-google-sheets",
        "/api/v1/databases/{db_name}/suggest-mappings-from-excel",
        "/api/v1/databases/{db_name}/suggest-schema-from-google-sheets",
        "/api/v1/databases/{db_name}/suggest-schema-from-excel",
        "/api/v1/databases/{db_name}/import-from-google-sheets/dry-run",
        "/api/v1/databases/{db_name}/import-from-google-sheets/commit",
        "/api/v1/databases/{db_name}/import-from-excel/dry-run",
        "/api/v1/databases/{db_name}/import-from-excel/commit",
        "/api/v1/databases/{db_name}/classes",
        "/api/v1/databases/{db_name}/class/{class_id}/sample-values",
        "/api/v1/databases/{db_name}/ontology/{class_id}/schema",
        "/api/v1/databases/{db_name}/ontology/{class_id}/mapping-metadata",
        "/api/v1/databases/{db_name}/ontology/validate",
        "/api/v1/databases/{db_name}/ontology/proposals",
        "/api/v1/databases/{db_name}/ontology/proposals/{proposal_id}/approve",
        "/api/v1/databases/{db_name}/ontology/deploy",
        "/api/v1/databases/{db_name}/ontology/health",
    ]

    for path in generated_docs:
        text = path.read_text(encoding="utf-8")
        for needle in removed_literals:
            assert needle not in text


@pytest.mark.unit
def test_tabular_ingest_response_schema_uses_tabular_analysis_key_only() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    generated_openapi = (repo_root / "docs-portal" / "static" / "generated" / "bff-openapi.json").read_text(
        encoding="utf-8"
    )
    assert '"funnel_analysis"' not in generated_openapi
    assert '"tabular_analysis"' in generated_openapi
