from __future__ import annotations

from typing import Any, Iterable

import pytest

from tests.test_openapi_contract_smoke import Operation, SmokeContext, _build_plan


_RECIPE_GUARD_OPERATIONS: list[tuple[str, str]] = [
    ("GET", "/api/v2/ontologies/{ontology}/objectTypes/{objectType}/fullMetadata"),
    ("GET", "/api/v2/ontologies/{ontology}/interfaceTypes"),
    ("GET", "/api/v2/ontologies/{ontology}/interfaceTypes/{interfaceType}"),
    ("GET", "/api/v2/ontologies/{ontology}/sharedPropertyTypes"),
    ("GET", "/api/v2/ontologies/{ontology}/sharedPropertyTypes/{sharedPropertyType}"),
    ("GET", "/api/v2/ontologies/{ontology}/valueTypes"),
    ("GET", "/api/v2/ontologies/{ontology}/valueTypes/{valueType}"),
    ("GET", "/api/v2/ontologies/{ontology}/actionTypes/byRid/{actionTypeRid}"),
    ("GET", "/api/v2/ontologies/{ontology}/actionTypes/{actionType}"),
    ("POST", "/api/v2/ontologies/{ontology}/actions/{action}/apply"),
    ("POST", "/api/v2/ontologies/{ontology}/actions/{action}/applyBatch"),
    ("POST", "/api/v2/ontologies/{ontology}/objectSets/loadObjects"),
    ("POST", "/api/v2/ontologies/{ontology}/objectSets/loadLinks"),
    ("POST", "/api/v2/ontologies/{ontology}/objectSets/loadObjectsMultipleObjectTypes"),
    ("POST", "/api/v2/ontologies/{ontology}/objectSets/loadObjectsOrInterfaces"),
    ("POST", "/api/v2/ontologies/{ontology}/objectSets/aggregate"),
    ("POST", "/api/v2/ontologies/{ontology}/objectSets/createTemporary"),
    ("GET", "/api/v2/ontologies/{ontology}/objectSets/{objectSetRid}"),
    ("GET", "/api/v2/ontologies/{ontology}/queryTypes/{queryApiName}"),
    ("POST", "/api/v2/ontologies/{ontology}/actions/logs/{actionLogId}/undo"),
    ("GET", "/api/v1/lineage/column-lineage"),
]

_PREVIEW_ENDPOINTS: set[tuple[str, str]] = {
    ("GET", "/api/v2/ontologies/{ontology}/objectTypes/{objectType}/fullMetadata"),
    ("GET", "/api/v2/ontologies/{ontology}/interfaceTypes"),
    ("GET", "/api/v2/ontologies/{ontology}/interfaceTypes/{interfaceType}"),
    ("GET", "/api/v2/ontologies/{ontology}/sharedPropertyTypes"),
    ("GET", "/api/v2/ontologies/{ontology}/sharedPropertyTypes/{sharedPropertyType}"),
    ("GET", "/api/v2/ontologies/{ontology}/valueTypes"),
    ("GET", "/api/v2/ontologies/{ontology}/valueTypes/{valueType}"),
    ("POST", "/api/v2/ontologies/{ontology}/objectSets/loadLinks"),
    ("POST", "/api/v2/ontologies/{ontology}/objectSets/loadObjectsMultipleObjectTypes"),
    ("POST", "/api/v2/ontologies/{ontology}/objectSets/loadObjectsOrInterfaces"),
}


def _iter_values(value: Any) -> Iterable[Any]:
    if isinstance(value, dict):
        for inner in value.values():
            yield from _iter_values(inner)
        return
    if isinstance(value, (list, tuple)):
        for inner in value:
            yield from _iter_values(inner)
        return
    yield value


@pytest.mark.asyncio
@pytest.mark.parametrize(("method", "path"), _RECIPE_GUARD_OPERATIONS)
async def test_openapi_smoke_recipes_exist_and_query_params_are_aiohttp_safe(method: str, path: str) -> None:
    ctx = SmokeContext(
        db_name="openapi_smoke_recipe_guard",
        branch_name="main",
        class_id="Product",
        advanced_class_id="Order",
        wrapper_class_id="SmokeWrapper",
        instance_id="prod_guard",
        command_ids={"create_database": "cmd_guard"},
        udf_id="udf_guard",
        action_type_id="RenameProduct_guard",
    )
    operation = Operation(method=method, path=path, tags=(), summary="recipe guard")

    plan = await _build_plan(operation, ctx)
    assert plan.method == method
    assert plan.path_template == path
    assert isinstance(plan.expected_statuses, tuple) and plan.expected_statuses

    if plan.params:
        for item in _iter_values(plan.params):
            # aiohttp/yarl rejects bool query values as route vars; require explicit string coercion.
            assert not isinstance(item, bool), f"{method} {path} has bool query param value: {plan.params!r}"


@pytest.mark.asyncio
@pytest.mark.parametrize(("method", "path"), sorted(_PREVIEW_ENDPOINTS))
async def test_openapi_smoke_preview_recipes_use_string_preview_true(method: str, path: str) -> None:
    ctx = SmokeContext(
        db_name="openapi_smoke_recipe_guard",
        branch_name="main",
        class_id="Product",
        advanced_class_id="Order",
        wrapper_class_id="SmokeWrapper",
        instance_id="prod_guard",
        command_ids={"create_database": "cmd_guard"},
        udf_id="udf_guard",
        action_type_id="RenameProduct_guard",
    )
    operation = Operation(method=method, path=path, tags=(), summary="recipe guard")

    plan = await _build_plan(operation, ctx)
    params = plan.params or {}
    assert params.get("preview") == "true", f"{method} {path} must send preview='true'"
