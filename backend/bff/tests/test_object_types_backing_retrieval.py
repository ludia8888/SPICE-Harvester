import pytest
from starlette.requests import Request

from bff.routers import object_types as object_types_router


class _FakeOMSClient:
    async def get_ontology_resource(self, db_name, resource_type, resource_id, *, branch="main"):  # noqa: ANN001, ANN003
        _ = db_name, resource_type, branch
        return {
            "data": {
                "id": resource_id,
                "label": {"en": "Account"},
                "spec": {
                    "status": "ACTIVE",
                    "pk_spec": {"primary_key": ["account_id"], "title_key": ["account_id"]},
                    "visibility": "NORMAL",
                },
            }
        }

    async def get_ontology(self, db_name, class_id, *, branch="main"):  # noqa: ANN001, ANN003
        _ = db_name, branch
        return {
            "data": {
                "properties": [
                    {
                        "name": "account_id",
                        "label": {"en": "Account ID"},
                        "type": "xsd:string",
                        "required": True,
                    }
                ]
            }
        }


async def _noop_require_domain_role(request, *, db_name):  # noqa: ANN001, ANN003
    _ = request, db_name
    return None


@pytest.mark.asyncio
async def test_object_type_retrieval_uses_foundry_shape():
    request = Request({"type": "http", "headers": []})
    original_require = object_types_router._require_domain_role
    object_types_router._require_domain_role = _noop_require_domain_role
    try:
        response = await object_types_router.get_object_type_contract(
            db_name="test_db",
            class_id="Account",
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        object_types_router._require_domain_role = original_require

    data = response.data
    assert data["apiName"] == "Account"
    assert data["displayName"] == "Account"
    assert data["status"] == "ACTIVE"
    assert data["visibility"] == "NORMAL"
    assert data["primaryKey"] == "account_id"
    assert data["titleProperty"] == "account_id"
    assert data["properties"]["account_id"]["displayName"] == "Account ID"
    assert data["properties"]["account_id"]["dataType"]["type"] == "string"
    assert data["properties"]["account_id"]["required"] is True
    assert data["properties"]["account_id"]["status"] == "ACTIVE"
    assert "backing_datasource" not in data
    assert "backing_datasource_version" not in data
    assert "mapping_spec" not in data
