from __future__ import annotations

import pytest
from fastapi import HTTPException, Response
from starlette.requests import Request

from bff.routers import link_types as link_types_router
from bff.routers import object_types as object_types_router
from shared.utils.foundry_page_token import encode_offset_page_token


class _FakeObjectTypeOMSClient:
    async def list_ontology_resources(
        self,
        db_name,
        *,
        resource_type,
        branch="main",
        limit=200,
        offset=0,
    ):  # noqa: ANN001, ANN003
        _ = db_name, branch, limit, offset
        assert resource_type == "object_type"
        return {
            "data": {
                "resources": [
                    {
                        "id": "Account",
                        "label": {"en": "Account"},
                        "description": {"en": "Account object"},
                        "spec": {
                            "status": "ACTIVE",
                            "pk_spec": {"primary_key": ["account_id"]},
                        },
                    }
                ]
            }
        }

    async def get_ontology(self, db_name, class_id, *, branch="main"):  # noqa: ANN001, ANN003
        _ = db_name, branch
        if class_id != "Account":
            return {"data": {"properties": []}}
        return {
            "data": {
                "properties": [
                    {
                        "name": "account_id",
                        "label": {"en": "Account ID"},
                        "type": "xsd:string",
                    }
                ]
            }
        }


class _FakeObjectTypeGetOMSClient:
    async def get_ontology_resource(
        self,
        db_name,
        *,
        resource_type,
        resource_id,
        branch="main",
    ):  # noqa: ANN001, ANN003
        _ = db_name, branch
        assert resource_type == "object_type"
        return {
            "data": {
                "id": resource_id,
                "label": {"en": "Account"},
                "spec": {
                    "status": "ACTIVE",
                    "visibility": "NORMAL",
                    "pk_spec": {"primary_key": ["account_id"], "title_key": ["account_id"]},
                },
            }
        }

    async def get_ontology(self, db_name, class_id, *, branch="main"):  # noqa: ANN001, ANN003
        _ = db_name, class_id, branch
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


class _FakeObjectTypeGetNoPkSpecOMSClient:
    async def get_ontology_resource(
        self,
        db_name,
        *,
        resource_type,
        resource_id,
        branch="main",
    ):  # noqa: ANN001, ANN003
        _ = db_name, branch
        assert resource_type == "object_type"
        return {
            "data": {
                "id": resource_id,
                "label": {"en": "Product"},
                "spec": {
                    "status": "ACTIVE",
                    "visibility": "NORMAL",
                },
            }
        }

    async def get_ontology(self, db_name, class_id, *, branch="main"):  # noqa: ANN001, ANN003
        _ = db_name, class_id, branch
        return {
            "data": {
                "properties": [
                    {
                        "name": "product_id",
                        "label": {"en": "Product ID"},
                        "type": "xsd:string",
                        "required": True,
                        "primaryKey": True,
                    },
                    {
                        "name": "name",
                        "label": {"en": "Name"},
                        "type": "xsd:string",
                        "required": True,
                        "titleKey": True,
                    },
                ]
            }
        }


class _FakeLinkTypeOMSClient:
    async def list_ontology_resources(
        self,
        db_name,
        *,
        resource_type,
        branch="main",
        limit=200,
        offset=0,
    ):  # noqa: ANN001, ANN003
        _ = db_name, branch, limit, offset
        assert resource_type == "link_type"
        return {
            "data": {
                "resources": [
                    {
                        "id": "owned_by",
                        "spec": {
                            "from": "Account",
                            "to": "User",
                            "cardinality": "n:1",
                            "relationship_spec": {"fk_column": "owner_id"},
                        },
                    },
                    {
                        "id": "contains",
                        "spec": {
                            "from": "Order",
                            "to": "Product",
                            "cardinality": "1:n",
                        },
                    },
                    {
                        "id": "tags",
                        "spec": {
                            "from": "Account",
                            "to": "Tag",
                            "cardinality": "n:n",
                        },
                    },
                ]
            }
        }


class _FakeLinkTypeGetOMSClient:
    async def get_ontology_resource(
        self,
        db_name,
        *,
        resource_type,
        resource_id,
        branch="main",
    ):  # noqa: ANN001, ANN003
        _ = db_name, branch
        assert resource_type == "link_type"
        if resource_id == "owned_by":
            return {
                "data": {
                    "id": "owned_by",
                    "label": {"en": "Owned By"},
                    "spec": {
                        "from": "Account",
                        "to": "User",
                        "cardinality": "n:1",
                        "status": "ACTIVE",
                        "relationship_spec": {"fk_column": "owner_id"},
                    },
                }
            }
        return {"data": {"id": resource_id, "spec": {"from": "Order", "to": "Product"}}}


class _PagedLinkTypeOMSClient:
    _resources = [
        {"id": "non_match_1", "spec": {"from": "Order", "to": "Product", "cardinality": "1:n"}},
        {"id": "account_owner", "spec": {"from": "Account", "to": "User", "cardinality": "n:1"}},
        {"id": "non_match_2", "spec": {"from": "Invoice", "to": "Company", "cardinality": "n:1"}},
        {"id": "account_tag", "spec": {"from": "Account", "to": "Tag", "cardinality": "n:n"}},
    ]

    async def list_ontology_resources(
        self,
        db_name,
        *,
        resource_type,
        branch="main",
        limit=200,
        offset=0,
    ):  # noqa: ANN001, ANN003
        _ = db_name, branch
        assert resource_type == "link_type"
        start = max(0, int(offset))
        end = start + max(0, int(limit))
        return {"data": {"resources": self._resources[start:end]}}


async def _noop_require_domain_role(request, *, db_name):  # noqa: ANN001, ANN003
    _ = request, db_name
    return None


@pytest.mark.asyncio
async def test_list_object_types_foundry_shape():
    request = Request({"type": "http", "headers": []})
    headers = Response()
    original_require = object_types_router._require_domain_role
    object_types_router._require_domain_role = _noop_require_domain_role
    try:
        response = await object_types_router.list_object_type_contracts(
            db_name="test_db",
            request=request,
            response=headers,
            branch="main",
            page_size=1,
            page_token=None,
            oms_client=_FakeObjectTypeOMSClient(),
        )
    finally:
        object_types_router._require_domain_role = original_require

    assert response.data is not None
    assert response.data["nextPageToken"] is not None
    assert response.data["data"][0]["apiName"] == "Account"
    assert response.data["data"][0]["displayName"] == "Account"
    assert response.data["data"][0]["pluralDisplayName"] == "Account"
    assert response.data["data"][0]["primaryKey"] == "account_id"
    assert response.data["data"][0]["properties"]["account_id"]["displayName"] == "Account ID"
    assert response.data["data"][0]["properties"]["account_id"]["dataType"]["type"] == "string"
    assert response.data["data"][0]["properties"]["account_id"]["status"] == "ACTIVE"
    assert headers.headers.get("Deprecation") == "true"
    assert headers.headers.get("Sunset") is not None
    assert "successor-version" in str(headers.headers.get("Link"))


@pytest.mark.asyncio
async def test_list_object_types_invalid_page_token_rejected():
    request = Request({"type": "http", "headers": []})
    original_require = object_types_router._require_domain_role
    object_types_router._require_domain_role = _noop_require_domain_role
    try:
        with pytest.raises(HTTPException) as exc_info:
            await object_types_router.list_object_type_contracts(
                db_name="test_db",
                request=request,
                branch="main",
                page_size=10,
                page_token="bad-token",
                oms_client=_FakeObjectTypeOMSClient(),
            )
    finally:
        object_types_router._require_domain_role = original_require

    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_list_object_types_page_token_scope_mismatch_rejected():
    request = Request({"type": "http", "headers": []})
    original_require = object_types_router._require_domain_role
    object_types_router._require_domain_role = _noop_require_domain_role
    try:
        with pytest.raises(HTTPException) as exc_info:
            await object_types_router.list_object_type_contracts(
                db_name="test_db",
                request=request,
                branch="main",
                page_size=10,
                page_token=encode_offset_page_token(10, scope="v1/object-types|other_db|main"),
                oms_client=_FakeObjectTypeOMSClient(),
            )
    finally:
        object_types_router._require_domain_role = original_require

    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_list_object_types_page_token_rejected_when_page_size_changes():
    request = Request({"type": "http", "headers": []})
    original_require = object_types_router._require_domain_role
    object_types_router._require_domain_role = _noop_require_domain_role
    try:
        first = await object_types_router.list_object_type_contracts(
            db_name="test_db",
            request=request,
            branch="main",
            page_size=1,
            page_token=None,
            oms_client=_FakeObjectTypeOMSClient(),
        )
        assert first.data is not None
        token = first.data["nextPageToken"]
        assert token is not None

        with pytest.raises(HTTPException) as exc_info:
            await object_types_router.list_object_type_contracts(
                db_name="test_db",
                request=request,
                branch="main",
                page_size=2,
                page_token=token,
                oms_client=_FakeObjectTypeOMSClient(),
            )
    finally:
        object_types_router._require_domain_role = original_require

    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_get_object_type_foundry_shape():
    request = Request({"type": "http", "headers": []})
    headers = Response()
    original_require = object_types_router._require_domain_role
    object_types_router._require_domain_role = _noop_require_domain_role
    try:
        response = await object_types_router.get_object_type_contract(
            db_name="test_db",
            class_id="Account",
            request=request,
            response=headers,
            branch="main",
            oms_client=_FakeObjectTypeGetOMSClient(),
        )
    finally:
        object_types_router._require_domain_role = original_require

    assert response.data is not None
    row = response.data
    assert row["apiName"] == "Account"
    assert row["displayName"] == "Account"
    assert row["status"] == "ACTIVE"
    assert row["visibility"] == "NORMAL"
    assert row["primaryKey"] == "account_id"
    assert row["titleProperty"] == "account_id"
    assert row["properties"]["account_id"]["dataType"]["type"] == "string"
    assert row["properties"]["account_id"]["required"] is True
    assert row["properties"]["account_id"]["status"] == "ACTIVE"
    assert "backing_datasource" not in row
    assert headers.headers.get("Deprecation") == "true"
    assert "successor-version" in str(headers.headers.get("Link"))


@pytest.mark.asyncio
async def test_get_object_type_infers_primary_key_without_pk_spec():
    request = Request({"type": "http", "headers": []})
    original_require = object_types_router._require_domain_role
    object_types_router._require_domain_role = _noop_require_domain_role
    try:
        response = await object_types_router.get_object_type_contract(
            db_name="test_db",
            class_id="Product",
            request=request,
            branch="main",
            oms_client=_FakeObjectTypeGetNoPkSpecOMSClient(),
        )
    finally:
        object_types_router._require_domain_role = original_require

    assert response.data is not None
    row = response.data
    assert row["primaryKey"] == "product_id"
    assert row["titleProperty"] == "name"


@pytest.mark.asyncio
async def test_list_outgoing_link_types_foundry_shape_and_filtering():
    headers = Response()
    response = await link_types_router.list_outgoing_link_types(
        db_name="test_db",
        object_type_api_name="Account",
        response=headers,
        branch="main",
        page_size=10,
        page_token=None,
        oms_client=_FakeLinkTypeOMSClient(),
    )

    assert response.data is not None
    rows = response.data["data"]
    assert [row["apiName"] for row in rows] == ["owned_by", "tags"]
    assert rows[0]["objectTypeApiName"] == "User"
    assert rows[0]["cardinality"] == "ONE"
    assert rows[0]["foreignKeyPropertyApiName"] == "owner_id"
    assert rows[1]["cardinality"] == "MANY"
    assert headers.headers.get("Deprecation") == "true"
    assert headers.headers.get("Sunset") is not None


@pytest.mark.asyncio
async def test_list_outgoing_link_types_invalid_page_token_rejected():
    with pytest.raises(HTTPException) as exc_info:
        await link_types_router.list_outgoing_link_types(
            db_name="test_db",
            object_type_api_name="Account",
            branch="main",
            page_size=10,
            page_token="bad-token",
            oms_client=_FakeLinkTypeOMSClient(),
        )

    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_list_outgoing_link_types_page_token_scope_mismatch_rejected():
    with pytest.raises(HTTPException) as exc_info:
        await link_types_router.list_outgoing_link_types(
            db_name="test_db",
            object_type_api_name="Account",
            branch="main",
            page_size=10,
            page_token=encode_offset_page_token(
                10,
                scope="v1/outgoing-link-types|test_db|main|OtherObjectType",
            ),
            oms_client=_FakeLinkTypeOMSClient(),
        )

    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_list_outgoing_link_types_pagination_uses_filtered_offset_semantics():
    first = await link_types_router.list_outgoing_link_types(
        db_name="test_db",
        object_type_api_name="Account",
        branch="main",
        page_size=1,
        page_token=None,
        oms_client=_PagedLinkTypeOMSClient(),
    )

    assert first.data is not None
    assert [row["apiName"] for row in first.data["data"]] == ["account_owner"]
    assert first.data["nextPageToken"] is not None

    second = await link_types_router.list_outgoing_link_types(
        db_name="test_db",
        object_type_api_name="Account",
        branch="main",
        page_size=1,
        page_token=first.data["nextPageToken"],
        oms_client=_PagedLinkTypeOMSClient(),
    )

    assert second.data is not None
    assert [row["apiName"] for row in second.data["data"]] == ["account_tag"]


@pytest.mark.asyncio
async def test_list_outgoing_link_types_page_token_rejected_when_page_size_changes():
    first = await link_types_router.list_outgoing_link_types(
        db_name="test_db",
        object_type_api_name="Account",
        branch="main",
        page_size=1,
        page_token=None,
        oms_client=_PagedLinkTypeOMSClient(),
    )

    assert first.data is not None
    token = first.data["nextPageToken"]
    assert token is not None

    with pytest.raises(HTTPException) as exc_info:
        await link_types_router.list_outgoing_link_types(
            db_name="test_db",
            object_type_api_name="Account",
            branch="main",
            page_size=2,
            page_token=token,
            oms_client=_PagedLinkTypeOMSClient(),
        )

    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_get_outgoing_link_type_foundry_shape():
    headers = Response()
    response = await link_types_router.get_outgoing_link_type(
        db_name="test_db",
        object_type_api_name="Account",
        link_type_api_name="owned_by",
        response=headers,
        branch="main",
        oms_client=_FakeLinkTypeGetOMSClient(),
    )

    assert response.data is not None
    assert response.data["apiName"] == "owned_by"
    assert response.data["displayName"] == "Owned By"
    assert response.data["objectTypeApiName"] == "User"
    assert headers.headers.get("Deprecation") == "true"
    assert response.data["cardinality"] == "ONE"
    assert response.data["status"] == "ACTIVE"
