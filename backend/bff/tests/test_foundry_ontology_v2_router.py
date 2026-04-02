from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from fastapi.responses import JSONResponse
from starlette.requests import Request

from bff.routers import foundry_ontology_v2 as router_v2
from shared.utils.foundry_page_token import encode_offset_page_token


class _FakeOMSClient:
    def __init__(self) -> None:
        self.last_post: tuple[str, dict] | None = None
        self.last_call_headers: dict[str, dict[str, str] | None] = {}

    async def list_databases(self):  # noqa: ANN201
        return {
            "data": {
                "databases": [
                    {"name": "test_db", "description": "Test ontology", "rid": "ri.ontology.test_db"},
                    {"name": "sales", "description": "Sales ontology"},
                ]
            }
        }

    async def get_database(self, db_name):  # noqa: ANN001
        if db_name == "missing":
            raise RuntimeError("not found")
        return {"data": {"name": db_name, "description": f"{db_name} ontology"}}

    async def post(self, path, **kwargs):  # noqa: ANN001, ANN003
        self.last_post = (path, kwargs)
        if path.endswith("/count"):
            return {"count": 7}
        if path.endswith("/search"):
            payload = kwargs.get("json") if isinstance(kwargs, dict) else None
            where = payload.get("where") if isinstance(payload, dict) else None
            if isinstance(where, dict) and where.get("type") == "eq" and where.get("field") == "account_id":
                value = str(where.get("value") or "")
                if value == "acc-404":
                    return {"data": [], "nextPageToken": None, "totalCount": "0"}
                return {
                    "data": [
                        {
                            "instance_id": value or "acc-1",
                            "account_id": value or "acc-1",
                            "owner_id": "u-1",
                            "owned_by": ["u-1"],
                            "name": "Alice Account",
                        }
                    ],
                    "nextPageToken": None,
                    "totalCount": "1",
                }
            if isinstance(where, dict) and where.get("type") == "eq" and where.get("field") == "user_id":
                value = str(where.get("value") or "")
                if value in {"u-404", "u-missing"}:
                    return {"data": [], "nextPageToken": None, "totalCount": "0"}
                return {
                    "data": [{"instance_id": value or "u-1", "user_id": value or "u-1", "name": "Owner User"}],
                    "nextPageToken": None,
                    "totalCount": "1",
                }
            if isinstance(where, dict) and where.get("type") == "or":
                rows = []
                for clause in where.get("value") or []:
                    if not isinstance(clause, dict):
                        continue
                    if clause.get("type") != "eq" or clause.get("field") != "user_id":
                        continue
                    value = str(clause.get("value") or "").strip()
                    if not value or value in {"u-404", "u-missing"}:
                        continue
                    rows.append({"instance_id": value, "user_id": value, "name": "Owner User"})
                return {"data": rows, "nextPageToken": None, "totalCount": str(len(rows))}
            return {
                "data": [{"instance_id": "i-1", "account_id": "acc-1", "name": "Alice"}],
                "nextPageToken": None,
                "totalCount": "1",
            }
        return {}

    async def list_ontology_resources(
        self,
        db_name,
        *,
        resource_type=None,
        branch="main",
        limit=200,
        offset=0,
    ):  # noqa: ANN001, ANN003
        _ = db_name, branch, limit, offset
        if resource_type == "object_type":
            return {
                "data": {
                    "resources": [
                        {
                            "id": "Account",
                            "label": {"en": "Account"},
                            "spec": {
                                "status": "ACTIVE",
                                "pk_spec": {"primary_key": ["account_id"], "title_key": ["account_id"]},
                            },
                            "metadata": {
                                "interfaces": ["interface:Auditable"],
                                "implements_interfaces2": {
                                    "Auditable": {"propertyMapping": {"created_at": "created_at"}}
                                },
                                "shared_property_type_mapping": {"TenantScope": "tenant_scope"},
                            },
                        }
                    ]
                }
            }
        if resource_type == "link_type":
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
                    ]
                }
            }
        if resource_type == "action_type":
            return {
                "data": {
                    "resources": [
                        {
                            "id": "ApproveAccount",
                            "rid": "ri.action-type.approve-account",
                            "label": {"en": "Approve Account"},
                            "spec": {
                                "status": "ACTIVE",
                                "target_object_type": "Account",
                                "permission_model": "ontology_roles",
                                "edits_beyond_actions": False,
                                "permission_policy": {
                                    "effect": "ALLOW",
                                    "principals": ["role:DomainModeler"],
                                    "inherit_project_policy": True,
                                    "project_policy_scope": "action_access",
                                    "project_policy_subject_type": "project",
                                },
                            },
                        }
                    ]
                }
            }
        if resource_type == "function":
            return {
                "data": {
                    "resources": [
                        {
                            "id": "findAccounts",
                            "label": {"en": "Find Accounts"},
                            "spec": {
                                "status": "ACTIVE",
                                "version": "1.0.0",
                                "parameters": {"status": {"type": "string"}},
                                "output": {"type": "array", "items": {"type": "object"}},
                                "query": {
                                    "objectType": "Account",
                                    "where": {
                                        "type": "eq",
                                        "field": "account_id",
                                        "value": "${accountId}",
                                    },
                                },
                            },
                        }
                    ]
                }
            }
        if resource_type == "interface":
            return {
                "data": {
                    "resources": [
                        {
                            "id": "Auditable",
                            "label": {"en": "Auditable"},
                            "spec": {"status": "ACTIVE"},
                        }
                    ]
                }
            }
        if resource_type == "shared_property":
            return {
                "data": {
                    "resources": [
                        {
                            "id": "TenantScope",
                            "label": {"en": "Tenant Scope"},
                            "spec": {"status": "ACTIVE"},
                        }
                    ]
                }
            }
        if resource_type == "value_type":
            return {
                "data": {
                    "resources": [
                        {
                            "id": "Money",
                            "label": {"en": "Money"},
                            "spec": {"status": "ACTIVE"},
                        }
                    ]
                }
            }
        return {"data": {"resources": []}}

    async def get_ontology_resource(
        self,
        db_name,
        *,
        resource_type,
        resource_id,
        branch="main",
    ):  # noqa: ANN001, ANN003
        _ = db_name, branch
        if resource_type == "object_type" and resource_id == "Account":
            return {
                "data": {
                    "id": resource_id,
                    "label": {"en": "Account"},
                    "spec": {
                        "status": "ACTIVE",
                        "visibility": "NORMAL",
                        "pk_spec": {"primary_key": ["account_id"], "title_key": ["account_id"]},
                    },
                    "metadata": {
                        "interfaces": ["interface:Auditable"],
                        "implements_interfaces2": {
                            "Auditable": {"propertyMapping": {"created_at": "created_at"}}
                        },
                        "shared_property_type_mapping": {"TenantScope": "tenant_scope"},
                    },
                }
            }
        if resource_type == "object_type" and resource_id == "User":
            return {
                "data": {
                    "id": resource_id,
                    "label": {"en": "User"},
                    "spec": {
                        "status": "ACTIVE",
                        "visibility": "NORMAL",
                        "pk_spec": {"primary_key": ["user_id"], "title_key": ["user_id"]},
                    },
                }
            }
        if resource_type == "link_type" and resource_id == "owned_by":
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
        if resource_type == "action_type" and resource_id == "ApproveAccount":
            return {
                "data": {
                    "id": "ApproveAccount",
                    "rid": "ri.action-type.approve-account",
                    "label": {"en": "Approve Account"},
                    "spec": {
                        "status": "ACTIVE",
                        "target_object_type": "Account",
                        "permission_model": "ontology_roles",
                        "edits_beyond_actions": False,
                        "permission_policy": {
                            "effect": "ALLOW",
                            "principals": ["role:DomainModeler"],
                            "inherit_project_policy": True,
                            "project_policy_scope": "action_access",
                            "project_policy_subject_type": "project",
                        },
                    },
                }
            }
        if resource_type == "function" and resource_id == "findAccounts":
            return {
                "data": {
                    "id": "findAccounts",
                    "label": {"en": "Find Accounts"},
                    "spec": {
                        "status": "ACTIVE",
                        "version": "1.0.0",
                        "parameters": {"status": {"type": "string"}},
                        "output": {"type": "array", "items": {"type": "object"}},
                        "query": {
                            "objectType": "Account",
                            "where": {
                                "type": "eq",
                                "field": "account_id",
                                "value": "${accountId}",
                            },
                        },
                    },
                }
            }
        if resource_type == "interface" and resource_id == "Auditable":
            return {
                "data": {
                    "id": "Auditable",
                    "label": {"en": "Auditable"},
                    "spec": {"status": "ACTIVE"},
                }
            }
        if resource_type == "shared_property" and resource_id == "TenantScope":
            return {
                "data": {
                    "id": "TenantScope",
                    "label": {"en": "Tenant Scope"},
                    "spec": {"status": "ACTIVE"},
                }
            }
        if resource_type == "value_type" and resource_id == "Money":
            return {
                "data": {
                    "id": "Money",
                    "label": {"en": "Money"},
                    "spec": {"status": "ACTIVE"},
                }
            }
        return {"data": {"id": resource_id, "spec": {"from": "Order", "to": "Product"}}}

    async def get_ontology(self, db_name, class_id, *, branch="main"):  # noqa: ANN001, ANN003
        _ = db_name, class_id, branch
        if class_id == "User":
            return {
                "data": {
                    "properties": [
                        {
                            "name": "user_id",
                            "label": {"en": "User ID"},
                            "type": "xsd:string",
                            "required": True,
                        }
                    ]
                }
            }
        return {
            "data": {
                "properties": [
                    {
                        "name": "account_id",
                        "label": {"en": "Account ID"},
                        "type": "xsd:string",
                        "required": True,
                    },
                    {
                        "name": "tenant_scope",
                        "label": {"en": "Tenant Scope"},
                        "type": "xsd:string",
                        "required": False,
                        "shared_property_ref": "TenantScope",
                    },
                ]
            }
        }

    async def get_timeseries_first_point(  # noqa: ANN001, ANN003
        self, db_name, object_type, primary_key, property_name, *, branch="main", headers=None
    ):
        _ = db_name, object_type, primary_key, property_name, branch
        self.last_call_headers["timeseries_first_point"] = dict(headers or {}) or None
        return {"time": "2024-01-01T00:00:00Z", "value": 10}

    async def get_timeseries_last_point(  # noqa: ANN001, ANN003
        self, db_name, object_type, primary_key, property_name, *, branch="main", headers=None
    ):
        _ = db_name, object_type, primary_key, property_name, branch
        self.last_call_headers["timeseries_last_point"] = dict(headers or {}) or None
        return {"time": "2024-01-02T00:00:00Z", "value": 20}

    async def stream_timeseries_points(  # noqa: ANN001, ANN003
        self, db_name, object_type, primary_key, property_name, payload, *, branch="main", headers=None
    ):
        _ = db_name, object_type, primary_key, property_name, payload, branch
        self.last_call_headers["timeseries_stream_points"] = dict(headers or {}) or None
        request = httpx.Request("POST", "http://oms/api/v2/timeseries/streamPoints")
        return httpx.Response(
            200,
            request=request,
            content=b'{"time":"2024-01-01T00:00:00Z","value":10}\n',
            headers={"content-type": "application/x-ndjson"},
        )

    async def upload_attachment(  # noqa: ANN001, ANN003
        self, *, filename, data, content_type="application/octet-stream", headers=None
    ):
        _ = data, content_type
        self.last_call_headers["attachments_upload"] = dict(headers or {}) or None
        return {
            "rid": "ri.attachments.main.attachment.test",
            "filename": filename,
            "sizeBytes": 4,
            "mediaType": "application/octet-stream",
        }

    async def list_property_attachments(  # noqa: ANN001, ANN003
        self, db_name, object_type, primary_key, property_name, *, branch="main", headers=None
    ):
        _ = db_name, object_type, primary_key, property_name, branch
        self.last_call_headers["attachments_list_property"] = dict(headers or {}) or None
        return {
            "type": "single",
            "rid": "ri.attachments.main.attachment.test",
            "filename": "doc.txt",
            "sizeBytes": 4,
            "mediaType": "text/plain",
        }

    async def get_attachment_by_rid(  # noqa: ANN001, ANN003
        self, db_name, object_type, primary_key, property_name, attachment_rid, *, branch="main", headers=None
    ):
        _ = db_name, object_type, primary_key, property_name, branch
        self.last_call_headers["attachments_get_by_rid"] = dict(headers or {}) or None
        return {
            "rid": attachment_rid,
            "filename": "doc.txt",
            "sizeBytes": 4,
            "mediaType": "text/plain",
        }

    async def get_attachment_content(  # noqa: ANN001, ANN003
        self, db_name, object_type, primary_key, property_name, *, branch="main", headers=None
    ):
        _ = db_name, object_type, primary_key, property_name, branch
        self.last_call_headers["attachments_get_content"] = dict(headers or {}) or None
        request = httpx.Request("GET", "http://oms/api/v2/attachments/content")
        return httpx.Response(200, request=request, content=b"data", headers={"content-type": "text/plain"})

    async def get_attachment_content_by_rid(  # noqa: ANN001, ANN003
        self, db_name, object_type, primary_key, property_name, attachment_rid, *, branch="main", headers=None
    ):
        _ = db_name, object_type, primary_key, property_name, attachment_rid, branch
        self.last_call_headers["attachments_get_content_by_rid"] = dict(headers or {}) or None
        request = httpx.Request("GET", "http://oms/api/v2/attachments/content")
        return httpx.Response(200, request=request, content=b"data", headers={"content-type": "text/plain"})


class _ApiNameOnlyOMSClient(_FakeOMSClient):
    async def list_databases(self):  # noqa: ANN201
        return {"data": {"databases": [{"apiName": "finance", "displayName": "Finance"}]}}


class _MissingObjectTypeOMSClient(_FakeOMSClient):
    async def get_ontology_resource(  # noqa: ANN001, ANN003
        self,
        db_name,
        *,
        resource_type,
        resource_id,
        branch="main",
    ):
        if resource_type == "object_type" and resource_id == "MissingType":
            _ = db_name, branch
            return None
        return await super().get_ontology_resource(
            db_name,
            resource_type=resource_type,
            resource_id=resource_id,
            branch=branch,
        )


class _MissingSharedPropertyOMSClient(_FakeOMSClient):
    async def get_ontology_resource(  # noqa: ANN001, ANN003
        self,
        db_name,
        *,
        resource_type,
        resource_id,
        branch="main",
    ):
        if resource_type == "shared_property" and resource_id == "MissingShared":
            _ = db_name, branch
            return None
        return await super().get_ontology_resource(
            db_name,
            resource_type=resource_type,
            resource_id=resource_id,
            branch=branch,
        )


class _MissingQueryExecutionSpecOMSClient(_FakeOMSClient):
    async def get_ontology_resource(  # noqa: ANN001, ANN003
        self,
        db_name,
        *,
        resource_type,
        resource_id,
        branch="main",
    ):
        response = await super().get_ontology_resource(
            db_name,
            resource_type=resource_type,
            resource_id=resource_id,
            branch=branch,
        )
        if resource_type != "function" or resource_id != "findAccounts":
            return response

        if not isinstance(response, dict):
            return response
        data = response.get("data")
        if not isinstance(data, dict):
            return response
        spec = data.get("spec")
        if not isinstance(spec, dict):
            return response

        mutated = dict(spec)
        mutated.pop("query", None)
        return {"data": {**data, "spec": mutated}}


class _CanonicalQueryObjectTypeOMSClient(_FakeOMSClient):
    async def get_ontology_resource(  # noqa: ANN001, ANN003
        self,
        db_name,
        *,
        resource_type,
        resource_id,
        branch="main",
    ):
        response = await super().get_ontology_resource(
            db_name,
            resource_type=resource_type,
            resource_id=resource_id,
            branch=branch,
        )
        if resource_type != "function" or resource_id != "findAccounts":
            return response

        if not isinstance(response, dict):
            return response
        data = response.get("data")
        if not isinstance(data, dict):
            return response
        spec = data.get("spec")
        if not isinstance(spec, dict):
            return response

        mutated = dict(spec)
        mutated["execution"] = {
            "objectTypeApiName": "CanonicalAccount",
            "search": {
                "where": {
                    "type": "eq",
                    "field": "account_id",
                    "value": "${accountId}",
                }
            },
        }
        mutated["query"] = {
            "objectType": "LegacyAccount",
            "where": {
                "type": "eq",
                "field": "account_id",
                "value": "${accountId}",
            },
        }
        return {"data": {**data, "spec": mutated}}


class _ListDatabasesStatusErrorOMSClient(_FakeOMSClient):
    async def list_databases(self):  # noqa: ANN201
        request = httpx.Request("GET", "http://oms/api/v1/database/list")
        response = httpx.Response(
            status_code=503,
            request=request,
            json={"errorCode": "UPSTREAM_UNAVAILABLE"},
        )
        raise httpx.HTTPStatusError("Service unavailable", request=request, response=response)


class _FullMetadataPartialFailureOMSClient(_FakeOMSClient):
    async def list_ontology_resources(
        self,
        db_name,
        *,
        resource_type,
        branch="main",
        limit=200,
        offset=0,
    ):  # noqa: ANN001, ANN003
        if resource_type == "action_type":
            _ = db_name, branch, limit, offset
            raise httpx.ConnectError("action type registry unavailable")
        return await super().list_ontology_resources(
            db_name,
            resource_type=resource_type,
            branch=branch,
            limit=limit,
            offset=offset,
        )


class _LegacyActionResourceOnlyOMSClient(_FakeOMSClient):
    async def list_ontology_resources(
        self,
        db_name,
        *,
        resource_type=None,
        branch="main",
        limit=200,
        offset=0,
    ):  # noqa: ANN001, ANN003
        _ = db_name, branch, limit, offset
        if resource_type == "action_type":
            return {"data": {"resources": []}}
        if resource_type is None:
            return {
                "data": {
                    "resources": [
                        {
                            "id": "HoldOrder",
                            "rid": "ri.action-type.hold-order",
                            "resource_type": "action",
                            "label": {"en": "Hold Order"},
                            "spec": {
                                "status": "ACTIVE",
                                "target_object_type": "Order",
                                "input_schema": {"type": "object"},
                            },
                        }
                    ]
                }
            }
        return await super().list_ontology_resources(
            db_name,
            resource_type=resource_type,
            branch=branch,
            limit=limit,
            offset=offset,
        )

    async def get_ontology_resource(
        self,
        db_name,
        *,
        resource_type,
        resource_id,
        branch="main",
    ):  # noqa: ANN001, ANN003
        _ = db_name, branch
        if resource_type == "action_type" and resource_id == "HoldOrder":
            request = httpx.Request("GET", f"http://oms/api/v1/database/{db_name}/ontology/resources/action_type/{resource_id}")
            response = httpx.Response(status_code=404, request=request, json={"detail": "not found"})
            raise httpx.HTTPStatusError("Not found", request=request, response=response)
        return await super().get_ontology_resource(
            db_name,
            resource_type=resource_type,
            resource_id=resource_id,
            branch=branch,
        )


class _ActionEvidenceOMSClient(_FakeOMSClient):
    async def post(self, path, **kwargs):  # noqa: ANN001, ANN003
        self.last_post = (path, kwargs)
        if path.endswith("/apply"):
            return {
                "validation": {"result": "VALID"},
                "action_log_id": "log-123",
                "sideEffectDelivery": {"status": "SENT"},
            }
        return await super().post(path, **kwargs)


class _PagedLinkTypesOMSClient(_FakeOMSClient):
    def __init__(self) -> None:
        super().__init__()
        self._resources: list[dict] = []
        for index in range(510):
            if index in {498, 501, 504}:
                self._resources.append(
                    {
                        "id": f"rel_{index}",
                        "spec": {
                            "from": "Account",
                            "to": "User",
                            "cardinality": "n:1",
                            "relationship_spec": {"fk_column": "owner_id"},
                        },
                    }
                )
            else:
                self._resources.append(
                    {
                        "id": f"other_{index}",
                        "spec": {
                            "from": "Order",
                            "to": "Product",
                            "cardinality": "1:n",
                        },
                    }
                )

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
        if resource_type == "link_type":
            sliced = self._resources[offset : offset + limit]
            return {"data": {"resources": sliced}}
        return await super().list_ontology_resources(
            db_name,
            resource_type=resource_type,
            branch=branch,
            limit=limit,
            offset=offset,
        )


class _LinkedPaginationOMSClient(_FakeOMSClient):
    async def post(self, path, **kwargs):  # noqa: ANN001, ANN003
        self.last_post = (path, kwargs)
        payload = kwargs.get("json") if isinstance(kwargs, dict) else None
        where = payload.get("where") if isinstance(payload, dict) else None

        if path.endswith("/ontologies/test_db/objects/Account/search"):
            if isinstance(where, dict) and where.get("type") == "eq" and where.get("field") == "account_id":
                return {
                    "data": [
                        {
                            "account_id": "acc-1",
                            "owner_id": "u-1",
                            "owned_by": ["u-1", "u-2", {"id": "u-3"}, "u-4"],
                            "links": {"owned_by": ["u-4", "u-5"]},
                        }
                    ],
                    "nextPageToken": None,
                    "totalCount": "1",
                }

        if path.endswith("/ontologies/test_db/objects/User/search"):
            if isinstance(where, dict) and where.get("type") == "eq" and where.get("field") == "user_id":
                value = str(where.get("value") or "").strip()
                if not value:
                    return {"data": [], "nextPageToken": None, "totalCount": "0"}
                return {
                    "data": [{"instance_id": value, "user_id": value, "name": f"User {value}"}],
                    "nextPageToken": None,
                    "totalCount": "1",
                }
            if isinstance(where, dict) and where.get("type") == "or":
                rows: list[dict] = []
                for clause in where.get("value") or []:
                    if not isinstance(clause, dict):
                        continue
                    if clause.get("type") != "eq" or clause.get("field") != "user_id":
                        continue
                    value = str(clause.get("value") or "").strip()
                    if not value:
                        continue
                    rows.append({"instance_id": value, "user_id": value, "name": f"User {value}"})
                return {"data": rows, "nextPageToken": None, "totalCount": str(len(rows))}

        return await super().post(path, **kwargs)


async def _noop_require_domain_role(request, *, db_name):  # noqa: ANN001, ANN003
    _ = request, db_name
    return None


def _request_with_body(
    body: bytes,
    *,
    method: str = "POST",
    path: str = "/",
    headers: list[tuple[bytes, bytes]] | None = None,
) -> Request:
    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {"type": "http", "method": method, "path": path, "headers": headers or []},
        receive=receive,
    )


@pytest.mark.asyncio
async def test_list_object_types_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_object_types_v2(
            ontologyRid="test_db",
            request=request,
            page_size=10,
            page_token=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"][0]["apiName"] == "Account"
    assert response["data"][0]["primaryKey"] == "account_id"
    assert response["data"][0]["properties"]["account_id"]["status"] == "ACTIVE"
    assert "nextPageToken" in response


@pytest.mark.asyncio
async def test_list_ontologies_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    response = await router_v2.list_ontologies_v2(
        request=request,
        oms_client=_FakeOMSClient(),
    )

    assert isinstance(response, dict)
    assert [row["apiName"] for row in response["data"]] == ["test_db", "sales"]
    assert response["data"][0]["displayName"] == "test_db"
    assert "nextPageToken" not in response


@pytest.mark.asyncio
async def test_create_object_type_v2_routes_to_contract_service_with_backing_sources():
    request = Request({"type": "http", "headers": []})
    oms_client = _FakeOMSClient()
    original_require = router_v2._require_domain_role
    original_create = router_v2.object_type_contract_service.create_object_type_contract
    captured: dict[str, Any] = {}

    async def _fake_create_object_type_contract(**kwargs):  # noqa: ANN003
        captured.update(kwargs)
        return SimpleNamespace(
            data={
                "object_type": {
                    "id": "Order",
                    "spec": {
                        "status": "DRAFT",
                        "pk_spec": {"primary_key": [], "title_key": []},
                        "backing_source": {},
                    },
                }
            }
        )

    router_v2._require_domain_role = _noop_require_domain_role
    router_v2.object_type_contract_service.create_object_type_contract = _fake_create_object_type_contract
    try:
        response = await router_v2.create_object_type_v2(
            ontologyRid="test_db",
            body=router_v2.ObjectTypeContractCreateRequestV2(
                apiName="Order",
                status="DRAFT",
                backingSources=[{"dataset_id": "ds-1"}],
            ),
            request=request,
            branch="main",
            expected_head_commit=None,
            oms_client=oms_client,
            dataset_registry=SimpleNamespace(),
            objectify_registry=SimpleNamespace(),
        )
    finally:
        router_v2._require_domain_role = original_require
        router_v2.object_type_contract_service.create_object_type_contract = original_create

    assert isinstance(response, dict)
    assert response["apiName"] == "Order"
    captured_body = captured["body"]
    assert captured_body.class_id == "Order"
    assert captured_body.status == "DRAFT"
    assert captured_body.backing_sources == [{"dataset_id": "ds-1"}]
    assert captured["expected_head_commit"] == "branch:main"


@pytest.mark.asyncio
async def test_update_object_type_v2_routes_to_contract_service():
    request = Request({"type": "http", "headers": []})
    oms_client = _FakeOMSClient()
    original_require = router_v2._require_domain_role
    original_update = router_v2.object_type_contract_service.update_object_type_contract
    captured: dict[str, Any] = {}

    async def _fake_update_object_type_contract(**kwargs):  # noqa: ANN003
        captured.update(kwargs)
        return SimpleNamespace(
            data={
                "object_type": {
                    "id": "Order",
                    "spec": {
                        "status": "ACTIVE",
                        "pk_spec": {"primary_key": ["order_id"], "title_key": ["order_id"]},
                        "backing_source": {"dataset_id": "ds-1"},
                    },
                }
            }
        )

    router_v2._require_domain_role = _noop_require_domain_role
    router_v2.object_type_contract_service.update_object_type_contract = _fake_update_object_type_contract
    try:
        response = await router_v2.update_object_type_v2(
            ontologyRid="test_db",
            objectTypeApiName="Order",
            body=router_v2.ObjectTypeContractUpdateRequestV2(
                status="ACTIVE",
                primaryKey="order_id",
                titleProperty="order_id",
                backingSource={"dataset_id": "ds-1"},
            ),
            request=request,
            branch="main",
            expected_head_commit=None,
            oms_client=oms_client,
            dataset_registry=SimpleNamespace(),
            objectify_registry=SimpleNamespace(),
        )
    finally:
        router_v2._require_domain_role = original_require
        router_v2.object_type_contract_service.update_object_type_contract = original_update

    assert isinstance(response, dict)
    assert response["apiName"] == "Order"
    captured_body = captured["body"]
    assert captured["class_id"] == "Order"
    assert captured_body.status == "ACTIVE"
    assert captured_body.pk_spec == {"primary_key": ["order_id"], "title_key": ["order_id"]}
    assert captured_body.backing_sources == [{"dataset_id": "ds-1"}]
    assert captured["expected_head_commit"] == "branch:main"


@pytest.mark.asyncio
async def test_list_ontologies_v2_accepts_apiname_source_rows():
    request = Request({"type": "http", "headers": []})
    response = await router_v2.list_ontologies_v2(
        request=request,
        oms_client=_ApiNameOnlyOMSClient(),
    )

    assert isinstance(response, dict)
    assert response["data"] == [{"apiName": "finance", "displayName": "Finance"}]


@pytest.mark.asyncio
async def test_get_ontology_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_ontology_v2(
            ontologyRid="test_db",
            request=request,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "test_db"
    assert response["displayName"] == "test_db"
    assert response["description"] == "test_db ontology"


@pytest.mark.asyncio
async def test_get_ontology_v2_resolves_rid_identifier():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_ontology_v2(
            ontologyRid="ri.ontology.test_db",
            request=request,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "test_db"


@pytest.mark.asyncio
async def test_get_ontology_v2_unknown_returns_ontology_not_found():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_ontology_v2(
            ontologyRid="unknown_ontology",
            request=request,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "LinkTypeNotFound"
    assert payload["errorName"] == "OntologyNotFound"


@pytest.mark.asyncio
async def test_get_ontology_v2_permission_denied_returns_foundry_403():
    request = Request({"type": "http", "headers": []})
    original_enforce = router_v2.enforce_database_role

    async def _deny_permission(*args, **kwargs):  # noqa: ANN002, ANN003
        _ = args, kwargs
        raise ValueError("Permission denied")

    router_v2.enforce_database_role = _deny_permission
    try:
        response = await router_v2.get_ontology_v2(
            ontologyRid="test_db",
            request=request,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2.enforce_database_role = original_enforce

    assert isinstance(response, JSONResponse)
    assert response.status_code == 403
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "PERMISSION_DENIED"
    assert payload["errorName"] == "PermissionDenied"


@pytest.mark.asyncio
async def test_get_ontology_v2_non_permission_role_error_returns_invalid_argument():
    request = Request({"type": "http", "headers": []})
    original_enforce = router_v2.enforce_database_role

    async def _invalid_role(*args, **kwargs):  # noqa: ANN002, ANN003
        _ = args, kwargs
        raise ValueError("invalid role policy")

    router_v2.enforce_database_role = _invalid_role
    try:
        response = await router_v2.get_ontology_v2(
            ontologyRid="test_db",
            request=request,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2.enforce_database_role = original_enforce

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"
    assert payload["errorName"] == "InvalidArgument"


@pytest.mark.asyncio
async def test_get_ontology_v2_preflight_upstream_error_returns_upstream_contract():
    request = Request({"type": "http", "headers": []})
    response = await router_v2.get_ontology_v2(
        ontologyRid="test_db",
        request=request,
        oms_client=_ListDatabasesStatusErrorOMSClient(),
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 503
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "UPSTREAM_ERROR"
    assert payload["errorName"] == "UpstreamError"


@pytest.mark.asyncio
async def test_get_full_metadata_v2_returns_foundry_full_metadata_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_full_metadata_v2(
            ontologyRid="test_db",
            request=request,
            preview=True,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["ontology"]["apiName"] == "test_db"
    assert response["branch"]["rid"] == "main"

    assert "Account" in response["objectTypes"]
    account_full_metadata = response["objectTypes"]["Account"]
    assert account_full_metadata["objectType"]["apiName"] == "Account"
    assert account_full_metadata["linkTypes"][0]["apiName"] == "owned_by"
    assert account_full_metadata["implementsInterfaces"] == ["Auditable"]
    assert account_full_metadata["implementsInterfaces2"]["Auditable"] == {
        "propertyMapping": {"created_at": "created_at"}
    }
    assert account_full_metadata["sharedPropertyTypeMapping"]["TenantScope"] == "tenant_scope"

    assert "ApproveAccount" in response["actionTypes"]
    assert "findAccounts:1.0.0" in response["queryTypes"]
    assert response["queryTypes"]["findAccounts:1.0.0"]["version"] == "1.0.0"
    assert "Auditable" in response["interfaceTypes"]
    assert "TenantScope" in response["sharedPropertyTypes"]
    assert "Money" in response["valueTypes"]


@pytest.mark.asyncio
async def test_get_full_metadata_v2_requires_preview_true():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_full_metadata_v2(
            ontologyRid="test_db",
            request=request,
            preview=False,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"
    assert payload["errorName"] == "ApiFeaturePreviewUsageOnly"


@pytest.mark.asyncio
async def test_get_full_metadata_v2_omits_partial_entities_when_upstream_unavailable():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_full_metadata_v2(
            ontologyRid="test_db",
            request=request,
            preview=True,
            branch="main",
            oms_client=_FullMetadataPartialFailureOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert "Account" in response["objectTypes"]
    assert response["actionTypes"] == {}
    assert "findAccounts:1.0.0" in response["queryTypes"]


@pytest.mark.asyncio
async def test_list_action_types_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_action_types_v2(
            ontologyRid="test_db",
            request=request,
            page_size=10,
            page_token=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"][0]["apiName"] == "ApproveAccount"
    assert response["data"][0]["displayName"] == "Approve Account"
    assert response["data"][0]["permissionModel"] == "ontology_roles"
    assert response["data"][0]["editsBeyondActions"] is False
    assert response["data"][0]["dynamicSecurity"]["projectPolicyInheritance"]["enabled"] is True
    assert "nextPageToken" in response


@pytest.mark.asyncio
async def test_list_action_types_v2_falls_back_to_legacy_action_rows() -> None:
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_action_types_v2(
            ontologyRid="test_db",
            request=request,
            page_size=10,
            page_token=None,
            branch="main",
            oms_client=_LegacyActionResourceOnlyOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"][0]["apiName"] == "HoldOrder"
    assert response["data"][0]["displayName"] == "Hold Order"


@pytest.mark.asyncio
async def test_get_action_type_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_action_type_v2(
            ontologyRid="test_db",
            actionTypeApiName="ApproveAccount",
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "ApproveAccount"
    assert response["displayName"] == "Approve Account"
    assert response["permissionModel"] == "ontology_roles"
    assert response["editsBeyondActions"] is False
    assert response["dynamicSecurity"]["projectPolicyInheritance"]["scope"] == "action_access"


@pytest.mark.asyncio
async def test_get_action_type_v2_falls_back_to_legacy_action_rows() -> None:
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_action_type_v2(
            ontologyRid="test_db",
            actionTypeApiName="HoldOrder",
            request=request,
            branch="main",
            oms_client=_LegacyActionResourceOnlyOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "HoldOrder"
    assert response["displayName"] == "Hold Order"


@pytest.mark.asyncio
async def test_get_action_type_by_rid_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_action_type_by_rid_v2(
            ontologyRid="test_db",
            actionTypeRid="ri.action-type.approve-account",
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "ApproveAccount"
    assert response["displayName"] == "Approve Account"


@pytest.mark.asyncio
async def test_get_action_type_by_rid_v2_falls_back_to_legacy_action_rows() -> None:
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_action_type_by_rid_v2(
            ontologyRid="test_db",
            actionTypeRid="ri.action-type.hold-order",
            request=request,
            branch="main",
            oms_client=_LegacyActionResourceOnlyOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "HoldOrder"


@pytest.mark.asyncio
async def test_apply_action_v2_forwards_to_oms_v2_apply_with_foundry_path():
    request = Request(
        {
            "type": "http",
            "headers": [
                (b"x-user-id", b"alice"),
                (b"x-user-type", b"service"),
            ],
        }
    )
    fake_client = _FakeOMSClient()
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.apply_action_v2(
            ontologyRid="test_db",
            actionApiName="ApproveAccount",
            body=router_v2.ApplyActionRequestV2(
                parameters={"ticket": {"class_id": "Ticket", "instance_id": "t1"}},
            ),
            request=request,
            branch="main",
            sdk_package_rid=None,
            sdk_version=None,
            transaction_id=None,
            oms_client=fake_client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert response["validation"]["result"] == "VALID"
    assert response["parameters"]["ticket"]["result"] == "VALID"
    assert response["parameters"]["ticket"]["required"] is True
    assert response["auditLogId"] is None
    assert response["sideEffectDelivery"] is None
    assert response["writebackStatus"] == "not_configured"
    assert fake_client.last_post is not None
    assert fake_client.last_post[0] == "/api/v2/ontologies/test_db/actions/ApproveAccount/apply"
    submit_payload = fake_client.last_post[1]["json"]
    assert submit_payload["parameters"]["ticket"]["instance_id"] == "t1"
    assert submit_payload["options"]["mode"] == "VALIDATE_AND_EXECUTE"
    assert submit_payload["metadata"]["user_id"] == "alice"
    assert submit_payload["metadata"]["user_type"] == "service"
    assert fake_client.last_post[1]["params"]["branch"] == "main"
    assert "preview" not in fake_client.last_post[1]["params"]
    assert "validate" not in fake_client.last_post[1]["params"]


@pytest.mark.asyncio
async def test_apply_action_v2_validate_only_maps_to_oms_v2_apply():
    request = Request(
        {
            "type": "http",
            "headers": [
                (b"x-user-id", b"alice"),
                (b"x-user-type", b"user"),
            ],
        }
    )
    fake_client = _FakeOMSClient()
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.apply_action_v2(
            ontologyRid="test_db",
            actionApiName="ApproveAccount",
            body=router_v2.ApplyActionRequestV2(
                options=router_v2.ApplyActionRequestOptionsV2(mode="VALIDATE_ONLY"),
                parameters={"ticket": {"class_id": "Ticket", "instance_id": "t1"}},
            ),
            request=request,
            branch="main",
            sdk_package_rid=None,
            sdk_version=None,
            transaction_id=None,
            oms_client=fake_client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert response["validation"]["result"] == "VALID"
    assert response["parameters"]["ticket"]["result"] == "VALID"
    assert response["parameters"]["ticket"]["required"] is True
    assert response["validation"]["submissionCriteria"] == []
    assert response["auditLogId"] is None
    assert response["sideEffectDelivery"] is None
    assert response["writebackStatus"] == "not_configured"
    assert fake_client.last_post is not None
    assert fake_client.last_post[0] == "/api/v2/ontologies/test_db/actions/ApproveAccount/apply"
    simulate_payload = fake_client.last_post[1]["json"]
    assert simulate_payload["options"]["mode"] == "VALIDATE_ONLY"
    assert simulate_payload["metadata"]["user_id"] == "alice"
    assert "validate" not in fake_client.last_post[1]["params"]


@pytest.mark.asyncio
async def test_apply_action_v2_emits_standardized_action_evidence_fields():
    request = Request(
        {
            "type": "http",
            "headers": [
                (b"x-user-id", b"alice"),
                (b"x-user-type", b"user"),
            ],
        }
    )
    fake_client = _ActionEvidenceOMSClient()
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.apply_action_v2(
            ontologyRid="test_db",
            actionApiName="ApproveAccount",
            body=router_v2.ApplyActionRequestV2(parameters={"ticket": {"instance_id": "t1"}}),
            request=request,
            branch="main",
            sdk_package_rid=None,
            sdk_version=None,
            transaction_id=None,
            oms_client=fake_client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert response["auditLogId"] == "log-123"
    assert response["action_log_id"] == "log-123"
    assert response["sideEffectDelivery"] == {"status": "SENT"}
    assert response["writebackStatus"] == "confirmed"


@pytest.mark.asyncio
async def test_apply_action_batch_v2_forwards_requests_to_submit_batch():
    request = Request(
        {
            "type": "http",
            "headers": [
                (b"x-user-id", b"alice"),
                (b"x-user-type", b"service"),
            ],
        }
    )
    fake_client = _FakeOMSClient()
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.apply_action_batch_v2(
            ontologyRid="test_db",
            actionApiName="ApproveAccount",
            body=router_v2.BatchApplyActionRequestV2(
                requests=[
                    router_v2.BatchApplyActionRequestItemV2(parameters={"ticket": {"instance_id": "t1"}}),
                    router_v2.BatchApplyActionRequestItemV2(parameters={"ticket": {"instance_id": "t2"}}),
                ]
            ),
            request=request,
            branch="main",
            sdk_package_rid=None,
            sdk_version=None,
            oms_client=fake_client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert response == {}
    assert fake_client.last_post is not None
    assert fake_client.last_post[0] == "/api/v2/ontologies/test_db/actions/ApproveAccount/applyBatch"
    submit_payload = fake_client.last_post[1]["json"]
    assert len(submit_payload["requests"]) == 2
    assert submit_payload["requests"][0]["parameters"]["ticket"]["instance_id"] == "t1"
    assert submit_payload["requests"][1]["parameters"]["ticket"]["instance_id"] == "t2"
    assert submit_payload["metadata"]["user_id"] == "alice"
    assert fake_client.last_post[1]["params"]["branch"] == "main"
    assert "preview" not in fake_client.last_post[1]["params"]


@pytest.mark.asyncio
async def test_apply_action_batch_v2_forwards_return_edits_option():
    request = Request(
        {
            "type": "http",
            "headers": [
                (b"x-user-id", b"alice"),
                (b"x-user-type", b"service"),
            ],
        }
    )
    fake_client = _FakeOMSClient()
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        await router_v2.apply_action_batch_v2(
            ontologyRid="test_db",
            actionApiName="ApproveAccount",
            body=router_v2.BatchApplyActionRequestV2(
                options=router_v2.BatchApplyActionRequestOptionsV2(returnEdits="ALL"),
                requests=[router_v2.BatchApplyActionRequestItemV2(parameters={"ticket": {"instance_id": "t1"}})],
            ),
            request=request,
            branch="main",
            sdk_package_rid=None,
            sdk_version=None,
            oms_client=fake_client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert fake_client.last_post is not None
    submit_payload = fake_client.last_post[1]["json"]
    assert submit_payload["options"]["returnEdits"] == "ALL"


@pytest.mark.asyncio
async def test_count_objects_v2_forwards_to_oms_count_path():
    request = Request({"type": "http", "headers": []})
    fake_client = _FakeOMSClient()
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.count_objects_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            request=request,
            branch="main",
            sdk_package_rid=None,
            sdk_version=None,
            oms_client=fake_client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert response == {"count": 7}
    assert fake_client.last_post is not None
    assert fake_client.last_post[0] == "/api/v2/ontologies/test_db/objects/Account/count"
    assert fake_client.last_post[1]["params"]["branch"] == "main"


@pytest.mark.asyncio
async def test_apply_action_v2_normalizes_non_foundry_validation_error():
    class _ValidationErrorOMSClient(_FakeOMSClient):
        async def post(self, path, **kwargs):  # noqa: ANN003
            self.last_post = (path, kwargs)
            request = httpx.Request("POST", f"http://test{path}")
            response = httpx.Response(
                400,
                request=request,
                json={
                    "code": "REQUEST_VALIDATION_FAILED",
                    "category": "input",
                    "message": "invalid parameters",
                    "status": 400,
                },
            )
            raise httpx.HTTPStatusError("validation failed", request=request, response=response)

    request = Request({"type": "http", "headers": []})
    fake_client = _ValidationErrorOMSClient()
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.apply_action_v2(
            ontologyRid="test_db",
            actionApiName="ApproveAccount",
            body=router_v2.ApplyActionRequestV2(parameters={"ticket": {"instance_id": "t1"}}),
            request=request,
            branch="main",
            sdk_package_rid=None,
            sdk_version=None,
            transaction_id=None,
            oms_client=fake_client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "ACTION_VALIDATION_FAILED"
    assert payload["errorName"] == "ActionValidationFailed"


@pytest.mark.asyncio
async def test_apply_action_batch_v2_normalizes_non_foundry_validation_error():
    class _ValidationErrorOMSClient(_FakeOMSClient):
        async def post(self, path, **kwargs):  # noqa: ANN003
            self.last_post = (path, kwargs)
            request = httpx.Request("POST", f"http://test{path}")
            response = httpx.Response(
                400,
                request=request,
                json={
                    "code": "REQUEST_VALIDATION_FAILED",
                    "category": "input",
                    "message": "invalid batch parameters",
                    "status": 400,
                },
            )
            raise httpx.HTTPStatusError("validation failed", request=request, response=response)

    request = Request({"type": "http", "headers": []})
    fake_client = _ValidationErrorOMSClient()
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.apply_action_batch_v2(
            ontologyRid="test_db",
            actionApiName="ApproveAccount",
            body=router_v2.BatchApplyActionRequestV2(
                requests=[router_v2.BatchApplyActionRequestItemV2(parameters={"ticket": {"instance_id": "t1"}})],
            ),
            request=request,
            branch="main",
            sdk_package_rid=None,
            sdk_version=None,
            oms_client=fake_client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "ACTION_VALIDATION_FAILED"
    assert payload["errorName"] == "ActionValidationFailed"


@pytest.mark.asyncio
async def test_list_query_types_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_query_types_v2(
            ontologyRid="test_db",
            request=request,
            page_size=10,
            page_token=None,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"][0]["apiName"] == "findAccounts"
    assert response["data"][0]["displayName"] == "Find Accounts"
    assert response["data"][0]["version"] == "1.0.0"


@pytest.mark.asyncio
async def test_get_query_type_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_query_type_v2(
            ontologyRid="test_db",
            queryApiName="findAccounts",
            request=request,
            version="1.0.0",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "findAccounts"
    assert response["displayName"] == "Find Accounts"
    assert response["version"] == "1.0.0"


@pytest.mark.asyncio
async def test_get_query_type_v2_mismatched_version_returns_not_found():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_query_type_v2(
            ontologyRid="test_db",
            queryApiName="findAccounts",
            request=request,
            version="9.9.9",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "QueryTypeNotFound"


@pytest.mark.asyncio
async def test_execute_query_v2_runs_function_query_and_returns_value_envelope():
    request = Request({"type": "http", "headers": []})
    fake_client = _FakeOMSClient()
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.execute_query_v2(
            ontologyRid="test_db",
            queryApiName="findAccounts",
            body=router_v2.ExecuteQueryRequestV2(parameters={"accountId": "acc-1"}),
            request=request,
            version="1.0.0",
            sdk_package_rid=None,
            sdk_version=None,
            transaction_id=None,
            oms_client=fake_client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert "value" in response
    assert response["value"]["data"][0]["account_id"] == "acc-1"
    assert fake_client.last_post is not None
    assert fake_client.last_post[0] == "/api/v2/ontologies/test_db/objects/Account/search"
    assert fake_client.last_post[1]["json"]["where"]["value"] == "acc-1"


@pytest.mark.asyncio
async def test_execute_query_v2_missing_required_parameter_returns_invalid_argument():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.execute_query_v2(
            ontologyRid="test_db",
            queryApiName="findAccounts",
            body=router_v2.ExecuteQueryRequestV2(parameters={}),
            request=request,
            version="1.0.0",
            sdk_package_rid=None,
            sdk_version=None,
            transaction_id=None,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"
    assert payload["errorName"] == "InvalidArgument"


@pytest.mark.asyncio
async def test_execute_query_v2_without_execution_spec_returns_invalid_argument():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.execute_query_v2(
            ontologyRid="test_db",
            queryApiName="findAccounts",
            body=router_v2.ExecuteQueryRequestV2(parameters={"accountId": "acc-1"}),
            request=request,
            version="1.0.0",
            sdk_package_rid=None,
            sdk_version=None,
            transaction_id=None,
            oms_client=_MissingQueryExecutionSpecOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"
    assert payload["errorName"] == "InvalidArgument"


@pytest.mark.asyncio
async def test_execute_query_v2_prefers_canonical_object_type_field_over_fallback():
    request = Request({"type": "http", "headers": []})
    fake_client = _CanonicalQueryObjectTypeOMSClient()
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.execute_query_v2(
            ontologyRid="test_db",
            queryApiName="findAccounts",
            body=router_v2.ExecuteQueryRequestV2(parameters={"accountId": "acc-1"}),
            request=request,
            version="1.0.0",
            sdk_package_rid=None,
            sdk_version=None,
            transaction_id=None,
            oms_client=fake_client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["value"]["data"][0]["account_id"] == "acc-1"
    assert fake_client.last_post is not None
    assert fake_client.last_post[0] == "/api/v2/ontologies/test_db/objects/CanonicalAccount/search"


@pytest.mark.asyncio
async def test_list_interface_types_v2_requires_preview_true():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_interface_types_v2(
            ontologyRid="test_db",
            request=request,
            preview=False,
            page_size=10,
            page_token=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"
    assert payload["errorName"] == "ApiFeaturePreviewUsageOnly"


@pytest.mark.asyncio
async def test_list_interface_types_v2_returns_foundry_raw_shape_with_preview():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_interface_types_v2(
            ontologyRid="test_db",
            request=request,
            preview=True,
            page_size=10,
            page_token=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"][0]["apiName"] == "Auditable"
    assert response["data"][0]["displayName"] == "Auditable"


@pytest.mark.asyncio
async def test_list_shared_property_types_v2_requires_preview_true():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_shared_property_types_v2(
            ontologyRid="test_db",
            request=request,
            preview=False,
            page_size=10,
            page_token=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"
    assert payload["errorName"] == "ApiFeaturePreviewUsageOnly"


@pytest.mark.asyncio
async def test_list_shared_property_types_v2_returns_foundry_raw_shape_with_preview():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_shared_property_types_v2(
            ontologyRid="test_db",
            request=request,
            preview=True,
            page_size=10,
            page_token=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"][0]["apiName"] == "TenantScope"
    assert response["data"][0]["displayName"] == "Tenant Scope"
    assert "nextPageToken" in response


@pytest.mark.asyncio
async def test_get_shared_property_type_v2_returns_foundry_raw_shape_with_preview():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_shared_property_type_v2(
            ontologyRid="test_db",
            sharedPropertyTypeApiName="TenantScope",
            request=request,
            preview=True,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "TenantScope"
    assert response["displayName"] == "Tenant Scope"


@pytest.mark.asyncio
async def test_get_shared_property_type_v2_missing_returns_not_found():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_shared_property_type_v2(
            ontologyRid="test_db",
            sharedPropertyTypeApiName="MissingShared",
            request=request,
            preview=True,
            branch="main",
            oms_client=_MissingSharedPropertyOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "SharedPropertyTypeNotFound"


@pytest.mark.asyncio
async def test_list_value_types_v2_requires_preview_true():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_value_types_v2(
            ontologyRid="test_db",
            request=request,
            preview=False,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"
    assert payload["errorName"] == "ApiFeaturePreviewUsageOnly"


@pytest.mark.asyncio
async def test_get_value_type_v2_returns_foundry_raw_shape_with_preview():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_value_type_v2(
            ontologyRid="test_db",
            valueTypeApiName="Money",
            request=request,
            preview=True,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "Money"
    assert response["displayName"] == "Money"


@pytest.mark.asyncio
async def test_list_value_types_v2_returns_foundry_raw_shape_without_pagination_token():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_value_types_v2(
            ontologyRid="test_db",
            request=request,
            preview=True,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"][0]["apiName"] == "Money"
    assert "nextPageToken" not in response


@pytest.mark.asyncio
async def test_get_object_type_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_object_type_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "Account"
    assert response["titleProperty"] == "account_id"
    assert response["visibility"] == "NORMAL"


@pytest.mark.asyncio
async def test_get_object_type_full_metadata_v2_returns_foundry_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_object_type_full_metadata_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            request=request,
            branch="main",
            preview=True,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["objectType"]["apiName"] == "Account"
    assert response["linkTypes"][0]["apiName"] == "owned_by"
    assert response["implementsInterfaces"] == ["Auditable"]
    assert response["implementsInterfaces2"]["Auditable"]["propertyMapping"]["created_at"] == "created_at"
    assert response["sharedPropertyTypeMapping"]["TenantScope"] == "tenant_scope"


@pytest.mark.asyncio
async def test_get_object_type_full_metadata_v2_missing_returns_object_type_not_found():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_object_type_full_metadata_v2(
            ontologyRid="test_db",
            objectTypeApiName="MissingType",
            request=request,
            branch="main",
            preview=True,
            oms_client=_MissingObjectTypeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "ObjectTypeNotFound"


@pytest.mark.asyncio
async def test_get_object_type_v2_missing_returns_object_type_not_found():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_object_type_v2(
            ontologyRid="test_db",
            objectTypeApiName="MissingType",
            request=request,
            branch="main",
            oms_client=_MissingObjectTypeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "ObjectTypeNotFound"


@pytest.mark.asyncio
async def test_list_outgoing_link_types_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_outgoing_link_types_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            request=request,
            page_size=10,
            page_token=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert [row["apiName"] for row in response["data"]] == ["owned_by"]
    assert response["data"][0]["objectTypeApiName"] == "User"
    assert response["data"][0]["cardinality"] == "ONE"


@pytest.mark.asyncio
async def test_list_outgoing_link_types_v2_filters_before_pagination():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    client = _PagedLinkTypesOMSClient()
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        first = await router_v2.list_outgoing_link_types_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            request=request,
            page_size=2,
            page_token=None,
            branch="main",
            oms_client=client,
        )
        second = await router_v2.list_outgoing_link_types_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            request=request,
            page_size=2,
            page_token=first.get("nextPageToken"),
            branch="main",
            oms_client=client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert [row["apiName"] for row in first["data"]] == ["rel_498", "rel_501"]
    assert first.get("nextPageToken")
    assert [row["apiName"] for row in second["data"]] == ["rel_504"]
    assert second.get("nextPageToken") is None


@pytest.mark.asyncio
async def test_list_outgoing_link_types_v2_rejects_page_token_when_page_size_changes():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    client = _PagedLinkTypesOMSClient()
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        first = await router_v2.list_outgoing_link_types_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            request=request,
            page_size=1,
            page_token=None,
            branch="main",
            oms_client=client,
        )
        second = await router_v2.list_outgoing_link_types_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            request=request,
            page_size=2,
            page_token=first.get("nextPageToken"),
            branch="main",
            oms_client=client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(second, JSONResponse)
    assert second.status_code == 400
    payload = json.loads(second.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"


@pytest.mark.asyncio
async def test_get_outgoing_link_type_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_outgoing_link_type_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            linkTypeApiName="owned_by",
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "owned_by"
    assert response["displayName"] == "Owned By"
    assert response["status"] == "ACTIVE"


@pytest.mark.asyncio
async def test_get_outgoing_link_type_v2_missing_returns_link_type_not_found():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_outgoing_link_type_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            linkTypeApiName="unknown_link",
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "LinkTypeNotFound"


# ---------------------------------------------------------------------------
# Incoming Link Types
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_incoming_link_types_v2_returns_foundry_shape():
    """``User`` is the *target* of ``owned_by`` (Account→User).

    The incoming perspective should show ``objectTypeApiName == 'Account'``
    and inverted cardinality: original ``n:1`` ⇒ incoming ``MANY``.
    """
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_incoming_link_types_v2(
            ontologyRid="test_db",
            objectTypeApiName="User",
            request=request,
            page_size=10,
            page_token=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert [row["apiName"] for row in response["data"]] == ["owned_by"]
    assert response["data"][0]["objectTypeApiName"] == "Account"
    assert response["data"][0]["cardinality"] == "MANY"


@pytest.mark.asyncio
async def test_list_incoming_link_types_v2_excludes_non_matching():
    """``Account`` is not a *target* of any link, so incoming should be empty."""
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_incoming_link_types_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            request=request,
            page_size=10,
            page_token=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"] == []


@pytest.mark.asyncio
async def test_list_incoming_link_types_v2_filters_before_pagination():
    """``User`` is the target of 3 link types in _PagedLinkTypesOMSClient
    (rel_498, rel_501, rel_504). Page through them in pages of 2."""
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    client = _PagedLinkTypesOMSClient()
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        first = await router_v2.list_incoming_link_types_v2(
            ontologyRid="test_db",
            objectTypeApiName="User",
            request=request,
            page_size=2,
            page_token=None,
            branch="main",
            oms_client=client,
        )
        second = await router_v2.list_incoming_link_types_v2(
            ontologyRid="test_db",
            objectTypeApiName="User",
            request=request,
            page_size=2,
            page_token=first.get("nextPageToken"),
            branch="main",
            oms_client=client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert [row["apiName"] for row in first["data"]] == ["rel_498", "rel_501"]
    assert first.get("nextPageToken")
    assert [row["apiName"] for row in second["data"]] == ["rel_504"]
    assert second.get("nextPageToken") is None


@pytest.mark.asyncio
async def test_get_incoming_link_type_v2_returns_foundry_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_incoming_link_type_v2(
            ontologyRid="test_db",
            objectTypeApiName="User",
            linkTypeApiName="owned_by",
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["apiName"] == "owned_by"
    assert response["objectTypeApiName"] == "Account"
    assert response["cardinality"] == "MANY"
    assert response["status"] == "ACTIVE"


@pytest.mark.asyncio
async def test_get_incoming_link_type_v2_wrong_target_returns_not_found():
    """``Account`` is not a *target* of ``owned_by``, so this should be 404."""
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_incoming_link_type_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            linkTypeApiName="owned_by",
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "LinkTypeNotFound"


@pytest.mark.asyncio
async def test_get_incoming_link_type_v2_unknown_link_returns_not_found():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_incoming_link_type_v2(
            ontologyRid="test_db",
            objectTypeApiName="User",
            linkTypeApiName="nonexistent_link",
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "LinkTypeNotFound"


@pytest.mark.asyncio
async def test_search_objects_v2_passthrough_foundry_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.search_objects_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            payload={"where": {"type": "eq", "field": "status", "value": "ACTIVE"}, "pageSize": 10},
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["totalCount"] == "1"
    assert response["data"][0]["instance_id"] == "i-1"


@pytest.mark.asyncio
async def test_list_objects_v2_passthrough_foundry_shape_and_orderby_parse():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    fake = _FakeOMSClient()
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_objects_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            request=request,
            page_size=10,
            page_token="MTA",
            select=["account_id", "name"],
            order_by="properties.account_id:desc,p.name:asc",
            exclude_rid=True,
            snapshot=False,
            branch="main",
            oms_client=fake,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["totalCount"] == "1"
    assert response["data"][0]["account_id"] == "acc-1"
    assert fake.last_post is not None
    _, kwargs = fake.last_post
    assert kwargs["params"]["branch"] == "main"
    assert kwargs["json"]["pageSize"] == 10
    assert kwargs["json"]["pageToken"] == "MTA"
    assert kwargs["json"]["select"] == ["account_id", "name"]
    assert kwargs["json"]["excludeRid"] is True
    assert kwargs["json"]["snapshot"] is False
    assert kwargs["json"]["orderBy"] == {
        "orderType": "fields",
        "fields": [
            {"field": "account_id", "direction": "desc"},
            {"field": "name", "direction": "asc"},
        ],
    }


@pytest.mark.asyncio
async def test_get_object_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_object_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            primaryKey="acc-1",
            request=request,
            select=["account_id", "name"],
            exclude_rid=True,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["account_id"] == "acc-1"
    assert response["name"] == "Alice Account"


@pytest.mark.asyncio
async def test_get_object_v2_not_found_returns_foundry_error():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_object_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            primaryKey="acc-404",
            request=request,
            select=None,
            exclude_rid=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "ObjectNotFound"
    assert payload["parameters"]["primaryKey"] == "acc-404"


@pytest.mark.asyncio
async def test_list_linked_objects_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    fake = _FakeOMSClient()
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_linked_objects_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            primaryKey="acc-1",
            linkTypeApiName="owned_by",
            request=request,
            page_size=10,
            page_token=None,
            select=["user_id", "name"],
            order_by="properties.user_id:asc",
            exclude_rid=True,
            snapshot=False,
            branch="main",
            oms_client=fake,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["data"][0]["user_id"] == "u-1"
    assert response.get("nextPageToken") is None
    assert fake.last_post is not None
    _, kwargs = fake.last_post
    assert kwargs["json"]["where"] == {"type": "eq", "field": "user_id", "value": "u-1"}


@pytest.mark.asyncio
async def test_list_linked_objects_v2_paginates_after_dedup_link_filter():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    fake = _LinkedPaginationOMSClient()
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        first = await router_v2.list_linked_objects_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            primaryKey="acc-1",
            linkTypeApiName="owned_by",
            request=request,
            page_size=2,
            page_token=None,
            select=None,
            order_by=None,
            exclude_rid=None,
            snapshot=None,
            branch="main",
            oms_client=fake,
        )
        second = await router_v2.list_linked_objects_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            primaryKey="acc-1",
            linkTypeApiName="owned_by",
            request=request,
            page_size=2,
            page_token=first.get("nextPageToken"),
            select=None,
            order_by=None,
            exclude_rid=None,
            snapshot=None,
            branch="main",
            oms_client=fake,
        )
        third = await router_v2.list_linked_objects_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            primaryKey="acc-1",
            linkTypeApiName="owned_by",
            request=request,
            page_size=2,
            page_token=second.get("nextPageToken"),
            select=None,
            order_by=None,
            exclude_rid=None,
            snapshot=None,
            branch="main",
            oms_client=fake,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert [row["user_id"] for row in first["data"]] == ["u-1", "u-2"]
    assert [row["user_id"] for row in second["data"]] == ["u-3", "u-4"]
    assert [row["user_id"] for row in third["data"]] == ["u-5"]
    assert first.get("nextPageToken")
    assert second.get("nextPageToken")
    assert third.get("nextPageToken") is None


@pytest.mark.asyncio
async def test_list_linked_objects_v2_rejects_page_token_when_page_size_changes():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    fake = _LinkedPaginationOMSClient()
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        first = await router_v2.list_linked_objects_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            primaryKey="acc-1",
            linkTypeApiName="owned_by",
            request=request,
            page_size=2,
            page_token=None,
            select=None,
            order_by=None,
            exclude_rid=None,
            snapshot=None,
            branch="main",
            oms_client=fake,
        )
        second = await router_v2.list_linked_objects_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            primaryKey="acc-1",
            linkTypeApiName="owned_by",
            request=request,
            page_size=3,
            page_token=first.get("nextPageToken"),
            select=None,
            order_by=None,
            exclude_rid=None,
            snapshot=None,
            branch="main",
            oms_client=fake,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(second, JSONResponse)
    assert second.status_code == 400
    payload = json.loads(second.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"


@pytest.mark.asyncio
async def test_list_linked_objects_v2_missing_link_type_returns_link_type_not_found():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_linked_objects_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            primaryKey="acc-1",
            linkTypeApiName="unknown_link",
            request=request,
            page_size=10,
            page_token=None,
            select=None,
            order_by=None,
            exclude_rid=None,
            snapshot=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "LinkTypeNotFound"


@pytest.mark.asyncio
async def test_get_linked_object_v2_returns_foundry_raw_shape():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_linked_object_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            primaryKey="acc-1",
            linkTypeApiName="owned_by",
            linkedObjectPrimaryKey="u-1",
            request=request,
            select=["user_id", "name"],
            exclude_rid=True,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["user_id"] == "u-1"
    assert response["name"] == "Owner User"


@pytest.mark.asyncio
async def test_get_linked_object_v2_not_found_returns_foundry_error():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    fake = _FakeOMSClient()
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_linked_object_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            primaryKey="acc-1",
            linkTypeApiName="owned_by",
            linkedObjectPrimaryKey="u-404",
            request=request,
            select=None,
            exclude_rid=None,
            branch="main",
            oms_client=fake,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"
    assert payload["errorName"] == "LinkedObjectNotFound"
    assert payload["parameters"]["linkedObjectPrimaryKey"] == "u-404"
    assert fake.last_post is not None
    assert fake.last_post[0].endswith("/ontologies/test_db/objects/Account/search")


@pytest.mark.asyncio
async def test_load_object_set_objects_v2_supports_search_around(
    monkeypatch: pytest.MonkeyPatch,
):
    captured_routing: dict[str, Any] = {}

    async def _capture_routing(**kwargs: Any) -> None:  # noqa: ANN401
        captured_routing.update(dict(kwargs.get("decision_metadata") or {}))

    monkeypatch.setattr(router_v2, "_audit_search_around_compute_routing", _capture_routing)
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.load_object_set_objects_v2(
            ontologyRid="test_db",
            payload={
                "objectSet": {
                    "type": "searchAround",
                    "link": "owned_by",
                    "objectSet": {
                        "objectType": "Account",
                        "where": {"type": "eq", "field": "account_id", "value": "acc-1"},
                    },
                },
                "select": ["user_id", "name"],
                "pageSize": 10,
            },
            request=request,
            branch="main",
            transaction_id=None,
            sdk_package_rid=None,
            sdk_version=None,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["totalCount"] == "1"
    assert response["data"][0]["user_id"] == "u-1"
    assert response["data"][0]["name"] == "Owner User"
    assert captured_routing["route"] == "search_around"
    assert captured_routing["selected_backend"] == "index_pruning"
    assert captured_routing["execution_backend"] == "index_pruning"


@pytest.mark.asyncio
async def test_load_object_set_objects_v2_search_around_routes_to_spark_on_threshold(
    monkeypatch: pytest.MonkeyPatch,
):
    captured_routing: dict[str, Any] = {}

    async def _capture_routing(**kwargs: Any) -> None:  # noqa: ANN401
        captured_routing.update(dict(kwargs.get("decision_metadata") or {}))

    monkeypatch.setenv("ONTOLOGY_SEARCH_AROUND_SPARK_THRESHOLD", "1")
    monkeypatch.setattr(router_v2, "_audit_search_around_compute_routing", _capture_routing)

    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.load_object_set_objects_v2(
            ontologyRid="test_db",
            payload={
                "objectSet": {
                    "type": "searchAround",
                    "link": "owned_by",
                    "objectSet": {
                        "objectType": "Account",
                        "where": {"type": "eq", "field": "account_id", "value": "acc-1"},
                    },
                },
                "select": ["user_id", "name"],
                "pageSize": 10,
            },
            request=request,
            branch="main",
            transaction_id=None,
            sdk_package_rid=None,
            sdk_version=None,
            oms_client=_LinkedPaginationOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, dict)
    assert response["totalCount"] == "5"
    assert captured_routing["route"] == "search_around"
    assert captured_routing["estimated_count"] == 5
    assert captured_routing["threshold"] == 1
    assert captured_routing["selected_backend"] == "spark_on_demand"
    assert captured_routing["execution_backend"] == "spark_on_demand"
    assert captured_routing["spark_job_id"]


@pytest.mark.asyncio
async def test_load_object_set_objects_or_interfaces_v2_search_around_invalid_link_returns_not_found():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.load_object_set_objects_or_interfaces_v2(
            ontologyRid="test_db",
            payload={
                "objectSet": {
                    "type": "searchAround",
                    "link": "contains",
                    "objectSet": {"objectType": "Account"},
                },
                "select": ["user_id"],
                "pageSize": 10,
            },
            request=request,
            branch="main",
            preview=True,
            sdk_package_rid=None,
            sdk_version=None,
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "NOT_FOUND"


@pytest.mark.asyncio
async def test_load_object_set_objects_v2_resolves_temporary_object_set_rid(
    monkeypatch: pytest.MonkeyPatch,
):
    request = Request({"type": "http", "headers": []})
    captured: dict[str, Any] = {}

    async def _fake_load_temporary_object_set(object_set_rid: str, *, redis_service):  # noqa: ANN001
        captured["rid"] = object_set_rid
        captured["redis_service"] = redis_service
        return {"objectType": "Account"}

    redis_service = object()
    original_require = router_v2._require_read_role
    router_v2._require_read_role = _noop_require_domain_role
    monkeypatch.setattr(router_v2, "_load_temporary_object_set", _fake_load_temporary_object_set)
    try:
        response = await router_v2.load_object_set_objects_v2(
            ontologyRid="test_db",
            payload={
                "objectSet": "ri.object-set.main.versioned-object-set.temp-1",
                "select": ["account_id"],
                "pageSize": 10,
            },
            request=request,
            branch="main",
            transaction_id=None,
            sdk_package_rid=None,
            sdk_version=None,
            oms_client=_FakeOMSClient(),
            redis_service=redis_service,  # type: ignore[arg-type]
        )
    finally:
        router_v2._require_read_role = original_require

    assert isinstance(response, dict)
    assert response["totalCount"] == "1"
    assert captured["rid"] == "ri.object-set.main.versioned-object-set.temp-1"
    assert captured["redis_service"] is redis_service


@pytest.mark.asyncio
async def test_v2_invalid_ontology_returns_foundry_error_envelope():
    request = Request({"type": "http", "headers": []})
    response = await router_v2.list_object_types_v2(
        ontologyRid="INVALID NAME",
        request=request,
        page_size=10,
        page_token=None,
        branch="main",
        oms_client=_FakeOMSClient(),
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"
    assert payload["errorName"] == "InvalidArgument"
    assert isinstance(payload["errorInstanceId"], str)
    assert payload["parameters"]["ontology"] == "INVALID NAME"


@pytest.mark.asyncio
async def test_list_objects_v2_rejects_invalid_orderby_expression():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_objects_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            request=request,
            page_size=10,
            page_token=None,
            select=None,
            order_by="account_id:desc",
            exclude_rid=None,
            snapshot=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"


@pytest.mark.asyncio
async def test_list_objects_v2_rejects_invalid_branch():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    fake = _FakeOMSClient()
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.list_objects_v2(
            ontologyRid="test_db",
            objectTypeApiName="Account",
            request=request,
            page_size=10,
            page_token=None,
            select=None,
            order_by=None,
            exclude_rid=None,
            snapshot=None,
            branch="invalid$branch",
            oms_client=fake,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"
    assert fake.last_post is None


@pytest.mark.asyncio
async def test_list_object_types_v2_rejects_expired_page_token():
    request = Request({"type": "http", "headers": []})
    issued_at = 0
    expired_token = encode_offset_page_token(10, issued_at=issued_at)

    response = await router_v2.list_object_types_v2(
        ontologyRid="test_db",
        request=request,
        page_size=10,
        page_token=expired_token,
        branch="main",
        oms_client=_FakeOMSClient(),
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"


@pytest.mark.asyncio
async def test_list_object_types_v2_rejects_page_token_scope_mismatch():
    request = Request({"type": "http", "headers": []})
    mismatched_scope_token = encode_offset_page_token(10, scope="v2/objectTypes|other_db|main")

    response = await router_v2.list_object_types_v2(
        ontologyRid="test_db",
        request=request,
        page_size=10,
        page_token=mismatched_scope_token,
        branch="main",
        oms_client=_FakeOMSClient(),
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"


@pytest.mark.asyncio
async def test_list_object_types_v2_rejects_page_token_when_page_size_changes():
    request = Request({"type": "http", "headers": []})
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        first = await router_v2.list_object_types_v2(
            ontologyRid="test_db",
            request=request,
            page_size=1,
            page_token=None,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
        second = await router_v2.list_object_types_v2(
            ontologyRid="test_db",
            request=request,
            page_size=2,
            page_token=first.get("nextPageToken"),
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(second, JSONResponse)
    assert second.status_code == 400
    payload = json.loads(second.body.decode("utf-8"))
    assert payload["errorCode"] == "INVALID_ARGUMENT"


@pytest.mark.asyncio
async def test_timeseries_first_point_v2_requires_domain_role_and_returns_payload():
    request = Request({"type": "http", "headers": []})
    calls: list[str] = []

    async def _capture_require_domain_role(req, *, db_name):  # noqa: ANN001, ANN003
        _ = req
        calls.append(db_name)
        return None

    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _capture_require_domain_role
    try:
        response = await router_v2.get_timeseries_first_point_v2(
            ontologyRid="test_db",
            objectTypeApiName="Sensor",
            primaryKey="sensor-001",
            property="temperature",
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["time"] == "2024-01-01T00:00:00Z"
    assert calls == ["test_db"]


@pytest.mark.asyncio
async def test_timeseries_first_point_v2_forwards_actor_headers_to_oms():
    request = Request(
        {
            "type": "http",
            "headers": [
                (b"x-user-id", b"alice"),
                (b"x-user-type", b"user"),
                (b"x-user-roles", b"domain_editor"),
            ],
        }
    )
    oms_client = _FakeOMSClient()
    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _noop_require_domain_role
    try:
        response = await router_v2.get_timeseries_first_point_v2(
            ontologyRid="test_db",
            objectTypeApiName="Sensor",
            primaryKey="sensor-001",
            property="temperature",
            request=request,
            branch="main",
            oms_client=oms_client,
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    headers = oms_client.last_call_headers.get("timeseries_first_point") or {}
    assert headers.get("X-User-ID") == "alice"
    assert headers.get("X-User-Type") == "user"
    assert headers.get("X-User-Roles") == "domain_editor"


@pytest.mark.asyncio
async def test_upload_attachment_v2_proxies_binary_payload_to_oms():
    request = _request_with_body(b"data", path="/api/v2/ontologies/attachments/upload")
    response = await router_v2.upload_attachment_v2(
        request=request,
        filename="doc.txt",
        oms_client=_FakeOMSClient(),
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["filename"] == "doc.txt"
    assert payload["rid"].startswith("ri.attachments.main.attachment.")


@pytest.mark.asyncio
async def test_upload_attachment_v2_forwards_actor_headers_to_oms():
    request = _request_with_body(
        b"data",
        path="/api/v2/ontologies/attachments/upload",
        headers=[
            (b"x-user-id", b"alice"),
            (b"x-user-type", b"user"),
            (b"x-user-roles", b"domain_editor"),
        ],
    )
    oms_client = _FakeOMSClient()
    response = await router_v2.upload_attachment_v2(
        request=request,
        filename="doc.txt",
        oms_client=oms_client,
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    headers = oms_client.last_call_headers.get("attachments_upload") or {}
    assert headers.get("X-User-ID") == "alice"
    assert headers.get("X-User-Type") == "user"
    assert headers.get("X-User-Roles") == "domain_editor"


@pytest.mark.asyncio
async def test_attachment_property_v2_requires_domain_role_and_returns_payload():
    request = Request({"type": "http", "headers": []})
    calls: list[str] = []

    async def _capture_require_domain_role(req, *, db_name):  # noqa: ANN001, ANN003
        _ = req
        calls.append(db_name)
        return None

    original_require = router_v2._require_domain_role
    router_v2._require_domain_role = _capture_require_domain_role
    try:
        response = await router_v2.list_attachment_property_v2(
            ontologyRid="test_db",
            objectTypeApiName="Employee",
            primaryKey="emp-001",
            property="resume",
            request=request,
            branch="main",
            oms_client=_FakeOMSClient(),
        )
    finally:
        router_v2._require_domain_role = original_require

    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["type"] == "single"
    assert calls == ["test_db"]
