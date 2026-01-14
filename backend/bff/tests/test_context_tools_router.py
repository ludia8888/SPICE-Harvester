from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from bff.routers.context_tools import DatasetDescribeRequest, OntologySnapshotRequest, describe_datasets, snapshot_ontology
from shared.security.user_context import UserPrincipal


class _Request:
    def __init__(self, *, principal: UserPrincipal) -> None:
        self.state = SimpleNamespace(user=principal)


def _principal(*, tenant_id: str = "tenant-1") -> UserPrincipal:
    return UserPrincipal(id="user-1", tenant_id=tenant_id, verified=True)


class _FakePolicyRegistry:
    def __init__(self, *, data_policies: dict | None = None) -> None:
        self._data_policies = dict(data_policies or {})

    async def get_tenant_policy(self, *, tenant_id: str):  # noqa: ANN001
        _ = tenant_id
        return SimpleNamespace(data_policies=dict(self._data_policies))


class _FakeDatasetRegistry:
    async def list_datasets(self, *, db_name: str, branch: str | None = None):  # noqa: ANN001
        _ = branch
        now = datetime.now(timezone.utc)
        return [
            {
                "dataset_id": "ds-1",
                "db_name": db_name,
                "name": "sales",
                "branch": "main",
                "source_type": "csv_upload",
                "description": None,
                "schema_json": {"columns": [{"name": "id", "type": "xsd:string"}]},
                "sample_json": {"rows": [{"id": "1"}]},
                "latest_commit_id": "c1",
                "row_count": 1,
                "updated_at": now,
            },
            {
                "dataset_id": "ds-2",
                "db_name": db_name,
                "name": "customers",
                "branch": "main",
                "source_type": "csv_upload",
                "description": None,
                "schema_json": {"columns": [{"name": "id", "type": "xsd:string"}]},
                "sample_json": {"rows": [{"id": "1"}]},
                "latest_commit_id": "c2",
                "row_count": 1,
                "updated_at": now,
            },
        ]


class _FakeOMSClient:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def get(self, path: str, *, params=None):  # noqa: ANN001
        self.calls.append({"path": path, "params": dict(params or {})})
        return {"data": {"ontology_id": "ont-1", "ok": True}}


@pytest.mark.asyncio
async def test_context_tools_dataset_describe_enforces_db_policy() -> None:
    req = _Request(principal=_principal())
    policy = _FakePolicyRegistry(data_policies={"allowed_db_names": ["allowed-db"]})
    datasets = _FakeDatasetRegistry()

    with pytest.raises(HTTPException) as exc:
        await describe_datasets(
            body=DatasetDescribeRequest(db_name="blocked-db", dataset_ids=["ds-1"]),
            request=req,  # type: ignore[arg-type]
            policy_registry=policy,  # type: ignore[arg-type]
            dataset_registry=datasets,  # type: ignore[arg-type]
        )
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_context_tools_dataset_describe_constrains_allowed_dataset_ids() -> None:
    req = _Request(principal=_principal())
    policy = _FakePolicyRegistry(data_policies={"allowed_db_names": ["demo"], "allowed_dataset_ids": ["ds-1"]})
    datasets = _FakeDatasetRegistry()

    resp = await describe_datasets(
        body=DatasetDescribeRequest(db_name="demo"),
        request=req,  # type: ignore[arg-type]
        policy_registry=policy,  # type: ignore[arg-type]
        dataset_registry=datasets,  # type: ignore[arg-type]
    )
    assert resp.status == "success"
    ctx = resp.data and resp.data["context"]
    assert ctx and ctx["db_name"] == "demo"
    assert all(item["dataset_id"] == "ds-1" for item in ctx.get("datasets_overview") or [])


@pytest.mark.asyncio
async def test_context_tools_ontology_snapshot_enforces_policy() -> None:
    req = _Request(principal=_principal())
    policy = _FakePolicyRegistry(
        data_policies={
            "allowed_db_names": ["demo"],
            "allowed_branches": ["main"],
            "allowed_ontology_ids": ["ont-1"],
        }
    )
    oms = _FakeOMSClient()

    resp = await snapshot_ontology(
        body=OntologySnapshotRequest(db_name="demo", ontology_id="ont-1", branch="main"),
        request=req,  # type: ignore[arg-type]
        oms_client=oms,  # type: ignore[arg-type]
        policy_registry=policy,  # type: ignore[arg-type]
    )
    assert resp.status == "success"
    assert resp.data and resp.data["ontology"]["ok"] is True
