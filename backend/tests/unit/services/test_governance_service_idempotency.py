from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from bff.schemas.governance_requests import CreateBackingDatasourceRequest, CreateKeySpecRequest
from bff.services import governance_service


class _UniqueViolationError(RuntimeError):
    sqlstate = "23505"


def _request() -> Request:
    return Request({"type": "http", "headers": []})


@pytest.mark.asyncio
async def test_create_backing_datasource_returns_existing_when_create_races(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset = SimpleNamespace(
        dataset_id="dataset-1",
        db_name="demo",
        name="orders",
        description="Orders",
        source_type="dataset",
        source_ref="dataset://orders",
        branch="main",
    )
    existing = SimpleNamespace(backing_id="backing-1")
    calls = {"lookup": 0, "create": 0}

    class _Registry:
        async def get_dataset(self, *, dataset_id: str):
            assert dataset_id == dataset.dataset_id
            return dataset

        async def get_backing_datasource_by_dataset(self, *, dataset_id: str, branch: str):
            calls["lookup"] += 1
            assert dataset_id == dataset.dataset_id
            assert branch == dataset.branch
            return existing if calls["lookup"] >= 2 else None

        async def create_backing_datasource(self, **kwargs):  # noqa: ANN003
            calls["create"] += 1
            raise _UniqueViolationError("duplicate key value violates unique constraint")

    async def _allow(*args, **kwargs):  # noqa: ANN002, ANN003
        return None

    monkeypatch.setattr(governance_service, "enforce_db_scope_or_403", lambda request, db_name: None)
    monkeypatch.setattr(governance_service, "enforce_required_database_role", _allow)

    response = await governance_service.create_backing_datasource(
        body=CreateBackingDatasourceRequest(dataset_id=dataset.dataset_id),
        request=_request(),
        dataset_registry=_Registry(),
    )

    assert response.message == "Backing datasource already exists"
    assert response.data == {"backing_datasource": existing.__dict__}
    assert calls == {"lookup": 2, "create": 1}


@pytest.mark.asyncio
async def test_create_key_spec_returns_existing_when_create_races_same_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset = SimpleNamespace(dataset_id="dataset-1", db_name="demo")
    existing = SimpleNamespace(
        key_spec_id="key-spec-1",
        dataset_id=dataset.dataset_id,
        dataset_version_id=None,
        spec={
            "primary_key": ["customer_id"],
            "title_key": ["customer_id"],
            "unique_keys": [],
            "nullable_fields": [],
            "required_fields": [],
        },
        status="ACTIVE",
    )
    calls = {"get_or_create": 0}

    class _Registry:
        async def get_dataset(self, *, dataset_id: str):
            assert dataset_id == dataset.dataset_id
            return dataset

        async def get_or_create_key_spec(self, *, dataset_id: str, dataset_version_id: str | None = None, spec: dict):
            calls["get_or_create"] += 1
            assert dataset_id == dataset.dataset_id
            assert dataset_version_id is None
            assert spec["primary_key"] == ["customer_id"]
            return existing, False

    async def _allow(*args, **kwargs):  # noqa: ANN002, ANN003
        return None

    monkeypatch.setattr(governance_service, "enforce_db_scope_or_403", lambda request, db_name: None)
    monkeypatch.setattr(governance_service, "enforce_required_database_role", _allow)

    response = await governance_service.create_key_spec(
        body=CreateKeySpecRequest(dataset_id=dataset.dataset_id, primary_key=["customer_id"], title_key=["customer_id"]),
        request=_request(),
        dataset_registry=_Registry(),
    )

    assert response.message == "Key spec already exists"
    assert response.data == {"key_spec": existing.__dict__}
    assert calls == {"get_or_create": 1}


@pytest.mark.asyncio
async def test_create_key_spec_raises_conflict_when_raced_existing_spec_differs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset = SimpleNamespace(dataset_id="dataset-1", db_name="demo")
    existing = SimpleNamespace(
        key_spec_id="key-spec-1",
        dataset_id=dataset.dataset_id,
        dataset_version_id=None,
        spec={
            "primary_key": ["other_id"],
            "title_key": ["other_id"],
            "unique_keys": [],
            "nullable_fields": [],
            "required_fields": [],
        },
        status="ACTIVE",
    )
    calls = {"get_or_create": 0}

    class _Registry:
        async def get_dataset(self, *, dataset_id: str):
            assert dataset_id == dataset.dataset_id
            return dataset

        async def get_or_create_key_spec(self, *, dataset_id: str, dataset_version_id: str | None = None, spec: dict):
            calls["get_or_create"] += 1
            assert dataset_id == dataset.dataset_id
            assert dataset_version_id is None
            assert spec["primary_key"] == ["customer_id"]
            return existing, False

    async def _allow(*args, **kwargs):  # noqa: ANN002, ANN003
        return None

    monkeypatch.setattr(governance_service, "enforce_db_scope_or_403", lambda request, db_name: None)
    monkeypatch.setattr(governance_service, "enforce_required_database_role", _allow)

    with pytest.raises(HTTPException) as exc_info:
        await governance_service.create_key_spec(
            body=CreateKeySpecRequest(dataset_id=dataset.dataset_id, primary_key=["customer_id"], title_key=["customer_id"]),
            request=_request(),
            dataset_registry=_Registry(),
        )

    assert exc_info.value.status_code == 409
    assert calls == {"get_or_create": 1}
