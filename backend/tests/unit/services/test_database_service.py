from __future__ import annotations

import pytest
from fastapi import HTTPException, Request

from bff.services import database_service
from bff.services.database_service import create_database, delete_database, list_databases
from shared.models.requests import DatabaseCreateRequest
from shared.security.database_access import DatabaseAccessRegistryUnavailableError
from shared.errors.infra_errors import UpstreamUnavailableError


class _FailingOMS:
    async def delete_database(self, db_name: str, *, expected_seq: int):  # noqa: ANN201
        _ = db_name, expected_seq
        raise UpstreamUnavailableError(
            "boom",
            service="oms",
            operation="DeleteDatabase",
            path="/api/v1/database/demo",
        )


def _request() -> Request:
    return Request({"type": "http", "method": "DELETE", "path": "/", "headers": []})


def _request_with_actor() -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/",
            "headers": [
                (b"x-user-type", b"user"),
                (b"x-user-id", b"u1"),
                (b"x-user-name", b"User 1"),
            ],
        }
    )


@pytest.mark.asyncio
async def test_delete_database_returns_503_when_oms_transport_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(database_service, "_is_dev_mode", lambda: True)

    with pytest.raises(HTTPException) as exc_info:
        await delete_database(
            db_name="demo",
            http_request=_request(),
            expected_seq=1,
            oms=_FailingOMS(),
        )

    assert exc_info.value.status_code == 503


class _ListingOMS:
    async def list_databases(self):  # noqa: ANN201
        return {"data": {"databases": [{"name": "demo"}]}}


class _DatasetRegistry:
    async def count_datasets_by_db_names(self, *, db_names: list[str]):  # noqa: ANN201
        return {name: 0 for name in db_names}


class _CreateOMS:
    async def create_database(self, name: str, description: str | None):  # noqa: ANN201
        _ = name, description
        return {"status": "accepted", "command_id": "cmd-1"}


@pytest.mark.asyncio
async def test_list_databases_returns_503_when_registry_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _raise_registry(*, db_names):  # noqa: ANN001
        _ = db_names
        raise DatabaseAccessRegistryUnavailableError("Database access registry unavailable")

    monkeypatch.setattr(database_service, "fetch_database_access_entries", _raise_registry)

    with pytest.raises(HTTPException) as exc_info:
        await list_databases(
            request=_request_with_actor(),
            oms=_ListingOMS(),  # type: ignore[arg-type]
            dataset_registry=_DatasetRegistry(),  # type: ignore[arg-type]
        )

    assert exc_info.value.status_code == 503


@pytest.mark.asyncio
async def test_create_database_still_returns_202_when_owner_sync_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _raise_owner_sync(**kwargs):  # noqa: ANN001
        _ = kwargs
        raise DatabaseAccessRegistryUnavailableError("Database access registry unavailable")

    monkeypatch.setattr(database_service, "upsert_database_owner", _raise_owner_sync)

    response = await create_database(
        body=DatabaseCreateRequest(name="demo", description="desc"),
        http_request=_request_with_actor(),
        oms=_CreateOMS(),  # type: ignore[arg-type]
    )

    assert response.status_code == 202
