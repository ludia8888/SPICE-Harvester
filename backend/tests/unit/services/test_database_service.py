from __future__ import annotations

import pytest
from fastapi import HTTPException, Request

from bff.services import database_service
from bff.services.database_service import delete_database
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
