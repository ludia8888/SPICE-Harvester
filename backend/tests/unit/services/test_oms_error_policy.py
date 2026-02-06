from __future__ import annotations

import logging

import httpx
import pytest
from fastapi import HTTPException

from bff.services.oms_error_policy import raise_oms_boundary_exception


def _http_status_error(status_code: int, *, body: dict | None = None) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "http://oms.local/test")
    response = httpx.Response(status_code, request=request, json=body or {"detail": f"status-{status_code}"})
    return httpx.HTTPStatusError("upstream error", request=request, response=response)


def test_raise_oms_boundary_exception_with_custom_http_status_detail() -> None:
    exc = _http_status_error(404)

    with pytest.raises(HTTPException) as raised:
        raise_oms_boundary_exception(
            exc=exc,
            action="온톨로지 목록 조회",
            logger=logging.getLogger(__name__),
            custom_http_status_details={404: "데이터베이스를 찾을 수 없습니다"},
        )

    assert raised.value.status_code == 404
    assert raised.value.detail == "데이터베이스를 찾을 수 없습니다"


def test_raise_oms_boundary_exception_maps_value_error_to_400() -> None:
    with pytest.raises(HTTPException) as raised:
        raise_oms_boundary_exception(
            exc=ValueError("invalid payload"),
            action="온톨로지 생성",
            logger=logging.getLogger(__name__),
        )
    assert raised.value.status_code == 400
    assert raised.value.detail == "invalid payload"


def test_raise_oms_boundary_exception_maps_generic_to_500() -> None:
    with pytest.raises(HTTPException) as raised:
        raise_oms_boundary_exception(
            exc=RuntimeError("boom"),
            action="온톨로지 생성",
            logger=logging.getLogger(__name__),
        )
    assert raised.value.status_code == 500
    assert "온톨로지 생성 실패" in raised.value.detail
