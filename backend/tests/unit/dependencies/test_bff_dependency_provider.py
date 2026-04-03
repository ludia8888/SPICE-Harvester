from __future__ import annotations

import pytest
from fastapi import HTTPException

from bff.dependencies import BFFDependencyProvider
from bff.services.oms_client import OMSClient
from shared.services.registries.action_log_registry import ActionLogRegistry


class _FailingContainer:
    def __init__(self, service_type: type) -> None:
        self._service_type = service_type

    def has(self, service_type: type) -> bool:
        return service_type is self._service_type

    def register_singleton(self, service_type: type, factory) -> None:  # noqa: ANN001
        _ = service_type, factory

    async def get(self, service_type: type):
        assert service_type is self._service_type
        raise RuntimeError("container wiring failed")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_oms_client_surfaces_internal_wiring_failure() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await BFFDependencyProvider.get_oms_client(container=_FailingContainer(OMSClient))

    assert exc_info.value.status_code == 500
    assert "wiring failed" in str(exc_info.value.detail)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_action_log_registry_surfaces_internal_wiring_failure() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await BFFDependencyProvider.get_action_log_registry(container=_FailingContainer(ActionLogRegistry))

    assert exc_info.value.status_code == 500
    assert "wiring failed" in str(exc_info.value.detail)
