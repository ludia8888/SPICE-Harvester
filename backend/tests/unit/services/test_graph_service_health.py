"""Tests for graph_service_health (ES-only, no TerminusDB)."""
from __future__ import annotations

from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock

import pytest

from bff.services.graph_query_service import graph_service_health


def _make_graph_service(*, es_status: str = "green") -> MagicMock:
    """Create a mock GraphFederationServiceES with ES health."""
    svc = MagicMock()
    svc._ensure_connected = AsyncMock()
    svc._es = MagicMock()
    svc._es.get_cluster_health = AsyncMock(return_value={"status": es_status})
    return svc


@pytest.mark.asyncio
async def test_health_green() -> None:
    svc = _make_graph_service(es_status="green")
    result = await graph_service_health(graph_service=svc)

    assert result["status"] == "healthy"
    assert result["services"]["elasticsearch"] == "green"
    assert "operational" in result["message"]


@pytest.mark.asyncio
async def test_health_yellow() -> None:
    svc = _make_graph_service(es_status="yellow")
    result = await graph_service_health(graph_service=svc)

    assert result["status"] == "healthy"
    assert result["services"]["elasticsearch"] == "yellow"


@pytest.mark.asyncio
async def test_health_red() -> None:
    svc = _make_graph_service(es_status="red")
    result = await graph_service_health(graph_service=svc)

    assert result["status"] == "degraded"
    assert result["services"]["elasticsearch"] == "red"
    assert "degraded" in result["message"].lower()


@pytest.mark.asyncio
async def test_health_connection_failure() -> None:
    svc = _make_graph_service()
    svc._ensure_connected = AsyncMock(side_effect=RuntimeError("connection refused"))

    result = await graph_service_health(graph_service=svc)

    assert result["status"] == "unhealthy"
    assert "error" in result
    assert "connection refused" in result["error"]
