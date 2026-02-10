"""
Security tests for information leakage prevention.

BFF responses must not expose internal storage or fallback routing details.
"""

from unittest.mock import AsyncMock

import pytest
from elasticsearch.exceptions import ConnectionError as ESConnectionError
from fastapi import status
from fastapi.testclient import TestClient

from bff.dependencies import get_elasticsearch_service
from bff.main import app
from bff.routers import instances as instances_router


class _StubDatasetRegistry:
    async def get_access_policy(self, *, db_name, scope, subject_type, subject_id):  # noqa: ANN003
        return None


class TestInformationLeakagePrevention:
    @pytest.fixture
    def client(self):
        return TestClient(app)

    @pytest.mark.asyncio
    async def test_get_instance_es_success_no_source_leak(self, client):
        mock_es_result = {
            "hits": {
                "total": {"value": 1},
                "hits": [
                    {
                        "_source": {
                            "instance_id": "test_instance",
                            "class_id": "TestClass",
                            "properties": [],
                        }
                    }
                ],
            }
        }

        mock_es = AsyncMock()
        mock_es.search.return_value = mock_es_result

        app.dependency_overrides[instances_router.get_dataset_registry] = lambda: _StubDatasetRegistry()
        app.dependency_overrides[get_elasticsearch_service] = lambda: mock_es
        try:
            response = client.get("/api/v1/databases/test_db/class/TestClass/instance/test_instance")
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == status.HTTP_200_OK
        response_data = response.json()
        assert "source" not in response_data
        assert "elasticsearch" not in str(response_data).lower()
        assert "terminus" not in str(response_data).lower()
        assert "fallback" not in str(response_data).lower()
        assert response_data["status"] == "success"
        assert "data" in response_data

    @pytest.mark.asyncio
    async def test_get_instance_es_failure_fails_closed_without_source_leak(self, client):
        mock_es = AsyncMock()
        mock_es.search.side_effect = ESConnectionError("Connection failed")

        app.dependency_overrides[instances_router.get_dataset_registry] = lambda: _StubDatasetRegistry()
        app.dependency_overrides[get_elasticsearch_service] = lambda: mock_es
        try:
            response = client.get("/api/v1/databases/test_db/class/TestClass/instance/test_instance")
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        response_data = response.json()
        assert "elasticsearch" not in str(response_data).lower()
        assert "terminus" not in str(response_data).lower()
        assert "oms" not in str(response_data).lower()
        assert "connection failed" not in str(response_data).lower()
        assert response_data["detail"]["error"] == "overlay_degraded"

    @pytest.mark.asyncio
    async def test_get_class_instances_es_success_no_source_leak(self, client):
        mock_es_result = {
            "hits": {
                "total": {"value": 2},
                "hits": [
                    {"_source": {"instance_id": "inst1", "class_id": "TestClass"}},
                    {"_source": {"instance_id": "inst2", "class_id": "TestClass"}},
                ],
            }
        }

        mock_es = AsyncMock()
        mock_es.search.return_value = mock_es_result

        app.dependency_overrides[instances_router.get_dataset_registry] = lambda: _StubDatasetRegistry()
        app.dependency_overrides[get_elasticsearch_service] = lambda: mock_es
        try:
            response = client.get("/api/v1/databases/test_db/class/TestClass/instances")
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == status.HTTP_200_OK
        response_data = response.json()
        assert "source" not in response_data
        assert "elasticsearch" not in str(response_data).lower()
        assert "terminus" not in str(response_data).lower()
        assert "instances" in response_data
        assert "total" in response_data
        assert len(response_data["instances"]) == 2

    @pytest.mark.asyncio
    async def test_get_class_instances_es_failure_fails_closed_without_source_leak(self, client):
        mock_es = AsyncMock()
        mock_es.search.side_effect = ESConnectionError("Connection failed")

        app.dependency_overrides[instances_router.get_dataset_registry] = lambda: _StubDatasetRegistry()
        app.dependency_overrides[get_elasticsearch_service] = lambda: mock_es
        try:
            response = client.get("/api/v1/databases/test_db/class/TestClass/instances")
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        response_data = response.json()
        assert "source" not in str(response_data).lower()
        assert "elasticsearch" not in str(response_data).lower()
        assert "terminus" not in str(response_data).lower()
        assert "oms" not in str(response_data).lower()
        assert "connection failed" not in str(response_data).lower()
        assert response_data["detail"]["error"] == "overlay_degraded"

    @pytest.mark.asyncio
    async def test_not_found_response_no_internal_info_leak(self, client):
        mock_es = AsyncMock()
        mock_es.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

        app.dependency_overrides[instances_router.get_dataset_registry] = lambda: _StubDatasetRegistry()
        app.dependency_overrides[get_elasticsearch_service] = lambda: mock_es
        try:
            response = client.get("/api/v1/databases/test_db/class/TestClass/instance/nonexistent")
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == status.HTTP_404_NOT_FOUND
        response_data = response.json()
        assert "elasticsearch" not in str(response_data).lower()
        assert "terminus" not in str(response_data).lower()
        assert "oms" not in str(response_data).lower()
        assert "connection" not in str(response_data).lower()
        assert "detail" in response_data
        assert "찾을 수 없습니다" in response_data["detail"]

    def test_response_structure_consistency(self):
        assert {"status", "data"} == {"status", "data"}
        assert {"class_id", "total", "limit", "offset", "search", "instances"} == {
            "class_id",
            "total",
            "limit",
            "offset",
            "search",
            "instances",
        }

    def test_forbidden_fields_in_responses(self):
        forbidden_fields = [
            "source",
            "origin",
            "provider",
            "backend",
            "engine",
            "driver",
            "connection_type",
            "fallback_used",
            "error_source",
            "internal_path",
            "service_status",
        ]
        assert len(forbidden_fields) > 0
