"""
Unit tests for schema_changes router.
"""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from bff.routers.schema_changes import (
    SubscriptionCreateRequest,
    AcknowledgeRequest,
    CompatibilityCheckRequest,
)


class TestSchemaChangesModels:
    """Test request/response models"""

    def test_subscription_create_request_valid(self) -> None:
        """SubscriptionCreateRequest should accept valid data"""
        req = SubscriptionCreateRequest(
            subject_type="dataset",
            subject_id="ds-123",
            db_name="test_db",
            severity_filter=["warning", "breaking"],
            notification_channels=["websocket"],
        )
        assert req.subject_type == "dataset"
        assert req.subject_id == "ds-123"
        assert req.db_name == "test_db"
        assert "warning" in req.severity_filter

    def test_subscription_create_request_defaults(self) -> None:
        """SubscriptionCreateRequest should have sensible defaults"""
        req = SubscriptionCreateRequest(
            subject_type="mapping_spec",
            subject_id="ms-456",
            db_name="prod_db",
        )
        assert req.severity_filter == ["warning", "breaking"]
        assert req.notification_channels == ["websocket"]

    def test_subscription_create_request_invalid_subject_type(self) -> None:
        """SubscriptionCreateRequest should reject invalid subject types"""
        with pytest.raises(ValueError):
            SubscriptionCreateRequest(
                subject_type="invalid_type",
                subject_id="ds-123",
                db_name="test_db",
            )

    def test_acknowledge_request(self) -> None:
        """AcknowledgeRequest should require acknowledged_by"""
        req = AcknowledgeRequest(acknowledged_by="user@example.com")
        assert req.acknowledged_by == "user@example.com"

    def test_compatibility_check_request_optional_version(self) -> None:
        """CompatibilityCheckRequest should allow optional version"""
        req = CompatibilityCheckRequest()
        assert req.dataset_version_id is None

        req_with_version = CompatibilityCheckRequest(dataset_version_id="v-789")
        assert req_with_version.dataset_version_id == "v-789"


class TestSchemaChangesEndpoints:
    """Test endpoint logic (mocked database)"""

    @pytest.fixture
    def mock_pool(self):
        """Create a mock database pool"""
        pool = MagicMock()
        conn = AsyncMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        return pool, conn

    @pytest.fixture
    def mock_registry(self, mock_pool):
        """Create a mock dataset registry with pool"""
        pool, conn = mock_pool
        registry = MagicMock()
        registry._pool = pool
        return registry, conn

    @pytest.mark.asyncio
    async def test_list_schema_changes_empty(self, mock_registry) -> None:
        """list_schema_changes should return empty list when no history"""
        registry, conn = mock_registry
        conn.fetch = AsyncMock(return_value=[])

        with patch("bff.routers.schema_changes.get_dataset_registry", AsyncMock(return_value=registry)):
            from bff.routers.schema_changes import list_schema_changes

            result = await list_schema_changes(db_name="test_db")

            assert result["status"] == "success"
            assert result["data"]["count"] == 0
            assert result["data"]["items"] == []

    @pytest.mark.asyncio
    async def test_list_schema_changes_with_results(self, mock_registry) -> None:
        """list_schema_changes should return formatted results"""
        registry, conn = mock_registry
        now = datetime.now(timezone.utc)
        conn.fetch = AsyncMock(return_value=[
            {
                "drift_id": "d1d1d1d1-d1d1-d1d1-d1d1-d1d1d1d1d1d1",
                "subject_type": "dataset",
                "subject_id": "ds-123",
                "db_name": "test_db",
                "previous_hash": "abc",
                "current_hash": "def",
                "drift_type": "column_added",
                "severity": "info",
                "changes": [{"change_type": "column_added", "column_name": "new_col"}],
                "detected_at": now,
                "acknowledged_at": None,
                "acknowledged_by": None,
            }
        ])

        with patch("bff.routers.schema_changes.get_dataset_registry", AsyncMock(return_value=registry)):
            from bff.routers.schema_changes import list_schema_changes

            result = await list_schema_changes(db_name="test_db")

            assert result["status"] == "success"
            assert result["data"]["count"] == 1
            item = result["data"]["items"][0]
            assert item["drift_type"] == "column_added"
            assert item["severity"] == "info"

    @pytest.mark.asyncio
    async def test_list_schema_changes_with_filters(self, mock_registry) -> None:
        """list_schema_changes should apply filters correctly"""
        registry, conn = mock_registry
        conn.fetch = AsyncMock(return_value=[])

        with patch("bff.routers.schema_changes.get_dataset_registry", AsyncMock(return_value=registry)):
            from bff.routers.schema_changes import list_schema_changes

            await list_schema_changes(
                db_name="test_db",
                subject_type="mapping_spec",
                severity="breaking",
            )

            # Verify query was called with filter parameters
            call_args = conn.fetch.call_args
            query = call_args[0][0]
            assert "subject_type" in query
            assert "severity" in query

    @pytest.mark.asyncio
    async def test_acknowledge_drift_success(self, mock_registry) -> None:
        """acknowledge_drift should update drift record"""
        registry, conn = mock_registry
        conn.fetchrow = AsyncMock(return_value={"drift_id": "d1d1d1d1-d1d1-d1d1-d1d1-d1d1d1d1d1d1"})

        with patch("bff.routers.schema_changes.get_dataset_registry", AsyncMock(return_value=registry)):
            from bff.routers.schema_changes import acknowledge_drift

            result = await acknowledge_drift(
                drift_id="d1d1d1d1-d1d1-d1d1-d1d1-d1d1d1d1d1d1",
                request=AcknowledgeRequest(acknowledged_by="admin@test.com"),
            )

            assert result["status"] == "success"
            assert result["data"]["acknowledged_by"] == "admin@test.com"

    @pytest.mark.asyncio
    async def test_acknowledge_drift_not_found(self, mock_registry) -> None:
        """acknowledge_drift should return 404 for unknown drift"""
        registry, conn = mock_registry
        conn.fetchrow = AsyncMock(return_value=None)

        with patch("bff.routers.schema_changes.get_dataset_registry", AsyncMock(return_value=registry)):
            from bff.routers.schema_changes import acknowledge_drift
            from fastapi import HTTPException

            with pytest.raises(HTTPException) as exc_info:
                await acknowledge_drift(
                    drift_id="nonexistent",
                    request=AcknowledgeRequest(acknowledged_by="admin@test.com"),
                )

            assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_create_subscription_success(self, mock_registry) -> None:
        """create_subscription should create new subscription"""
        registry, conn = mock_registry
        now = datetime.now(timezone.utc)
        conn.fetchrow = AsyncMock(return_value={
            "subscription_id": "s1s1s1s1-s1s1-s1s1-s1s1-s1s1s1s1s1s1",
            "created_at": now,
        })

        mock_request = MagicMock()
        mock_request.headers.get.return_value = "test-user"

        with patch("bff.routers.schema_changes.get_dataset_registry", AsyncMock(return_value=registry)):
            from bff.routers.schema_changes import create_subscription

            result = await create_subscription(
                request=mock_request,
                body=SubscriptionCreateRequest(
                    subject_type="dataset",
                    subject_id="ds-123",
                    db_name="test_db",
                ),
            )

            assert result["status"] == "success"
            assert result["data"]["status"] == "ACTIVE"
            assert result["data"]["user_id"] == "test-user"

    @pytest.mark.asyncio
    async def test_delete_subscription_success(self, mock_registry) -> None:
        """delete_subscription should soft-delete subscription"""
        registry, conn = mock_registry
        conn.fetchrow = AsyncMock(return_value={"subscription_id": "s1s1s1s1-s1s1-s1s1-s1s1-s1s1s1s1s1s1"})

        mock_request = MagicMock()
        mock_request.headers.get.return_value = "test-user"

        with patch("bff.routers.schema_changes.get_dataset_registry", AsyncMock(return_value=registry)):
            from bff.routers.schema_changes import delete_subscription

            result = await delete_subscription(
                request=mock_request,
                subscription_id="s1s1s1s1-s1s1-s1s1-s1s1-s1s1s1s1s1s1",
            )

            assert result["status"] == "success"

    @pytest.mark.asyncio
    async def test_get_stats_empty(self, mock_registry) -> None:
        """get_schema_change_stats should return zero stats when empty"""
        registry, conn = mock_registry
        conn.fetch = AsyncMock(return_value=[])

        with patch("bff.routers.schema_changes.get_dataset_registry", AsyncMock(return_value=registry)):
            from bff.routers.schema_changes import get_schema_change_stats

            result = await get_schema_change_stats(db_name="test_db", days=30)

            assert result["status"] == "success"
            assert result["data"]["stats"]["total"] == 0

    @pytest.mark.asyncio
    async def test_get_stats_with_data(self, mock_registry) -> None:
        """get_schema_change_stats should aggregate by severity and type"""
        registry, conn = mock_registry
        conn.fetch = AsyncMock(return_value=[
            {"severity": "warning", "subject_type": "dataset", "count": 5, "unacknowledged": 2},
            {"severity": "breaking", "subject_type": "dataset", "count": 1, "unacknowledged": 1},
            {"severity": "info", "subject_type": "mapping_spec", "count": 10, "unacknowledged": 0},
        ])

        with patch("bff.routers.schema_changes.get_dataset_registry", AsyncMock(return_value=registry)):
            from bff.routers.schema_changes import get_schema_change_stats

            result = await get_schema_change_stats(db_name="test_db", days=30)

            stats = result["data"]["stats"]
            assert stats["total"] == 16
            assert stats["total_unacknowledged"] == 3
            assert stats["by_severity"]["warning"]["count"] == 5
            assert stats["by_severity"]["breaking"]["unacknowledged"] == 1


class TestMappingCompatibility:
    """Test mapping compatibility check endpoint"""

    @pytest.fixture
    def mock_registries(self):
        """Create mock registries"""
        dataset_registry = MagicMock()
        objectify_registry = MagicMock()
        dataset_registry.get_latest_version = AsyncMock(return_value=None)
        return dataset_registry, objectify_registry

    @pytest.mark.asyncio
    async def test_check_compatibility_no_drift(self, mock_registries) -> None:
        """check_mapping_compatibility should return compatible when no drift"""
        dataset_registry, objectify_registry = mock_registries

        # Mock mapping spec
        mapping_spec = MagicMock()
        mapping_spec.dataset_id = "ds-123"
        mapping_spec.schema_hash = "same-hash"
        objectify_registry.get_mapping_spec = AsyncMock(return_value=mapping_spec)

        # Mock dataset with same schema hash
        dataset = MagicMock()
        dataset.schema_json = {"columns": [{"name": "id", "type": "xsd:integer"}]}
        dataset_registry.get_dataset = AsyncMock(return_value=dataset)

        with patch("bff.routers.schema_changes.get_dataset_registry", AsyncMock(return_value=dataset_registry)), \
             patch("bff.routers.schema_changes.get_objectify_registry", AsyncMock(return_value=objectify_registry)), \
             patch("bff.routers.schema_changes.SchemaDriftDetector") as MockDetector:

            # Detector returns None (no drift)
            mock_detector = MagicMock()
            mock_detector.detect_drift.return_value = None
            MockDetector.return_value = mock_detector

            from bff.routers.schema_changes import check_mapping_compatibility

            result = await check_mapping_compatibility(
                mapping_spec_id="ms-123",
                db_name="test_db",
                dataset_version_id=None,  # Explicit None for direct function call
            )

            assert result["status"] == "success"
            assert result["data"]["is_compatible"] is True
            assert result["data"]["has_drift"] is False

    @pytest.mark.asyncio
    async def test_check_compatibility_with_breaking_drift(self, mock_registries) -> None:
        """check_mapping_compatibility should return incompatible on breaking drift"""
        dataset_registry, objectify_registry = mock_registries

        mapping_spec = MagicMock()
        mapping_spec.dataset_id = "ds-123"
        mapping_spec.schema_hash = "old-hash"
        objectify_registry.get_mapping_spec = AsyncMock(return_value=mapping_spec)

        dataset = MagicMock()
        dataset.schema_json = {"columns": [{"name": "new_id", "type": "xsd:string"}]}
        dataset_registry.get_dataset = AsyncMock(return_value=dataset)

        with patch("bff.routers.schema_changes.get_dataset_registry", AsyncMock(return_value=dataset_registry)), \
             patch("bff.routers.schema_changes.get_objectify_registry", AsyncMock(return_value=objectify_registry)), \
             patch("bff.routers.schema_changes.SchemaDriftDetector") as MockDetector:

            # Create a mock drift with breaking severity
            from shared.services.core.schema_drift_detector import SchemaDrift, SchemaChange
            mock_drift = SchemaDrift(
                subject_type="mapping_spec",
                subject_id="ms-123",
                db_name="test_db",
                previous_hash="old-hash",
                current_hash="new-hash",
                drift_type="column_removed",
                severity="breaking",
                changes=[
                    SchemaChange(
                        change_type="column_removed",
                        column_name="id",
                        old_value={"name": "id", "type": "xsd:integer"},
                        impact="mapping_invalid",
                    )
                ],
            )

            mock_detector = MagicMock()
            mock_detector.detect_drift.return_value = mock_drift
            MockDetector.return_value = mock_detector

            from bff.routers.schema_changes import check_mapping_compatibility

            result = await check_mapping_compatibility(
                mapping_spec_id="ms-123",
                db_name="test_db",
                dataset_version_id=None,  # Explicit None for direct function call
            )

            assert result["status"] == "success"
            assert result["data"]["is_compatible"] is False
            assert result["data"]["has_drift"] is True
            assert result["data"]["severity"] == "breaking"
            assert len(result["data"]["recommendations"]) > 0

    @pytest.mark.asyncio
    async def test_check_compatibility_mapping_not_found(self, mock_registries) -> None:
        """check_mapping_compatibility should return 404 for unknown mapping"""
        dataset_registry, objectify_registry = mock_registries
        objectify_registry.get_mapping_spec = AsyncMock(return_value=None)

        with patch("bff.routers.schema_changes.get_dataset_registry", AsyncMock(return_value=dataset_registry)), \
             patch("bff.routers.schema_changes.get_objectify_registry", AsyncMock(return_value=objectify_registry)):

            from bff.routers.schema_changes import check_mapping_compatibility
            from fastapi import HTTPException

            with pytest.raises(HTTPException) as exc_info:
                await check_mapping_compatibility(
                    mapping_spec_id="nonexistent",
                    db_name="test_db",
                )

            assert exc_info.value.status_code == 404
