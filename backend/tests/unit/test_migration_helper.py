"""
🔥 THINK ULTRA! Migration Helper Unit Tests

Tests for the dual-write migration pattern from PostgreSQL to S3/MinIO
"""

import pytest
import asyncio
import os
from unittest.mock import Mock, AsyncMock, patch
from uuid import uuid4

# Configure for testing
os.environ["ENABLE_S3_EVENT_STORE"] = "true"
os.environ["ENABLE_DUAL_WRITE"] = "true"

from oms.services.migration_helper import MigrationHelper
from oms.services.event_store import Event
from shared.models.commands import BaseCommand, CommandType


class TestMigrationHelper:
    """Test migration helper for gradual S3 adoption"""
    
    @pytest.fixture
    def migration_helper(self):
        """Create migration helper instance"""
        helper = MigrationHelper()
        return helper
    
    @pytest.fixture
    def sample_command(self):
        """Create sample command for testing"""
        return BaseCommand(
            command_id=str(uuid4()),
            command_type=CommandType.CREATE_INSTANCE,
            aggregate_type="Instance",
            aggregate_id=f"inst-{uuid4().hex[:8]}",
            payload={
                "data": "test data",
                "value": 123
            },
            metadata={
                "source": "test",
                "user": "test_user"
            }
        )
    
    def test_migration_modes(self, migration_helper):
        """Test different migration modes"""
        # Test with different configurations
        test_cases = [
            (False, False, "legacy"),
            (True, False, "s3_only"),
            (True, True, "dual_write"),
        ]
        
        for s3_enabled, dual_write, expected_mode in test_cases:
            migration_helper.s3_enabled = s3_enabled
            migration_helper.dual_write = dual_write
            
            mode = migration_helper._get_migration_mode()
            assert mode == expected_mode
    
    @pytest.mark.asyncio
    async def test_dual_write_mode(self, migration_helper):
        """Test dual-write mode writes to both storages"""
        migration_helper.s3_enabled = True
        migration_helper.dual_write = True
        
        # Mock dependencies
        mock_connection = AsyncMock()
        mock_outbox_service = AsyncMock()
        mock_event_store = AsyncMock()
        
        # Patch event store
        with patch.object(migration_helper, '_get_event_store', return_value=mock_event_store):
            command = self.sample_command()
            
            result = await migration_helper.handle_command_with_migration(
                connection=mock_connection,
                command=command,
                outbox_service=mock_outbox_service,
                topic="test-topic",
                actor="test_user"
            )
            
            # Verify both storages were written to
            assert mock_event_store.append_event.called
            assert mock_outbox_service.publish_command.called
            assert result["migration_mode"] == "dual_write"
            assert result["s3_stored"] is True
            assert result["postgres_stored"] is True
    
    @pytest.mark.asyncio
    async def test_s3_only_mode(self, migration_helper):
        """Test S3-only mode only writes to S3"""
        migration_helper.s3_enabled = True
        migration_helper.dual_write = False
        
        mock_connection = AsyncMock()
        mock_outbox_service = AsyncMock()
        mock_event_store = AsyncMock()
        
        with patch.object(migration_helper, '_get_event_store', return_value=mock_event_store):
            command = self.sample_command()
            
            result = await migration_helper.handle_command_with_migration(
                connection=mock_connection,
                command=command,
                outbox_service=mock_outbox_service,
                topic="test-topic",
                actor="test_user"
            )
            
            # Verify only S3 was written to
            assert mock_event_store.append_event.called
            assert mock_outbox_service.publish_command.called  # Still publishes for delivery
            assert result["migration_mode"] == "s3_only"
            assert result["s3_stored"] is True
    
    @pytest.mark.asyncio
    async def test_legacy_mode(self, migration_helper):
        """Test legacy mode only uses PostgreSQL"""
        migration_helper.s3_enabled = False
        migration_helper.dual_write = False
        
        mock_connection = AsyncMock()
        mock_outbox_service = AsyncMock()
        
        command = self.sample_command()
        
        result = await migration_helper.handle_command_with_migration(
            connection=mock_connection,
            command=command,
            outbox_service=mock_outbox_service,
            topic="test-topic",
            actor="test_user"
        )
        
        # Verify only PostgreSQL was used
        assert mock_outbox_service.publish_command.called
        assert result["migration_mode"] == "legacy"
        assert result.get("s3_stored") is False
        assert result["postgres_stored"] is True
    
    @pytest.mark.asyncio
    async def test_s3_failure_handling(self, migration_helper):
        """Test graceful handling of S3 failures in dual-write mode"""
        migration_helper.s3_enabled = True
        migration_helper.dual_write = True
        
        mock_connection = AsyncMock()
        mock_outbox_service = AsyncMock()
        mock_event_store = AsyncMock()
        
        # Make S3 write fail
        mock_event_store.append_event.side_effect = Exception("S3 connection failed")
        
        with patch.object(migration_helper, '_get_event_store', return_value=mock_event_store):
            command = self.sample_command()
            
            result = await migration_helper.handle_command_with_migration(
                connection=mock_connection,
                command=command,
                outbox_service=mock_outbox_service,
                topic="test-topic",
                actor="test_user"
            )
            
            # Should still write to PostgreSQL even if S3 fails
            assert mock_outbox_service.publish_command.called
            assert result["s3_stored"] is False
            assert result["postgres_stored"] is True
            assert "error" in result
    
    @pytest.mark.asyncio
    async def test_event_conversion(self, migration_helper):
        """Test command to event conversion for S3 storage"""
        command = self.sample_command()
        
        event = migration_helper._command_to_event(command, "test_actor")
        
        assert event.event_type == f"COMMAND_{command.command_type}"
        assert event.aggregate_type == command.aggregate_type
        assert event.aggregate_id == command.aggregate_id
        assert event.payload == command.payload
        assert event.actor == "test_actor"
        assert event.metadata["command_id"] == str(command.command_id)
    
    def test_feature_flag_interpretation(self, migration_helper):
        """Test correct interpretation of feature flags"""
        # Test various environment variable values
        test_cases = [
            ("true", True),
            ("True", True),
            ("TRUE", True),
            ("1", False),  # Only 'true' string counts
            ("false", False),
            ("False", False),
            ("", False),
            (None, False),
        ]
        
        for env_value, expected in test_cases:
            with patch.dict(os.environ, {"ENABLE_S3_EVENT_STORE": env_value or ""}):
                helper = MigrationHelper()
                if env_value is None:
                    assert helper.s3_enabled == False
                else:
                    assert helper.s3_enabled == expected


class TestMigrationRollback:
    """Test rollback scenarios"""
    
    @pytest.mark.asyncio
    async def test_instant_rollback(self):
        """Test instant rollback by changing feature flags"""
        helper = MigrationHelper()
        
        # Start in dual-write mode
        helper.s3_enabled = True
        helper.dual_write = True
        assert helper._get_migration_mode() == "dual_write"
        
        # Instant rollback to legacy
        helper.s3_enabled = False
        assert helper._get_migration_mode() == "legacy"
        
        # Back to dual-write
        helper.s3_enabled = True
        assert helper._get_migration_mode() == "dual_write"
        
        # Forward to S3-only
        helper.dual_write = False
        assert helper._get_migration_mode() == "s3_only"
    
    @pytest.mark.asyncio
    async def test_safe_migration_path(self):
        """Test the recommended safe migration path"""
        helper = MigrationHelper()
        
        migration_path = [
            (False, False, "legacy"),      # Start
            (True, True, "dual_write"),    # Enable dual-write
            (True, False, "s3_only"),      # Complete migration
        ]
        
        for s3, dual, expected_mode in migration_path:
            helper.s3_enabled = s3
            helper.dual_write = dual
            mode = helper._get_migration_mode()
            assert mode == expected_mode
            
            # Verify we can always roll back
            helper.s3_enabled = False
            helper.dual_write = False
            assert helper._get_migration_mode() == "legacy"
            
            # Restore state for next iteration
            helper.s3_enabled = s3
            helper.dual_write = dual


class TestMigrationMetrics:
    """Test migration metrics and monitoring"""
    
    @pytest.mark.asyncio
    async def test_migration_metrics(self, migration_helper):
        """Test that migration metrics are properly tracked"""
        migration_helper.s3_enabled = True
        migration_helper.dual_write = True
        
        mock_connection = AsyncMock()
        mock_outbox_service = AsyncMock()
        mock_event_store = AsyncMock()
        
        with patch.object(migration_helper, '_get_event_store', return_value=mock_event_store):
            command = BaseCommand(
                command_id=str(uuid4()),
                command_type=CommandType.CREATE_INSTANCE,
                aggregate_type="Instance",
                aggregate_id="test-123",
                payload={"test": "data"},
                metadata={}
            )
            
            result = await migration_helper.handle_command_with_migration(
                connection=mock_connection,
                command=command,
                outbox_service=mock_outbox_service,
                topic="test-topic",
                actor="test_user"
            )
            
            # Check metrics in result
            assert "migration_mode" in result
            assert "s3_stored" in result
            assert "postgres_stored" in result
            assert result["migration_mode"] == "dual_write"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])