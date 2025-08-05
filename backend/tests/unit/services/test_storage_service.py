"""
Unit tests for StorageService - Focus on DELETE instance event handling

Tests the critical anti-pattern fix for DELETE event processing logic:
- Ensures all commands are preserved for audit trails
- Validates proper deleted instance state reconstruction  
- Tests Event Sourcing deletion principles
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from datetime import datetime
from typing import Dict, Any

# Mock boto3 to avoid dependency issues
with patch.dict('sys.modules', {
    'boto3': MagicMock(),
    'botocore': MagicMock(),
    'botocore.client': MagicMock(),
    'botocore.exceptions': MagicMock()
}):
    # Test imports
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

    from shared.services.storage_service import StorageService
    from shared.models.commands import CommandType


class TestStorageServiceDeletionHandling:
    """Test suite for storage service deletion event handling"""
    
    @pytest.fixture
    def storage_service(self):
        """Create a mock storage service for testing"""
        service = StorageService(
            endpoint_url="http://localhost:9000",
            access_key="test",
            secret_key="test"
        )
        # Mock the S3 client
        service.client = Mock()
        return service
    
    @pytest.fixture
    def sample_command_files(self):
        """Sample command file list with various operations including deletion"""
        return [
            "test_db/TestClass/instance123/create_cmd_001.json",
            "test_db/TestClass/instance123/update_cmd_002.json", 
            "test_db/TestClass/instance123/delete_cmd_003.json",
            "test_db/TestClass/instance123/some_deleted.json",  # Should NOT be filtered
        ]
    
    @pytest.fixture
    def mock_s3_response(self):
        """Mock S3 list_objects_v2 response"""
        return {
            'Contents': [
                {
                    'Key': 'test_db/TestClass/instance123/create_cmd_001.json',
                    'LastModified': datetime(2023, 1, 1, 10, 0, 0)
                },
                {
                    'Key': 'test_db/TestClass/instance123/update_cmd_002.json', 
                    'LastModified': datetime(2023, 1, 1, 11, 0, 0)
                },
                {
                    'Key': 'test_db/TestClass/instance123/delete_cmd_003.json',
                    'LastModified': datetime(2023, 1, 1, 12, 0, 0)
                },
                {
                    'Key': 'test_db/TestClass/instance123/some_deleted.json',
                    'LastModified': datetime(2023, 1, 1, 13, 0, 0)
                },
                {
                    'Key': 'test_db/TestClass/instance123/not_json.txt',  # Should be filtered
                    'LastModified': datetime(2023, 1, 1, 14, 0, 0)
                }
            ],
            'IsTruncated': False
        }
    
    @pytest.mark.asyncio
    async def test_get_all_commands_includes_deleted_files(self, storage_service, mock_s3_response):
        """
        Critical Test: Ensure 'deleted.json' files are NOT filtered out
        
        This tests the fix for the anti-pattern where deleted files were excluded,
        violating Event Sourcing principles of preserving all command history.
        """
        # Mock S3 response
        storage_service.client.list_objects_v2.return_value = mock_s3_response
        
        # Call the method
        result = await storage_service.get_all_commands_for_instance(
            bucket="test-bucket",
            db_name="test_db", 
            class_id="TestClass",
            instance_id="instance123"
        )
        
        # Verify ALL .json files are included (no filtering of deleted files)
        expected_files = [
            'test_db/TestClass/instance123/create_cmd_001.json',
            'test_db/TestClass/instance123/update_cmd_002.json',
            'test_db/TestClass/instance123/delete_cmd_003.json',
            'test_db/TestClass/instance123/some_deleted.json'  # MUST be included
        ]
        
        assert result == expected_files
        assert 'test_db/TestClass/instance123/some_deleted.json' in result, \
            "deleted.json files must be preserved for audit trails"
        
        # Verify non-JSON files are still filtered out
        assert 'test_db/TestClass/instance123/not_json.txt' not in result
    
    @pytest.mark.asyncio 
    async def test_replay_deleted_instance_returns_complete_state(self, storage_service):
        """
        Test that deleted instances return complete state with deletion metadata
        
        This validates the fix where deleted instances should return full state
        information rather than None, preserving audit trail capability.
        """
        # Mock command files data
        command_files = [
            "test_db/TestClass/instance123/create_cmd_001.json",
            "test_db/TestClass/instance123/delete_cmd_002.json"
        ]
        
        # Mock S3 file content
        create_command = {
            'command_id': 'cmd-001',
            'command_type': CommandType.CREATE_INSTANCE.value,
            'instance_id': 'instance123',
            'class_id': 'TestClass',
            'db_name': 'test_db',
            'created_at': '2023-01-01T10:00:00Z',
            'created_by': 'user123',
            'payload': {
                'name': 'Test Instance',
                'value': 42
            }
        }
        
        delete_command = {
            'command_id': 'cmd-002', 
            'command_type': CommandType.DELETE_INSTANCE.value,
            'instance_id': 'instance123',
            'class_id': 'TestClass', 
            'db_name': 'test_db',
            'created_at': '2023-01-01T11:00:00Z',
            'created_by': 'admin',
            'payload': {
                'reason': 'Data cleanup'
            }
        }
        
        # Mock load_json to return appropriate command data
        def mock_load_json(bucket, key):
            if 'create_cmd_001.json' in key:
                return create_command
            elif 'delete_cmd_002.json' in key:
                return delete_command
            return {}
        
        storage_service.load_json = AsyncMock(side_effect=mock_load_json)
        
        # Execute replay
        result = await storage_service.replay_instance_state("test-bucket", command_files)
        
        # Validate result is NOT None
        assert result is not None, "Deleted instances must return state, not None"
        
        # Validate basic instance data is preserved
        assert result['instance_id'] == 'instance123'
        assert result['class_id'] == 'TestClass'
        assert result['name'] == 'Test Instance'  # Original data preserved
        assert result['value'] == 42
        
        # Validate deletion metadata
        metadata = result['_metadata']
        assert metadata['deleted'] is True
        assert metadata['deleted_at'] == '2023-01-01T11:00:00Z'
        assert metadata['deleted_by'] == 'admin'
        assert metadata['deletion_command_id'] == 'cmd-002'
        assert metadata['deletion_reason'] == 'Data cleanup'
        
        # Validate command history is preserved
        assert len(metadata['command_history']) == 2
        assert metadata['total_commands'] == 2
    
    @pytest.mark.asyncio
    async def test_replay_orphan_deletion_creates_minimal_state(self, storage_service):
        """
        Test handling of DELETE command without prior CREATE (data integrity issue)
        
        This tests the edge case where a DELETE command exists without a prior
        CREATE command, which could happen due to data corruption or partial restores.
        """
        command_files = ["test_db/TestClass/instance123/delete_cmd_001.json"]
        
        delete_command = {
            'command_id': 'cmd-001',
            'command_type': CommandType.DELETE_INSTANCE.value,
            'instance_id': 'instance123',
            'class_id': 'TestClass',
            'db_name': 'test_db', 
            'created_at': '2023-01-01T10:00:00Z',
            'created_by': 'admin',
            'payload': {
                'reason': 'Emergency deletion'
            }
        }
        
        storage_service.load_json = AsyncMock(return_value=delete_command)
        
        # Execute replay
        result = await storage_service.replay_instance_state("test-bucket", command_files)
        
        # Validate minimal state is created
        assert result is not None
        assert result['instance_id'] == 'instance123'
        assert result['class_id'] == 'TestClass'
        assert result['db_name'] == 'test_db'
        
        # Validate orphan deletion metadata
        metadata = result['_metadata']
        assert metadata['deleted'] is True
        assert metadata['orphan_deletion'] is True  # Special flag for orphan deletions
        assert metadata['deletion_reason'] == 'Emergency deletion'
        assert metadata['version'] == 1
    
    def test_is_instance_deleted_utility(self, storage_service):
        """Test the utility method for checking deletion status"""
        
        # Test with normal instance
        normal_instance = {
            '_metadata': {'deleted': False}
        }
        assert storage_service.is_instance_deleted(normal_instance) is False
        
        # Test with deleted instance
        deleted_instance = {
            '_metadata': {'deleted': True}
        }
        assert storage_service.is_instance_deleted(deleted_instance) is True
        
        # Test with None
        assert storage_service.is_instance_deleted(None) is False
        
        # Test with missing metadata
        no_metadata = {'data': 'value'}
        assert storage_service.is_instance_deleted(no_metadata) is False
    
    def test_get_deletion_info_utility(self, storage_service):
        """Test the utility method for extracting deletion information"""
        
        deleted_instance = {
            '_metadata': {
                'deleted': True,
                'deleted_at': '2023-01-01T10:00:00Z',
                'deleted_by': 'admin',
                'deletion_command_id': 'cmd-123',
                'deletion_reason': 'Cleanup',
                'orphan_deletion': False
            }
        }
        
        deletion_info = storage_service.get_deletion_info(deleted_instance)
        
        assert deletion_info is not None
        assert deletion_info['deleted_at'] == '2023-01-01T10:00:00Z'
        assert deletion_info['deleted_by'] == 'admin'
        assert deletion_info['deletion_command_id'] == 'cmd-123'
        assert deletion_info['deletion_reason'] == 'Cleanup'
        assert deletion_info['is_orphan_deletion'] is False
        
        # Test with normal instance
        normal_instance = {'_metadata': {'deleted': False}}
        assert storage_service.get_deletion_info(normal_instance) is None
    
    @pytest.mark.asyncio
    async def test_replay_preserves_full_command_sequence(self, storage_service):
        """
        Test that complex command sequences including deletions preserve full audit trail
        """
        command_files = [
            "test_db/TestClass/instance123/create_cmd_001.json",
            "test_db/TestClass/instance123/update_cmd_002.json",
            "test_db/TestClass/instance123/update_cmd_003.json", 
            "test_db/TestClass/instance123/delete_cmd_004.json"
        ]
        
        commands = [
            {
                'command_id': 'cmd-001',
                'command_type': CommandType.CREATE_INSTANCE.value,
                'instance_id': 'instance123',
                'class_id': 'TestClass',
                'db_name': 'test_db',
                'created_at': '2023-01-01T10:00:00Z',
                'created_by': 'user1',
                'payload': {'name': 'Original', 'value': 1}
            },
            {
                'command_id': 'cmd-002',
                'command_type': CommandType.UPDATE_INSTANCE.value,
                'instance_id': 'instance123',
                'class_id': 'TestClass',
                'db_name': 'test_db', 
                'created_at': '2023-01-01T11:00:00Z',
                'created_by': 'user2',
                'payload': {'name': 'Updated', 'new_field': 'added'}
            },
            {
                'command_id': 'cmd-003',
                'command_type': CommandType.UPDATE_INSTANCE.value,
                'instance_id': 'instance123',
                'class_id': 'TestClass',
                'db_name': 'test_db',
                'created_at': '2023-01-01T12:00:00Z', 
                'created_by': 'user3',
                'payload': {'value': 999}
            },
            {
                'command_id': 'cmd-004',
                'command_type': CommandType.DELETE_INSTANCE.value,
                'instance_id': 'instance123',
                'class_id': 'TestClass',
                'db_name': 'test_db',
                'created_at': '2023-01-01T13:00:00Z',
                'created_by': 'admin',
                'payload': {'reason': 'Compliance requirement'}
            }
        ]
        
        def mock_load_json(bucket, key):
            for i, cmd in enumerate(commands):
                if f'cmd_{str(i+1).zfill(3)}.json' in key:
                    return cmd
            return {}
        
        storage_service.load_json = AsyncMock(side_effect=mock_load_json)
        
        # Execute replay
        result = await storage_service.replay_instance_state("test-bucket", command_files)
        
        # Validate final state reflects all changes
        assert result['name'] == 'Updated'  # From update command
        assert result['value'] == 999       # From second update
        assert result['new_field'] == 'added'  # Added field preserved
        
        # Validate deletion state
        assert result['_metadata']['deleted'] is True
        assert result['_metadata']['version'] == 3  # CREATE + 2 UPDATEs
        
        # Validate complete audit trail
        history = result['_metadata']['command_history']
        assert len(history) == 4
        assert history[0]['command_type'] == CommandType.CREATE_INSTANCE.value
        assert history[1]['command_type'] == CommandType.UPDATE_INSTANCE.value
        assert history[2]['command_type'] == CommandType.UPDATE_INSTANCE.value
        assert history[3]['command_type'] == CommandType.DELETE_INSTANCE.value
        
        # Validate users are tracked
        assert history[0]['command_id'] == 'cmd-001'
        assert history[3]['command_id'] == 'cmd-004'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])